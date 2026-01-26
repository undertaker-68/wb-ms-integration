import json
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict

import requests

from app.config import load_config
from app.http import HttpClient
from app.wb_client import WBClient
from app.ms_client import MSClient
from app.logger import get_logger

log = get_logger("orders_sync")


def parse_dt(s: str) -> datetime:
    s = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def main():
    cfg = load_config()

    log.info("start", extra={"test_mode": cfg.test_mode})

    http_wb = HttpClient(
        cfg.wb_base_url,
        headers={"Authorization": cfg.wb_token},
        timeout=30,
    )
    wb = WBClient(http_wb)

    http_ms = HttpClient(
        cfg.ms_base_url,
        headers={"Authorization": f"Bearer {cfg.ms_token}"},
        timeout=30,
    )
    ms = MSClient(http_ms)

    # --- граница по дате ---
    min_created_at = None
    if cfg.min_created_at_iso:
        min_created_at = parse_dt(cfg.min_created_at_iso)

    # --- загрузка заказов WB ---
    all_orders = []
    next_ = 0
    skipped_by_created = 0

    while True:
        page = wb.list_orders(limit=1000, next_=next_, date_from=0)
        orders = page.get("orders", [])
        log.info(
            "wb_orders_page",
            extra={"got": len(orders), "next": page.get("next")},
        )

        for o in orders:
            ca = o.get("createdAt") or o.get("created_at")
            if ca and min_created_at:
                if parse_dt(str(ca)) < min_created_at:
                    skipped_by_created += 1
                    continue
            all_orders.append(o)

        next_ = page.get("next", 0)
        if not orders:
            break

    log.info(
        "wb_orders_total",
        extra={
            "count": len(all_orders),
            "skipped_by_createdAt": skipped_by_created,
            "min_created_at": cfg.min_created_at_iso,
        },
    )

    if not all_orders:
        log.info("done", extra={"created_customerorders": 0, "created_demands": 0})
        return

    # --- статусы WB ---
    ids = [int(o["id"]) for o in all_orders if "id" in o]
    statuses = wb.get_orders_status(ids)
    status_map = {int(x["id"]): x for x in statuses}

    log.info("wb_statuses_loaded", extra={"count": len(status_map)})

    # --- товары МС ---
    articles = {o.get("article") for o in all_orders if o.get("article")}
    products = ms.get_products_by_articles(articles)

    log.info(
        "ms_products_prefetched",
        extra={"uniq_articles": len(articles), "found": len(products)},
    )

    # --- существующие документы МС ---
    existing_demands = ms.get_existing_demands()
    existing_orders = ms.get_existing_customerorders()

    created_orders = 0
    created_demands = 0

    for o in all_orders:
        oid = str(o["id"])
        article = o.get("article")

        if not article or article not in products:
            continue

        st = status_map.get(int(o["id"]), {})
        supplier_status = st.get("supplierStatus")
        wb_status = st.get("wbStatus")

        # --- blacklist статусов ---
        if supplier_status in {"new", "cancel"}:
            continue
        if wb_status in {"waiting", "canceled"}:
            continue

        # --- customerorder ---
        if oid not in existing_orders:
            order_payload = ms.build_customerorder_payload(
                oid=oid,
                product=products[article],
                price=o.get("price"),
            )
            if not cfg.test_mode:
                ms_order = ms.create_customerorder(order_payload)
                created_orders += 1
                log.info(
                    "ms_order_created",
                    extra={"order_id": oid, "ms_id": ms_order["id"], "article": article},
                )

        # --- demand ---
        if oid in existing_demands:
            continue

        demand_payload = ms.build_demand_payload(
            oid=oid,
            product=products[article],
        )

        if cfg.test_mode:
            continue

        demand = ms.create_demand(demand_payload)

        # 1️⃣ пробуем провести
        try:
            ms.set_demand_applicable(demand, True)
            log.info("ms_demand_applied", extra={"order_id": oid})

        except requests.exceptions.HTTPError as e:
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", None)
            body = {}
            try:
                body = resp.json() if resp is not None else {}
            except Exception:
                pass

            ms_err_codes = {
                err.get("code")
                for err in (body.get("errors") or [])
                if isinstance(err, dict)
            }

            # нет остатков → оставляем непроведённой
            if status == 412 and 3007 in ms_err_codes:
                log.info(
                    "ms_demand_left_unapplied_no_stock",
                    extra={"order_id": oid},
                )
            else:
                raise

        # 2️⃣ ВСЕГДА ставим статус отгрузки
        if cfg.ms_demand_status_id:
            ms.update_demand_state(demand, cfg.ms_demand_status_id)
            log.info(
                "ms_demand_state_set",
                extra={"order_id": oid, "state_id": cfg.ms_demand_status_id},
            )

        created_demands += 1

    log.info(
        "done",
        extra={
            "created_customerorders": created_orders,
            "created_demands": created_demands,
            "test_mode": cfg.test_mode,
            "min_created_at": cfg.min_created_at_iso,
        },
    )


if __name__ == "__main__":
    main()
