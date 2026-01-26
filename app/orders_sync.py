import json
import logging
import os
import time as _t
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

from .config import load_config
from .http import HttpClient
from .logging_setup import setup_logging
from .ms_client import MSClient
from .wb_client import WBClient

log = logging.getLogger("orders_sync")


def _parse_iso_dt(s: str) -> datetime:
    ss = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(ss)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def extract_article(wb_order: Dict[str, Any]) -> str:
    return str(wb_order.get("article") or "").strip()


def resolve_ms_customerorder_state_id(
    cfg,
    supplier_status: str | None,
    wb_status: str | None,
    oid: str,
    active: set[str],
    *,
    has_demand: bool,
) -> str | None:
    """Маппинг пары (supplierStatus, wbStatus) -> статус CustomerOrder в МС.

    Правила:
    - Отмены обрабатываем только если Demand ещё НЕ создан.
    - confirm + waiting может маппиться в 2 статуса МС (на сборке/собран) через active.
    """
    if not supplier_status or not wb_status:
        return None

    # 1) Отмены ДО отгрузки (Demand)
    if not has_demand and (
        supplier_status == "cancel" or wb_status in ("canceled", "canceled_by_client")
    ):
        return cfg.ms_status_cancelled_id or None

    # 2) Ещё у нас
    if wb_status == "waiting":
        if supplier_status == "new":
            return cfg.ms_status_new_id or None

        if supplier_status == "confirm":
            # Двухшаговый confirm: первый раз -> confirm_id, повтор -> confirm2_id
            if oid in active and cfg.ms_status_confirm2_id:
                return cfg.ms_status_confirm2_id
            return cfg.ms_status_confirm_id or cfg.ms_status_confirm2_id or None

    # 3) Уехал от нас / у WB
    if supplier_status == "complete":
        if wb_status == "waiting":
            return cfg.ms_status_shipped_id or None
        if wb_status == "sorted":
            return cfg.ms_status_delivering_id or None
        if wb_status == "sold":
            return cfg.ms_status_delivered_id or None

    return None


def build_ms_order_payload(cfg, wb_order: Dict[str, Any], product: Dict[str, Any]) -> Dict[str, Any]:
    """CustomerOrder в МС. Номер = WB id, резервируем 1 шт, цена = дефолтная цена товара в МС."""
    order_num = str(wb_order["id"])
    external_code = order_num
    name = order_num

    qty = 1
    sale_prices = product.get("salePrices") or []
    price = int(sale_prices[0]["value"]) if sale_prices and sale_prices[0].get("value") is not None else 0

    positions = [
        {
            "quantity": qty,
            "reserve": qty,
            "price": price,
            "assortment": {"meta": product["meta"]},
        }
    ]

    payload: Dict[str, Any] = {
        "name": name,
        "externalCode": external_code,
        "organization": {
            "meta": {"type": "organization", "href": f"{cfg.ms_base_url}/entity/organization/{cfg.ms_org_id}"}
        },
        "agent": {
            "meta": {"type": "counterparty", "href": f"{cfg.ms_base_url}/entity/counterparty/{cfg.ms_agent_id_wb}"}
        },
        "store": {"meta": {"type": "store", "href": f"{cfg.ms_base_url}/entity/store/{cfg.ms_store_id_wb}"}},
        "salesChannel": {
            "meta": {"type": "saleschannel", "href": f"{cfg.ms_base_url}/entity/saleschannel/{cfg.ms_sales_channel_id_wb}"}
        },
        "positions": positions,
    }

    if cfg.ms_status_new_id:
        payload["state"] = {
            "meta": {
                "type": "state",
                "href": f"{cfg.ms_base_url}/entity/customerorder/metadata/states/{cfg.ms_status_new_id}",
            }
        }

    return payload


def build_ms_demand_payload(cfg, ms_order: Dict[str, Any], order_positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Создаём Demand и связываем с CustomerOrder (Связанные документы)."""
    external_code = ms_order.get("externalCode") or ms_order.get("name")

    demand_positions: List[Dict[str, Any]] = []
    for p in order_positions:
        assortment_meta = ((p.get("assortment") or {}).get("meta")) or None
        if not assortment_meta:
            continue
        demand_positions.append(
            {
                "quantity": p.get("quantity", 1),
                "price": p.get("price", 0),
                "assortment": {"meta": assortment_meta},
            }
        )

    payload: Dict[str, Any] = {
        "name": f"WB-{external_code}",
        "externalCode": str(external_code),
        "organization": ms_order["organization"],
        "agent": ms_order["agent"],
        "store": ms_order["store"],
        "customerOrder": {"meta": ms_order["meta"]},
        "positions": demand_positions,
    }
    return payload


def _state_id_from_href(href: str) -> str:
    return href.rstrip("/").split("/")[-1] if href else ""


def main() -> None:
    cfg = load_config()
    setup_logging(cfg.log_level)

    ms_http = HttpClient(
        cfg.ms_base_url,
        headers={"Authorization": f"Bearer {cfg.ms_token}"},
        timeout=cfg.http_timeout_sec,
    )
    wb_http = HttpClient(
        cfg.wb_base_url,
        headers={"Authorization": cfg.wb_token},
        timeout=cfg.http_timeout_sec,
    )

    ms = MSClient(ms_http)
    wb = WBClient(wb_http)

    log.info("start", extra={"test_mode": cfg.test_mode})

    # persistent state
    ms_created_file = os.getenv("MS_CREATED_FILE", "/root/wb_ms_integration/ms_created_orders.json")
    active_file = os.getenv("ACTIVE_FILE", "/root/wb_ms_integration/active_orders.json")

    # date cutoff
    min_created_at_iso = os.getenv("MIN_CREATED_AT_ISO", "2026-01-23T00:00:00+03:00")
    min_created_at = _parse_iso_dt(min_created_at_iso)
    date_from = int(min_created_at.astimezone(timezone.utc).timestamp())

    # optional: set Demand state after creation
    demand_state_id = os.getenv("MS_DEMAND_STATUS_ID", "").strip()

    def load_set(path: str) -> set[str]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()

    def save_set(path: str, s: set[str]) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted(s), f, ensure_ascii=False, indent=2)

    ms_created = load_set(ms_created_file)
    active = load_set(active_file)

    # --- fetch WB orders from cutoff ---
    listed: List[Dict[str, Any]] = []
    next_ = 0
    for _ in range(50):
        page = wb.list_orders(limit=1000, next_=next_, date_from=date_from)
        batch = page.get("orders", []) if isinstance(page, dict) else []
        listed.extend(batch)
        next_ = page.get("next", 0) if isinstance(page, dict) else 0
        log.info("wb_orders_page", extra={"got": len(batch), "next": next_})
        if not batch:
            break

    # strict filter by createdAt (чтобы не пролезали старые)
    all_orders: List[Dict[str, Any]] = []
    skipped = 0
    for o in listed:
        ca = o.get("createdAt") or o.get("created_at")
        if not ca:
            skipped += 1
            continue
        try:
            dt = _parse_iso_dt(str(ca))
            if dt >= min_created_at:
                all_orders.append(o)
            else:
                skipped += 1
        except Exception:
            skipped += 1

    log.info(
        "wb_orders_total",
        extra={"count": len(all_orders), "skipped_by_createdAt": skipped, "min_created_at": min_created_at.isoformat()},
    )

    # statuses
    ids = sorted({int(o["id"]) for o in all_orders if "id" in o})
    statuses = wb.get_orders_status(ids) if ids else []
    status_by_id: Dict[int, Dict[str, Any]] = {
        int(s["id"]): s for s in statuses if isinstance(s, dict) and "id" in s
    }
    log.info("wb_statuses_loaded", extra={"count": len(status_by_id)})

    # Prefetch products by article
    uniq_articles = sorted({extract_article(o) for o in all_orders if extract_article(o)})
    product_by_article: Dict[str, Dict[str, Any]] = {}
    for a in uniq_articles:
        if not a:
            continue
        p = ms.find_product_by_article(a)
        if p:
            product_by_article[a] = p
        _t.sleep(0.08)
    log.info("ms_products_prefetched", extra={"uniq_articles": len(uniq_articles), "found": len(product_by_article)})

    created_orders = 0
    created_demands = 0
    skipped_no_article = 0
    skipped_no_product = 0
    cancelled = 0
    demand_exists = 0
    activated = 0
    deactivated = 0
    demands_left_unapplied = 0

    # MS statuses where we must NOT create demand
    # (Новый, ожидает сборки, Отгружено, Отмены) — не создают отгрузку.
    demand_deny_state_ids = {
        cfg.ms_status_new_id,
        cfg.ms_status_confirm_id,
        cfg.ms_status_confirm2_id,
        cfg.ms_status_shipped_id,
        cfg.ms_status_cancelled_id,
        cfg.ms_status_cancelled_by_seller_id,
    }
    demand_deny_state_ids = {x for x in demand_deny_state_ids if x}

    for o in all_orders:
        if "id" not in o:
            continue

        oid = str(o["id"])
        st = status_by_id.get(int(o["id"]), {})
        supplier_status = st.get("supplierStatus")
        wb_status = st.get("wbStatus")

        # 1) Если Demand уже есть — стираем и не трогаем больше
        has_demand = False
        if not cfg.test_mode:
            has_demand = bool(ms.find_demand_by_external_code(oid))
        if has_demand:
            demand_exists += 1
            if oid in active:
                active.discard(oid)
                deactivated += 1
            continue

        # 2) Отмена ДО Demand — ставим Cancelled и стираем
        if supplier_status == "cancel" or wb_status in ("canceled", "canceled_by_client"):
            cancelled += 1
            if not cfg.test_mode and cfg.ms_status_cancelled_id:
                ms_order_tmp = ms.find_customer_order_by_external_code(oid)
                if ms_order_tmp:
                    ms.update_customer_order_state(ms_order_tmp, cfg.ms_status_cancelled_id)

            if oid in active:
                active.discard(oid)
                deactivated += 1
            continue

        # 3) Гарантируем CustomerOrder
        ms_order = None
        if oid in ms_created and not cfg.test_mode:
            ms_order = ms.find_customer_order_by_external_code(oid)
        if not ms_order and not cfg.test_mode:
            ms_order = ms.find_customer_order_by_external_code(oid)

        if not ms_order:
            article = extract_article(o)
            if not article:
                skipped_no_article += 1
                continue
            product = product_by_article.get(article)
            if not product:
                skipped_no_product += 1
                continue

            payload = build_ms_order_payload(cfg, o, product)

            if cfg.test_mode:
                created_orders += 1
                ms_order = {
                    "id": "TEST",
                    "meta": {"type": "customerorder", "href": "TEST"},
                    "externalCode": oid,
                    "name": oid,
                    "organization": payload["organization"],
                    "agent": payload["agent"],
                    "store": payload["store"],
                    "state": payload.get("state"),
                }
            else:
                ms_order = ms.create_customer_order(payload)
                created_orders += 1
                ms_created.add(oid)
                save_set(ms_created_file, ms_created)
                log.info("ms_order_created", extra={"order_id": oid, "ms_id": ms_order.get("id"), "article": article})

        # 4) Обновляем статус CustomerOrder по паре статусов WB
        target_state_id = resolve_ms_customerorder_state_id(
            cfg,
            supplier_status,
            wb_status,
            oid,
            active,
            has_demand=has_demand,
        )
        if target_state_id and not cfg.test_mode:
            ms_order = ms.update_customer_order_state(ms_order, target_state_id)

        # 5) Demand создаём по СТАТУСУ МС:
        # Новый/ожидает сборки/отгружено/отмены — НЕ создаём. Все остальные — создаём.
        ms_state_href = ((ms_order.get("state") or {}).get("meta") or {}).get("href") or ""
        ms_state_id = _state_id_from_href(ms_state_href)
        should_create_demand = bool(ms_state_id and ms_state_id not in demand_deny_state_ids)

        if should_create_demand:
            if cfg.test_mode:
                created_demands += 1
                if oid in active:
                    active.discard(oid)
                    deactivated += 1
                log.info("TEST_MODE_would_create_demand", extra={"order_id": oid, "ms_state_id": ms_state_id})
            else:
                order_positions = ms.get_customer_order_positions(ms_order)
                demand_payload = build_ms_demand_payload(cfg, ms_order, order_positions)
                demand = ms.create_demand(demand_payload)

                # проводим, если можно; если нет остатков — оставляем непроведенной
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
                        body = {}

                    ms_err_codes = {
                        err.get("code")
                        for err in (body.get("errors") or [])
                        if isinstance(err, dict)
                    }
                    if status == 412 and 3007 in ms_err_codes:
                        demands_left_unapplied += 1
                        log.info("ms_demand_left_unapplied_no_stock", extra={"order_id": oid})
                    else:
                        raise

                # ставим статус Demand, если задан
                if demand_state_id:
                    ms.update_demand_state(demand, demand_state_id)
                    log.info("ms_demand_state_set", extra={"order_id": oid, "state_id": demand_state_id})

                created_demands += 1
                if oid in active:
                    active.discard(oid)
                    deactivated += 1
                log.info("ms_demand_created", extra={"order_id": oid, "externalCode": oid, "positions": len(order_positions)})
        else:
            # ещё не время Demand -> держим в памяти active
            if oid not in active:
                active.add(oid)
                activated += 1

    save_set(active_file, active)

    log.info(
        "done",
        extra={
            "created_customerorders": created_orders,
            "created_demands": created_demands,
            "demand_exists_skipped": demand_exists,
            "cancelled": cancelled,
            "active_left": len(active),
            "activated": activated,
            "deactivated": deactivated,
            "skipped_no_article": skipped_no_article,
            "skipped_no_product": skipped_no_product,
            "demands_left_unapplied": demands_left_unapplied,
            "test_mode": cfg.test_mode,
            "active_file": active_file,
            "ms_created_file": ms_created_file,
            "min_created_at": min_created_at.isoformat(),
        },
    )


if __name__ == "__main__":
    main()
