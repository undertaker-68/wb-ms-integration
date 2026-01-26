import json
import logging
import os
import time as _t
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

from .config import load_config
from .http import HttpClient
from .logging_setup import setup_logging
from .ms_client import MSClient
from .wb_client import WBClient

log = logging.getLogger("orders_sync")


def build_ms_order_payload(cfg, wb_order: Dict[str, Any], product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Номер заказа МС = Номеру заказа WB (как в интерфейсе) => используем WB id (например 4508276599).
    """
    order_num = str(wb_order["id"])
    external_code = order_num
    name = order_num

    qty = 1  # WB FBS: одна позиция/1 шт (если будет иначе — расширим)
    sale_prices = product.get("salePrices") or []
    price = int(sale_prices[0]["value"]) if sale_prices and sale_prices[0].get("value") is not None else 0

    positions = [{
        "quantity": qty,
        "reserve": qty,
        "price": price,
        "assortment": {"meta": product["meta"]},
    }]

    payload: Dict[str, Any] = {
        "name": name,
        "externalCode": external_code,
        "organization": {"meta": {"type": "organization", "href": f"{cfg.ms_base_url}/entity/organization/{cfg.ms_org_id}"}},
        "agent": {"meta": {"type": "counterparty", "href": f"{cfg.ms_base_url}/entity/counterparty/{cfg.ms_agent_id_wb}"}},
        "store": {"meta": {"type": "store", "href": f"{cfg.ms_base_url}/entity/store/{cfg.ms_store_id_wb}"}},
        "salesChannel": {"meta": {"type": "saleschannel", "href": f"{cfg.ms_base_url}/entity/saleschannel/{cfg.ms_sales_channel_id_wb}"}},
        "positions": positions,
    }

    if cfg.ms_status_new_id:
        payload["state"] = {"meta": {"type": "state", "href": f"{cfg.ms_base_url}/entity/customerorder/metadata/states/{cfg.ms_status_new_id}"}}

    return payload


def build_ms_demand_payload(cfg, ms_order: Dict[str, Any]) -> Dict[str, Any]:
    external_code = ms_order.get("externalCode") or ms_order.get("name")
    payload: Dict[str, Any] = {
        "name": f"WB-{external_code}",
        "externalCode": str(external_code),
        "organization": ms_order["organization"],
        "agent": ms_order["agent"],
        "store": ms_order["store"],
        "customerOrder": {"meta": ms_order["meta"]},
    }
    if cfg.ms_status_shipped_id:
        payload["state"] = {"meta": {"type": "state", "href": f"{cfg.ms_base_url}/entity/demand/metadata/states/{cfg.ms_status_shipped_id}"}}
    return payload


def extract_article(wb_order: Dict[str, Any]) -> str:
    return str(wb_order.get("article") or "").strip()


def _parse_iso_dt(s: str) -> datetime:
    ss = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(ss)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


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

    # --- files ---
    ms_created_file = os.getenv("MS_CREATED_FILE", "/root/wb_ms_integration/ms_created_orders.json")

    # Active WB orders that are "in work" (CustomerOrder exists, Demand not created yet).
    active_file = os.getenv("ACTIVE_FILE", "/root/wb_ms_integration/active_orders.json")

    # Cutoff: process all WB orders created from this datetime inclusive.
    min_created_at_iso = os.getenv("MIN_CREATED_AT_ISO", "2025-01-23T00:00:00+03:00")
    min_created_at = _parse_iso_dt(min_created_at_iso)

    # Fallback lookback if WB objects don't have createdAt; used only for list_orders date_from.
    # We'll compute date_from from MIN_CREATED_AT_ISO anyway.
    now = datetime.now(timezone.utc)
    date_from = int(min_created_at.astimezone(timezone.utc).timestamp())

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
    for _ in range(50):  # больше, чтобы точно допагинировать
        page = wb.list_orders(limit=1000, next_=next_, date_from=date_from)
        batch = page.get("orders", []) if isinstance(page, dict) else []
        listed.extend(batch)
        next_ = page.get("next", 0) if isinstance(page, dict) else 0
        log.info("wb_orders_page", extra={"got": len(batch), "next": next_})
        if not batch:
            break

    # Filter by createdAt if WB provides it (strictly enforce MIN_CREATED_AT_ISO)
    all_orders: List[Dict[str, Any]] = []
    skipped = 0
    for o in listed:
        ca = o.get("createdAt") or o.get("created_at")
        if not ca:
            # if WB doesn't provide createdAt, we trust date_from boundary
            all_orders.append(o)
            continue
        try:
            dt = _parse_iso_dt(str(ca))
            if dt >= min_created_at:
                all_orders.append(o)
            else:
                skipped += 1
        except Exception:
            # can't parse -> keep (safe)
            all_orders.append(o)

    log.info("wb_orders_total", extra={"count": len(all_orders), "skipped_by_createdAt": skipped, "min_created_at": min_created_at.isoformat()})

    # Build id list for statuses
    ids = sorted({int(o["id"]) for o in all_orders if "id" in o})
    statuses = wb.get_orders_status(ids) if ids else []
    status_by_id: Dict[int, Dict[str, Any]] = {
        int(s["id"]): s for s in statuses if isinstance(s, dict) and "id" in s
    }
    log.info("wb_statuses_loaded", extra={"count": len(status_by_id)})

    # Prefetch products by article (only for orders that may need MS CustomerOrder)
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

    # Process every order in range
    for o in all_orders:
        if "id" not in o:
            continue

        oid = str(o["id"])
        st = status_by_id.get(int(o["id"]), {})
        supplier_status = st.get("supplierStatus")
        wb_status = st.get("wbStatus")

        # Cancelled -> remove from memory and skip
        if supplier_status == "cancel" or wb_status == "canceled":
            cancelled += 1
            if oid in active:
                active.discard(oid)
                deactivated += 1
            continue

        # If Demand exists -> erase from memory and skip (your rule)
        if not cfg.test_mode and ms.find_demand_by_external_code(oid):
            demand_exists += 1
            if oid in active:
                active.discard(oid)
                deactivated += 1
            continue

        # Ensure CustomerOrder exists in MS (either by our registry or by searching MS)
        ms_order = None
        if oid in ms_created and not cfg.test_mode:
            ms_order = ms.find_customer_order_by_external_code(oid)

        if not ms_order and not cfg.test_mode:
            ms_order = ms.find_customer_order_by_external_code(oid)

        if not ms_order:
            # Need to create CustomerOrder
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
                }
            else:
                ms_order = ms.create_customer_order(payload)
                created_orders += 1
                ms_created.add(oid)
                save_set(ms_created_file, ms_created)
                log.info("ms_order_created", extra={"order_id": oid, "ms_id": ms_order.get("id"), "article": article})

        # Now create Demand only when wbStatus == sorted
        if wb_status == "sorted":
            if cfg.test_mode:
                created_demands += 1
                if oid in active:
                    active.discard(oid)
                    deactivated += 1
                log.info("TEST_MODE_would_create_demand", extra={"order_id": oid})
            else:
                # Demand already checked above; if we are here - it doesn't exist yet
                demand_payload = build_ms_demand_payload(cfg, ms_order)
                ms.create_demand(demand_payload)
                created_demands += 1
                if oid in active:
                    active.discard(oid)
                    deactivated += 1
                log.info("ms_demand_created", extra={"order_id": oid, "externalCode": oid})
        else:
            # Not sorted yet -> keep in active memory
            if oid not in active:
                active.add(oid)
                activated += 1

    # Save active state
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
            "test_mode": cfg.test_mode,
            "active_file": active_file,
            "ms_created_file": ms_created_file,
            "min_created_at": min_created_at.isoformat(),
        },
    )


if __name__ == "__main__":
    main()
