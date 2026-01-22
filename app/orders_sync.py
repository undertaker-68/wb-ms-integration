import json
import logging
import os
import time as _t
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

    # --- state files ---
    state_file = os.getenv("STATE_FILE", "/root/wb_ms_integration/state_seen_orders.json")
    bootstrap = os.getenv("BOOTSTRAP", "0") == "1"

    ms_created_file = os.getenv("MS_CREATED_FILE", "/root/wb_ms_integration/ms_created_orders.json")

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

    # --- fetch orders from WB ---
    new_orders = wb.get_new_orders()
    log.info("wb_new_orders_loaded", extra={"count": len(new_orders)})

    listed: List[Dict[str, Any]] = []
    next_ = 0
    for _ in range(10):
        page = wb.list_orders(limit=1000, next_=next_)
        batch = page.get("orders", []) if isinstance(page, dict) else []
        listed.extend(batch)
        next_ = page.get("next", 0) if isinstance(page, dict) else 0
        log.info("wb_orders_page", extra={"got": len(batch), "next": next_})
        if not batch:
            break

    # uniq by WB id
    all_by_id: Dict[int, Dict[str, Any]] = {}
    for o in new_orders + listed:
        if "id" in o:
            all_by_id[int(o["id"])] = o
    all_orders = list(all_by_id.values())

    log.info("wb_orders_total", extra={"count": len(all_orders)})

    # --- bootstrap / new-only filter ---
    def order_num(o: Dict[str, Any]) -> str:
        return str(o["id"])  # строго WB id

    seen = load_set(state_file)
    current = {order_num(o) for o in all_orders}

    if bootstrap or not seen:
        save_set(state_file, current)
        log.info("bootstrap_done_skip_processing", extra={"saved": len(current), "state_file": state_file})
        return

    new_only = [o for o in all_orders if order_num(o) not in seen]
    log.info("after_filter_new_only", extra={"new_count": len(new_only), "seen_count": len(seen)})

    # если нет новых — просто выходим
    if not new_only:
        return

    # --- statuses (optional, for demand creation) ---
    ids = [int(o["id"]) for o in new_only if "id" in o]
    statuses = wb.get_orders_status(ids) if ids else []
    status_by_id: Dict[int, Dict[str, Any]] = {
        int(s["id"]): s for s in statuses if isinstance(s, dict) and "id" in s
    }
    log.info("wb_statuses_loaded", extra={"count": len(status_by_id)})

    if ids and not status_by_id:
        log.warning("wb_statuses_empty", extra={"ids_count": len(ids)})

    # --- prefetch products by article (новых обычно мало) ---
    uniq_articles = sorted({extract_article(o) for o in new_only if extract_article(o)})
    product_by_article: Dict[str, Dict[str, Any]] = {}
    for a in uniq_articles:
        p = ms.find_product_by_article(a)
        if p:
            product_by_article[a] = p
        _t.sleep(0.12)
    log.info("ms_products_prefetched", extra={"uniq_articles": len(uniq_articles), "found": len(product_by_article)})

    # --- MS created registry (чтобы не дергать MS customerorder по каждому заказу) ---
    ms_created = load_set(ms_created_file)

    created_count = 0
    skipped_no_article = 0
    skipped_no_product = 0
    skipped_already_created = 0
    demand_created = 0
    cancelled = 0

    for o in new_only:
        num = order_num(o)
        ext_code = num  # externalCode в МС

        # если уже создавали ранее (по нашему локальному файлу) — пропускаем
        if ext_code in ms_created:
            skipped_already_created += 1
            log.info("skip_already_created", extra={"order_id": num})
            continue

        article = extract_article(o)
        if not article:
            skipped_no_article += 1
            log.warning("skip_no_article", extra={"order_id": num})
            continue

        product = product_by_article.get(article)
        if not product:
            skipped_no_product += 1
            log.warning("skip_no_ms_product", extra={"order_id": num, "article": article})
            continue

        st = status_by_id.get(int(o["id"]), {})
        supplier_status = st.get("supplierStatus")
        wb_status = st.get("wbStatus")

        # отмена — просто отметим как seen, но в МС не создаём
        if supplier_status == "cancel" or wb_status == "canceled":
            cancelled += 1
            log.info("order_cancelled_seen", extra={"order_id": num})
            continue

        payload = build_ms_order_payload(cfg, o, product)

        if cfg.test_mode:
            created_count += 1
            log.info("TEST_MODE_skip_ms_order_create", extra={"order_id": num, "article": article})
            # в тесте НЕ пишем ms_created_file, чтобы не “запомнить” фиктивно
            ms_order = {
                "id": "TEST",
                "meta": {"type": "customerorder", "href": "TEST"},
                "externalCode": ext_code,
                "name": ext_code,
                "organization": payload["organization"],
                "agent": payload["agent"],
                "store": payload["store"],
            }
        else:
            ms_order = ms.create_customer_order(payload)
            created_count += 1
            log.info("ms_order_created", extra={"order_id": num, "ms_id": ms_order.get("id"), "article": article})
            ms_created.add(ext_code)
            save_set(ms_created_file, ms_created)

        # complete -> demand
        if supplier_status == "complete":
            if cfg.test_mode:
                log.info("TEST_MODE_skip_ms_demand_create", extra={"order_id": num, "externalCode": ext_code})
            else:
                if not ms.find_demand_by_external_code(ext_code):
                    demand_payload = build_ms_demand_payload(cfg, ms_order)
                    ms.create_demand(demand_payload)
                    demand_created += 1
                    log.info("ms_demand_created", extra={"order_id": num, "externalCode": ext_code})
                else:
                    log.info("ms_demand_exists", extra={"order_id": num, "externalCode": ext_code})

    # отметить обработанные WB-заказы как seen (чтобы не пытаться снова)
    seen |= {order_num(o) for o in new_only}
    save_set(state_file, seen)

    log.info(
        "done",
        extra={
            "created_count": created_count,
            "skipped_already_created": skipped_already_created,
            "skipped_no_article": skipped_no_article,
            "skipped_no_product": skipped_no_product,
            "cancelled_count": cancelled,
            "demand_created": demand_created,
            "test_mode": cfg.test_mode,
            "state_file": state_file,
            "ms_created_file": ms_created_file,
        },
    )


if __name__ == "__main__":
    main()
