import logging
from typing import Any, Dict, List

from .config import load_config
from .http import HttpClient
from .logging_setup import setup_logging
from .ms_client import MSClient
from .wb_client import WBClient
import json, os

log = logging.getLogger("orders_sync")


def build_ms_order_payload(cfg, wb_order: Dict[str, Any], product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Номер заказа МС = номеру заказа WB (как в интерфейсе) => используем orderUid.
    Если orderUid нет — fallback на id.
    """
    order_uid = str(wb_order.get("orderUid") or wb_order["id"])
    external_code = order_uid
    name = order_uid

    qty = 1  # WB FBS заказ обычно 1 шт; если будет иначе — расширим
    sale_prices = product.get("salePrices") or []
    if sale_prices and sale_prices[0].get("value") is not None:
        price = int(sale_prices[0]["value"])
    else:
        price = 0

    positions = [{
        "quantity": qty,
        "reserve": qty,
        "price": price,
        "assortment": {"meta": product["meta"]},
    }]

    payload: Dict[str, Any] = {
        "name": name,
        "externalCode": external_code,
        "organization": {
            "meta": {
                "type": "organization",
                "href": f"{cfg.ms_base_url}/entity/organization/{cfg.ms_org_id}",
            }
        },
        "agent": {
            "meta": {
                "type": "counterparty",
                "href": f"{cfg.ms_base_url}/entity/counterparty/{cfg.ms_agent_id_wb}",
            }
        },
        "store": {
            "meta": {
                "type": "store",
                "href": f"{cfg.ms_base_url}/entity/store/{cfg.ms_store_id_wb}",
            }
        },
        "salesChannel": {
            "meta": {
                "type": "saleschannel",
                "href": f"{cfg.ms_base_url}/entity/saleschannel/{cfg.ms_sales_channel_id_wb}",
            }
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


def build_ms_demand_payload(cfg, ms_order: Dict[str, Any]) -> Dict[str, Any]:
    external_code = ms_order.get("externalCode") or ms_order["name"]
    payload: Dict[str, Any] = {
        "name": f"WB-{external_code}",
        "externalCode": str(external_code),
        "organization": ms_order["organization"],
        "agent": ms_order["agent"],
        "store": ms_order["store"],
        "customerOrder": {"meta": ms_order["meta"]},
    }
    if cfg.ms_status_shipped_id:
        payload["state"] = {
            "meta": {
                "type": "state",
                "href": f"{cfg.ms_base_url}/entity/demand/metadata/states/{cfg.ms_status_shipped_id}",
            }
        }
    return payload


def extract_article(wb_order: Dict[str, Any]) -> str:
    # В WB /orders/new в примере есть поле article
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

    log.info("start", extra={"test_mode": getattr(cfg, "test_mode", False)})

    state_file = os.getenv("STATE_FILE", "state_seen_orders.json")
    bootstrap = os.getenv("BOOTSTRAP", "0") == "1"

    def load_seen() -> set[str]:
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()

    def save_seen(seen: set[str]) -> None:
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(sorted(seen), f, ensure_ascii=False, indent=2)

    # 1) новые
    new_orders = wb.get_new_orders()
    log.info("wb_new_orders_loaded", extra={"count": len(new_orders)})

    # 2) дополнительно активные из /orders
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

    # уникализируем по id
    all_orders_by_id: Dict[int, Dict[str, Any]] = {}
    for o in new_orders + listed:
        if "id" in o:
            all_orders_by_id[int(o["id"])] = o
    all_orders = list(all_orders_by_id.values())

    log.info("wb_orders_total", extra={"count": len(all_orders)})

    seen = load_seen()

    # берем “ключ” заказа так же, как номер для МС (см. ниже)
    def ms_order_number(o: dict) -> str:
        ou = str(o.get("orderUid") or "")
        if "_" in ou and ou.split("_", 1)[0].isdigit():
            return ou.split("_", 1)[0]          # берем числовую часть до "_"
        if ou.isdigit():
            return ou
        return str(o["id"])                     # fallback на внутренний id

    current = {ms_order_number(o) for o in all_orders}

    if bootstrap or not seen:
        save_seen(current)
        log.info("bootstrap_done_skip_processing", extra={"saved": len(current), "state_file": state_file})
        return

    new_only = [o for o in all_orders if ms_order_number(o) not in seen]
    log.info("after_filter_new_only", extra={"new": len(new_only), "seen": len(seen)})

    # дальше работаем не с all_orders, а с new_only
    all_orders = new_only

    # 3) статусы пачкой
    ids = [int(o["id"]) for o in all_orders if "id" in o]
    statuses = wb.get_orders_status(ids)
    status_by_id: Dict[int, Dict[str, Any]] = {
        int(s["id"]): s for s in statuses if isinstance(s, dict) and "id" in s
    }
    log.info("wb_statuses_loaded", extra={"count": len(status_by_id)})

    created = 0
    updated = 0
    skipped_no_product = 0
    demand_created = 0
    cancelled = 0
    skipped_no_article = 0

    # Собираем уникальные артикулы из WB
    articles = []
    for o in all_orders:
        a = extract_article(o)
        if a:
            articles.append(a)
    uniq_articles = sorted(set(articles))

    # Кэш: article -> product
    product_by_article: Dict[str, Dict[str, Any]] = {}

    # Быстрый вариант: для каждого артикула делаем 1 запрос, но с задержкой + кешем (уже лучше),
    # а в дальнейшем заменим на более умный батч по search.
    # Чтобы не ловить лимит — пауза 0.15с.
    import time as _t
    for a in uniq_articles:
        p = ms.find_product_by_article(a)
        if p:
            product_by_article[a] = p
        _t.sleep(0.15)

    log.info("ms_products_prefetched", extra={"uniq_articles": len(uniq_articles), "found": len(product_by_article)})

    for o in all_orders:
        order_uid = str(o.get("orderUid") or o["id"])  # номер из интерфейса WB
        ext_code = order_uid

        oid = int(o["id"])  # внутренний WB id (для поиска статуса)
        st = status_by_id.get(oid, {})
        supplier_status = st.get("supplierStatus")  # new/confirm/complete/cancel
        wb_status = st.get("wbStatus")              # waiting/sorted/sold/canceled

        article = extract_article(o)
        if not article:
            skipped_no_article += 1
            log.warning("skip_no_article", extra={"order_id": order_uid})
            continue

        product = product_by_article.get(article)
        if not product:
            skipped_no_product += 1
            log.warning("skip_no_ms_product", extra={"order_id": order_uid, "article": article})
            continue

        existing = ms.find_customer_order_by_external_code(ext_code)
        payload = build_ms_order_payload(cfg, o, product)

        if existing:
            ms_id = existing["id"]
            if cfg.test_mode:
                updated += 1
                log.info("TEST_MODE_skip_ms_order_update",
                         extra={"order_id": order_uid, "ms_id": ms_id, "article": article})
                ms_order = existing
            else:
                ms.update_customer_order(ms_id, payload)
                updated += 1
                log.info("ms_order_updated",
                         extra={"order_id": order_uid, "ms_id": ms_id, "article": article,
                                "supplierStatus": supplier_status, "wbStatus": wb_status})
                ms_order = existing
        else:
            if cfg.test_mode:
                created += 1
                log.info("TEST_MODE_skip_ms_order_create",
                         extra={"order_id": order_uid, "article": article})
                # фейковый объект, чтобы ниже не падало
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
                created += 1
                log.info("ms_order_created",
                         extra={"order_id": order_uid, "ms_id": ms_order.get("id"), "article": article,
                                "supplierStatus": supplier_status, "wbStatus": wb_status})

        # отмена
        if supplier_status == "cancel" or wb_status == "canceled":
            cancelled += 1
            log.info("order_cancelled_seen", extra={"order_id": order_uid, "ms_id": ms_order.get("id")})
            continue

        # complete -> demand
        if supplier_status == "complete":
            if cfg.test_mode:
                log.info("TEST_MODE_skip_ms_demand_create", extra={"order_id": order_uid, "externalCode": ext_code})
            else:
                if not ms.find_demand_by_external_code(ext_code):
                    demand_payload = build_ms_demand_payload(cfg, ms_order)
                    ms.create_demand(demand_payload)
                    demand_created += 1
                    log.info("ms_demand_created", extra={"order_id": order_uid, "externalCode": ext_code})
                else:
                    log.info("ms_demand_exists", extra={"order_id": order_uid, "externalCode": ext_code})

    log.info(
        "done",
        extra={
            "created_count": created,
            "updated-count": updated,
            "skipped_no_article": skipped_no_article,
            "skipped_no_product": skipped_no_product,
            "demand_created": demand_created,
            "cancelled": cancelled,
            "test_mode": cfg.test_mode,
        },
    )


if __name__ == "__main__":
    main()
