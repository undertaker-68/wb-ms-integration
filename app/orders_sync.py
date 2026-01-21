import logging
from typing import Any, Dict, List, Optional

from .config import load_config
from .http import HttpClient
from .logging_setup import setup_logging
from .ms_client import MSClient
from .wb_client import WBClient

log = logging.getLogger("orders_sync")

def ms_ref(entity: str, entity_id: str) -> Dict[str, Any]:
    return {"meta": {"type": entity, "href": entity_id}}

def build_ms_order_payload(cfg, wb_order: Dict[str, Any], product: Dict[str, Any]) -> Dict[str, Any]:
    # externalCode = id WB, name тоже (как вы делаете в WB->МС)
    external_code = str(wb_order["id"])
    name = external_code

    qty = 1  # в FBS order запись чаще = 1 штука; если у вас иначе — адаптируем
    price = None

    # цена — дефолтная/продажная цена из МС (как у вас в правилах)
    # (если нет — можно поставить 0)
    # NOTE: MS хранит value в копейках
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

    payload = {
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
    external_code = ms_order.get("externalCode") or ms_order["name"]
    payload = {
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
    # в примере WB есть поле article :contentReference[oaicite:3]{index=3}
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

    log.info("start")

    # 1) Берём новые + “последние 30 дней” (у WB /orders без dateFrom по умолчанию 30 дней) :contentReference[oaicite:4]{index=4}
    new_orders = wb.get_new_orders()
    log.info("wb_new_orders_loaded", count=len(new_orders))

    # дополнительно подберём активные из /orders (если хочешь — можно убрать)
    # Здесь без фильтра по дате, просто “как WB отдаёт”
    listed: List[Dict[str, Any]] = []
    next_ = 0
    for _ in range(10):  # защита от бесконечного цикла
        page = wb.list_orders(limit=1000, next_=next_)
        batch = page.get("orders", [])
        listed.extend(batch)
        next_ = page.get("next", 0)
        log.info("wb_orders_page", got=len(batch), next=next_)
        if not batch:
            break

    # уникализируем по id
    all_orders_by_id: Dict[int, Dict[str, Any]] = {}
    for o in new_orders + listed:
        if "id" in o:
            all_orders_by_id[int(o["id"])] = o
    all_orders = list(all_orders_by_id.values())

    log.info("wb_orders_total", count=len(all_orders))

    # 2) Тянем статусы пачкой
    ids = [int(o["id"]) for o in all_orders if "id" in o]
    statuses = wb.get_orders_status(ids)
    status_by_id: Dict[int, Dict[str, Any]] = {int(s["id"]): s for s in statuses if "id" in s}
    log.info("wb_statuses_loaded", count=len(status_by_id))

    created = updated = skipped_no_product = demand_created = cancelled = 0

    for o in all_orders:
        oid = int(o["id"])
        st = status_by_id.get(oid, {})
        supplier_status = st.get("supplierStatus")  # new/confirm/complete/cancel :contentReference[oaicite:5]{index=5}
        wb_status = st.get("wbStatus")              # waiting/sorted/sold/canceled :contentReference[oaicite:6]{index=6}

        ext_code = str(oid)

        article = extract_article(o)
        if not article:
            log.warning("skip_no_article", order_id=oid)
            continue

        product = ms.find_product_by_article(article)
        if not product:
            skipped_no_product += 1
            log.warning("skip_no_ms_product", order_id=oid, article=article)
            continue

        existing = ms.find_customer_order_by_external_code(ext_code)
        payload = build_ms_order_payload(cfg, o, product)

        if existing:
            ms_id = existing["id"]
            ms.update_customer_order(ms_id, payload)
            updated += 1
            log.info("ms_order_updated", order_id=oid, ms_id=ms_id, article=article,
                     supplierStatus=supplier_status, wbStatus=wb_status)
            ms_order = existing
        else:
            ms_order = ms.create_customer_order(payload)
            created += 1
            log.info("ms_order_created", order_id=oid, ms_id=ms_order.get("id"), article=article,
                     supplierStatus=supplier_status, wbStatus=wb_status)

        # 3) Отмена → снимаем резерв (упрощённо: ставим reserve=0 на позиции, если надо — сделаем точнее)
        if supplier_status == "cancel" or wb_status == "canceled":
            cancelled += 1
            # Здесь можно доработать: получить позиции заказа и обновить reserve=0.
            log.info("order_cancelled_seen", order_id=oid, ms_id=ms_order.get("id"))
            continue

        # 4) complete → создаём Demand (идемпотентно по externalCode)
        if supplier_status == "complete":
            if not ms.find_demand_by_external_code(ext_code):
                demand_payload = build_ms_demand_payload(cfg, ms_order)
                ms.create_demand(demand_payload)
                demand_created += 1
                log.info("ms_demand_created", order_id=oid, externalCode=ext_code)
            else:
                log.info("ms_demand_exists", order_id=oid, externalCode=ext_code)

    log.info("done",
             created=created, updated=updated, skipped_no_product=skipped_no_product,
             demand_created=demand_created, cancelled=cancelled)

if __name__ == "__main__":
    main()
