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
    order_num = str(wb_order["id"])
    external_code = order_num
    name = order_num

    qty = 1
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


def build_ms_demand_payload(cfg, ms_order: Dict[str, Any], ms_positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    ВАЖНО: Demand не подтягивает позиции сам по customerOrder — их надо передать явно.
    """
    external_code = ms_order.get("externalCode") or ms_order.get("name")

    demand_positions: List[Dict[str, Any]] = []
    for p in ms_positions:
        demand_positions.append({
            "quantity": p.get("quantity", 0),
            "price": p.get("price", 0),
            "assortment": {"meta": (p.get("assortment") or {}).get("meta")},
        })

    payload: Dict[str, Any] = {
        "name": f"WB-{external_code}",
        "externalCode": str(external_code),
        "organization": ms_order["organization"],
        "agent": ms_order["agent"],
        "store": ms_order["store"],
        "customerOrder": {"meta": ms_order["meta"]},
        "positions": demand_positions,
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

    log.info("start", extra={"test_m_
