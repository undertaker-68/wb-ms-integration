import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from app.config import load_config
from app.http import HttpClient
from app.logging_setup import setup_logging
from app.ms_client import MSClient

from .config_fbw import load_fbw_config
from .wb_supplies_client import WBSuppliesClient

log = logging.getLogger("fbw_supplies_sync")


def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        # WB приходит как 2026-01-23T11:28:45+03:00 -> fromisoformat ок
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _normalize_moment(raw: str, tz_offset: str) -> str:
    """Make WB 'plan date' acceptable for MS.

    WB can return date-only or datetime without timezone; MS prefers ISO with offset.
    """
    s = (raw or "").strip()
    if not s:
        return s

    # date-only
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return f"{s}T00:00:00{tz_offset}"

    # datetime with 'Z' or explicit offset
    if s.endswith("Z") or "+" in s[-6:] or "-" in s[-6:]:
        return s.replace("Z", "+00:00")

    # datetime without offset
    if "T" in s:
        return f"{s}{tz_offset}"

    return s


def _load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"bootstrappedAt": None, "supplies": {}}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_state(path: str, state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _comment(number: str, dest: str) -> str:
    number = (number or "").strip()
    dest = (dest or "").strip()
    return f"{number} - {dest}" if dest else number


def _extract_article(wb_good: Dict[str, Any]) -> str:
    # You said "в WB кажется тоже называется article". Keep fallback for real-life payloads.
    return str(
        wb_good.get("article")
        or wb_good.get("vendorCode")
        or wb_good.get("supplierArticle")
        or ""
    ).strip()


def _extract_qty(wb_good: Dict[str, Any]) -> float:
    v = wb_good.get("quantity")
    if v is None:
        v = wb_good.get("qty")
    try:
        return float(v)
    except Exception:
        return 0.0


def _ensure_customerorder(
    *,
    cfg,
    fbw_cfg,
    ms: MSClient,
    supply_id: int | str,
    number: str,
    plan_date_raw: str,
    dest_name: str,
    goods: list[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    name = f"fbw-{number}"
    external_code = name

    existing = ms.find_customer_order_by_external_code(external_code) or ms.find_customer_order_by_name(name)
    if existing:
        return existing

    positions = []
    not_found: list[str] = []
    for g in goods:
        article = _extract_article(g)
        qty = _extract_qty(g)
        if not article or qty <= 0:
            continue

        product = ms.find_product_by_article(article)
        if not product:
            not_found.append(article)
            continue

        positions.append(ms.make_position(product, qty))

    if not positions:
        log.warning(
            "skip_create_empty_positions",
            extra={"supply_id": supply_id, "number": number, "not_found": not_found},
        )
        return None

    planned = _normalize_moment(plan_date_raw or "", fbw_cfg.fbw_timezone_offset)

    payload: Dict[str, Any] = {
        "name": name,
        "externalCode": external_code,
        "organization": {"meta": {"type": "organization", "href": f"{cfg.ms_base_url}/entity/organization/{cfg.ms_org_id}"}},
        "agent": {"meta": {"type": "counterparty", "href": f"{cfg.ms_base_url}/entity/counterparty/{cfg.ms_agent_id_wb}"}},
        # store в заказе — как в FBS (у тебя было wb store). Оставляю как было в файле.
        "store": {"meta": {"type": "store", "href": f"{cfg.ms_base_url}/entity/store/{cfg.ms_store_id_wb}"}},
        "salesChannel": {"meta": {"type": "saleschannel", "href": f"{cfg.ms_base_url}/entity/saleschannel/{fbw_cfg.ms_sales_channel_id_fbw}"}},
        "state": {"meta": {"type": "state", "href": f"{cfg.ms_base_url}/entity/customerorder/metadata/states/{fbw_cfg.ms_status_customerorder_id}"}},
        "comment": _comment(str(number), dest_name),
        "positions": positions,
    }
    if planned:
        payload["deliveryPlannedMoment"] = planned

    created = ms.create_customer_order(payload)
    log.info(
        "customerorder_created",
        extra={"supply_id": supply_id, "number": number, "ms_id": created.get("id"), "not_found": not_found},
    )
    return created


def _update_planned_date_if_needed(
    *,
    ms: MSClient,
    order: Dict[str, Any],
    plan_date_raw: str,
    tz_offset: str,
) -> None:
    new_val = _normalize_moment(plan_date_raw or "", tz_offset)
    if not new_val:
        return

    current = (order.get("deliveryPlannedMoment") or "").strip()
    if current == new_val:
        return

    ms.update_customer_order_date(order["id"], new_val)
    log.info("customerorder_planned_updated", extra={"order_id": order["id"], "from": current, "to": new_val})


def _ensure_move(
    *,
    cfg,
    fbw_cfg,
    ms: MSClient,
    order: Dict[str, Any],
    external_code: str,
    goods: list[Dict[str, Any]],
    supply_id: int | str,
    number: str,
) -> Optional[Dict[str, Any]]:
    existing = ms.find_move_by_external_code(external_code)
    if existing:
        return existing

    positions = []
    for g in goods:
        article = _extract_article(g)
        qty = _extract_qty(g)
        if not article or qty <= 0:
            continue
        product = ms.find_product_by_article(article)
        if not product:
            continue
        positions.append({"quantity": qty, "assortment": {"meta": product["meta"]}})

    payload: Dict[str, Any] = {
        "name": order.get("name") or f"fbw-{number}",
        "externalCode": external_code,
        "organization": order["organization"],
        "comment": order.get("comment") or "",
        "sourceStore": {"meta": {"type": "store", "href": f"{cfg.ms_base_url}/entity/store/{fbw_cfg.ms_store_source_id}"}},
        "targetStore": {"meta": {"type": "store", "href": f"{cfg.ms_base_url}/entity/store/{fbw_cfg.ms_store_wb_id}"}},
        "state": {"meta": {"type": "state", "href": f"{cfg.ms_base_url}/entity/move/metadata/states/{fbw_cfg.ms_status_move_id}"}},
        "positions": positions,
    }

    created = ms.create_move(payload)
    log.info("move_created", extra={"supply_id": supply_id, "number": number, "ms_id": created.get("id")})

    ok = ms.try_apply_move(created["id"])
    if not ok:
        log.warning("move_apply_failed_keep_unapplied", extra={"move_id": created.get("id")})
    return created


def _ensure_demand(
    *,
    cfg,
    fbw_cfg,
    ms: MSClient,
    order: Dict[str, Any],
    external_code: str,
    goods: list[Dict[str, Any]],
    supply_id: int | str,
    number: str,
) -> Optional[Dict[str, Any]]:
    existing = ms.find_demand_by_external_code(external_code)
    if existing:
        return existing

    positions = []
    for g in goods:
        article = _extract_article(g)
        qty = _extract_qty(g)
        if not article or qty <= 0:
            continue
        product = ms.find_product_by_article(article)
        if not product:
            continue
        positions.append({
            "quantity": qty,
            "price": ms.get_product_sale_price_value(product),
            "assortment": {"meta": product["meta"]},
        })

    payload: Dict[str, Any] = {
        "name": order.get("name") or f"fbw-{number}",
        "externalCode": external_code,
        "organization": order["organization"],
        "agent": order["agent"],
        "store": {"meta": {"type": "store", "href": f"{cfg.ms_base_url}/entity/store/{fbw_cfg.ms_store_wb_id}"}},
        "state": {"meta": {"type": "state", "href": f"{cfg.ms_base_url}/entity/demand/metadata/states/{fbw_cfg.ms_status_demand_id}"}},
        "comment": order.get("comment") or "",
        "customerOrder": {"meta": order["meta"]},
        "positions": positions,
    }

    created = ms.create_demand(payload)
    log.info("demand_created", extra={"supply_id": supply_id, "number": number, "ms_id": created.get("id")})

    ok = ms.try_apply_demand(created["id"])
    if not ok:
        log.warning("demand_apply_failed_keep_unapplied", extra={"demand_id": created.get("id")})
    return created


def main() -> None:
    cfg = load_config()
    fbw_cfg = load_fbw_config()
    setup_logging(cfg.log_level)

    ms_http = HttpClient(
        cfg.ms_base_url,
        headers={"Authorization": f"Bearer {cfg.ms_token}"},
        timeout=cfg.http_timeout_sec,
    )
    wb_http = HttpClient(
        fbw_cfg.wb_supplies_base_url,
        headers={"Authorization": fbw_cfg.wb_supplies_token, "Content-Type": "application/json"},
        timeout=cfg.http_timeout_sec,
    )

    ms = MSClient(ms_http)
    wb = WBSuppliesClient(wb_http)

    state = _load_state(fbw_cfg.state_file)

    # First run: bootstrap only (do not import old supplies)
    if not state.get("bootstrappedAt"):
        state["bootstrappedAt"] = datetime.now(timezone.utc).isoformat()
        _save_state(fbw_cfg.state_file, state)
        log.info("bootstrap_done_no_import", extra={"state_file": fbw_cfg.state_file})
        return

    boot_at = _parse_dt(state["bootstrappedAt"]) or datetime.now(timezone.utc)

    date_from = datetime.now(timezone.utc) - timedelta(days=fbw_cfg.lookback_days)
    supplies = wb.list_supplies(date_from)

    supplies_by_id: Dict[str, Dict[str, Any]] = {}
    for s in supplies:
        # WB returns supplyID, not id
        sid = s.get("supplyID")
        if sid is None:
            continue
        supplies_by_id[str(sid)] = s

    # Ensure orders for NEW supplies (created AFTER bootstrap, and not in state)
    for sid, s in supplies_by_id.items():
        if sid in state["supplies"]:
            continue

        created_dt = _parse_dt(str(s.get("createDate") or ""))
        if created_dt and created_dt.astimezone(timezone.utc) <= boot_at:
            continue  # skip old supplies

        number = str(s.get("supplyID") or "").strip()  # номер в интерфейсе
        if not number:
            continue

        goods = wb.get_goods(sid)

        dest_name = str(
            s.get("warehouseName")
            or s.get("warehouse")
            or s.get("destinationWarehouse")
            or ""
        ).strip()
        plan_date_raw = str(s.get("supplyDate") or s.get("planDate") or "").strip()

        order = _ensure_customerorder(
            cfg=cfg,
            fbw_cfg=fbw_cfg,
            ms=ms,
            supply_id=sid,
            number=number,
            plan_date_raw=plan_date_raw,
            dest_name=dest_name,
            goods=goods,
        )
        if not order:
            continue

        state["supplies"][sid] = {
            "number": number,
            "orderId": order["id"],
            "move": False,
            "demand": False,
        }

    # Process ACTIVE supplies (update plan date; create move/demand by status)
    for sid, info in list(state["supplies"].items()):
        s = supplies_by_id.get(str(sid))
        if not s:
            continue

        number = str(info.get("number") or s.get("supplyID") or "").strip()
        if not number:
            continue

        order = (
            ms.find_customer_order_by_name(f"fbw-{number}")
            or ms.find_customer_order_by_external_code(f"fbw-{number}")
        )
        if not order:
            continue

        status_id = s.get("statusID")
        plan_date_raw = str(s.get("supplyDate") or s.get("planDate") or "").strip()

        # Update plan date while demand not created yet
        if not info.get("demand"):
            _update_planned_date_if_needed(
                ms=ms,
                order=order,
                plan_date_raw=plan_date_raw,
                tz_offset=fbw_cfg.fbw_timezone_offset,
            )

        goods: list[Dict[str, Any]] | None = None

        def _get_goods() -> list[Dict[str, Any]]:
            nonlocal goods
            if goods is None:
                goods = wb.get_goods(sid)
            return goods

        # status 3 -> move
        if status_id == 3 and not info.get("move"):
            move_ext = f"FBW:{order['name']}:MOVE"
            _ensure_move(
                cfg=cfg,
                fbw_cfg=fbw_cfg,
                ms=ms,
                order=order,
                external_code=move_ext,
                goods=_get_goods(),
                supply_id=sid,
                number=number,
            )
            info["move"] = True

        # status 5 -> demand
        if status_id == 5 and not info.get("demand"):
            demand_ext = f"FBW:{order['name']}:DEMAND"
            _ensure_demand(
                cfg=cfg,
                fbw_cfg=fbw_cfg,
                ms=ms,
                order=order,
                external_code=demand_ext,
                goods=_get_goods(),
                supply_id=sid,
                number=number,
            )
            info["demand"] = True

        state["supplies"][sid] = info

    _save_state(fbw_cfg.state_file, state)
    log.info("done", extra={"state_file": fbw_cfg.state_file, "supplies": len(state.get("supplies", {}))})


if __name__ == "__main__":
    main()
