import logging
from typing import Dict, List, Tuple

from .config import load_config
from .http import HttpClient
from .logging_setup import setup_logging
from .ms_client import MSClient
from .wb_client import WBClient

log = logging.getLogger("stocks_sync")


def _calc_available_from_stock_by_store(row: Dict, store_id: str) -> int:
    for s in row.get("stockByStore", []):
        meta = (s.get("meta") or {})
        href = meta.get("href", "")
        if store_id in href:
            stock = int(s.get("stock", 0) or 0)
            reserve = int(s.get("reserve", 0) or 0)
            return max(stock - reserve, 0)
    return 0


def wb_build_vendorcode_to_chrt(content_http: HttpClient) -> Dict[str, int]:
    out: Dict[str, int] = {}

    cursor = {"limit": 100}
    filter_ = {"withPhoto": -1}

    while True:
        payload = {"settings": {"cursor": cursor, "filter": filter_}}
        data = content_http.request("POST", "/content/v2/get/cards/list", json_body=payload)
        cards = (data or {}).get("cards") or []
        cur = (data or {}).get("cursor") or {}

        for c in cards:
            vc = (c.get("vendorCode") or "").strip()
            if not vc:
                continue
            sizes = c.get("sizes") or []
            if not sizes:
                continue
            chrt = sizes[0].get("chrtID")
            if isinstance(chrt, int):
                out[vc] = chrt

        if len(cards) < int(cursor.get("limit", 100)):
            break

        # пагинация
        cursor = {
            "limit": int(cursor.get("limit", 100)),
            "updatedAt": cur.get("updatedAt"),
            "nmID": cur.get("nmID"),
        }

    return out


def build_stocks_payload(
    ms: MSClient,
    ms_rows: List[Dict],
    store_id: str,
    vc_to_chrt: Dict[str, int],
) -> Tuple[List[Dict], Dict[str, int]]:
    stats = {"total": 0, "sent": 0, "skipped_no_vendorcode": 0, "skipped_no_chrt": 0, "product_fetch": 0}
    out: List[Dict] = []
    cache: Dict[str, str] = {}

    import time as _t

    for r in ms_rows:
        stats["total"] += 1

        href = ((r.get("meta") or {}).get("href") or "").strip()
        if not href:
            stats["skipped_no_vendorcode"] += 1
            continue

        if href in cache:
            vendor_code = cache[href]
        else:
            stats["product_fetch"] += 1
            obj = ms.get_by_href(href)
            vendor_code = (obj.get("article") or obj.get("code") or obj.get("externalCode") or "").strip()
            cache[href] = vendor_code
            _t.sleep(0.05)

        if not vendor_code:
            stats["skipped_no_vendorcode"] += 1
            continue

        chrt_id = vc_to_chrt.get(vendor_code)
        if not chrt_id:
            stats["skipped_no_chrt"] += 1
            continue

        amount = _calc_available_from_stock_by_store(r, store_id)
        out.append({"chrtId": int(chrt_id), "amount": int(amount)})
        stats["sent"] += 1

    return out, stats


def chunk(lst: List[Dict], n: int) -> List[List[Dict]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


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
    content_http = HttpClient(
        cfg.wb_content_base_url,
        headers={"Authorization": cfg.wb_content_token, "Content-Type": "application/json"},
        timeout=cfg.http_timeout_sec,
    )

    ms = MSClient(ms_http)
    wb = WBClient(wb_http)

    log.info("start", extra={"warehouse_id": cfg.wb_warehouse_id, "ms_store_id": cfg.ms_store_id_wb})

    vc_to_chrt = wb_build_vendorcode_to_chrt(content_http)
    log.info("wb_cards_loaded", extra={"vendorCodes": len(vc_to_chrt)})

    rows = ms.report_stock_by_store(cfg.ms_store_id_wb)
    stocks, stats = build_stocks_payload(ms, rows, cfg.ms_store_id_wb, vc_to_chrt)
    log.info("prepared", extra=stats)

    if cfg.test_mode:
        log.info("TEST_MODE_on_skip_wb_set_stocks", extra={"preview": stocks[:5], "total": len(stocks)})
        return

    batch_no = 0
    for part in chunk(stocks, 1000):
        batch_no += 1
        resp = wb.set_stocks_by_chrt(cfg.wb_warehouse_id, part)

        # успех: 204 => None
        if resp is None:
            log.info("batch_ok", extra={"batch": batch_no, "batch_size": len(part)})
            continue

        # ошибки валидации WB (409 и т.п.)
        if isinstance(resp, dict) and resp.get("status") == 409:
            body = resp.get("body")
            log.warning("batch_conflict", extra={"batch": batch_no, "body": body})
            # просто продолжаем: ODC/несовместимые пропускаем
            continue

        log.info("batch_sent", extra={"batch": batch_no, "batch_size": len(part), "resp": resp})

    log.info("done", extra={"batches": batch_no, **stats})


if __name__ == "__main__":
    main()
