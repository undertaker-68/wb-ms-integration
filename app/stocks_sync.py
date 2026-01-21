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

def build_stocks_payload(ms: MSClient, ms_rows: List[Dict], store_id: str) -> Tuple[List[Dict], Dict[str, int]]:
    stats = {"total": 0, "sent": 0, "skipped_no_sku": 0, "product_fetch": 0}
    out: List[Dict] = []
    cache: Dict[str, str] = {}

    import time as _t

    for r in ms_rows:
        stats["total"] += 1

        href = ((r.get("meta") or {}).get("href") or "").strip()
        if not href:
            stats["skipped_no_sku"] += 1
            continue

        if href in cache:
            sku = cache[href]
        else:
            stats["product_fetch"] += 1
            obj = ms.get_by_href(href)
            sku = (obj.get("article") or obj.get("code") or obj.get("externalCode") or "").strip()
            cache[href] = sku
            _t.sleep(0.05)  # чтобы не словить 429

        if not sku:
            stats["skipped_no_sku"] += 1
            continue

        amount = _calc_available_from_stock_by_store(r, store_id)
        out.append({"sku": sku, "amount": amount})
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

    ms = MSClient(ms_http)
    wb = WBClient(wb_http)

    log.info("start", extra={"warehouse_id": cfg.wb_warehouse_id, "ms_store_id": cfg.ms_store_id_wb})

    rows = ms.report_stock_by_store(cfg.ms_store_id_wb)
    stocks, stats = build_stocks_payload(ms, rows, cfg.ms_store_id_wb)

    log.info("prepared", extra=stats)

    if cfg.test_mode:
        log.info(
            "TEST_MODE_on_skip_wb_set_stocks",
            extra={
                "preview": stocks[:5],
                "preview_count": min(5, len(stocks)),
                "total": len(stocks),
            },
        )
        log.info("done_test", extra={"total": stats["total"], "prepared": stats["sent"]})
        return

    total_batches = 0
    for part in chunk(stocks, 1000):
        total_batches += 1
        wb.set_stocks(cfg.wb_warehouse_id, part)
        log.info("batch_sent", extra={"batch": total_batches, "batch_size": len(part)})

    log.info(
        "done",
        extra={
            "batches": total_batches,
            "total": stats["total"],
            "sent": stats["sent"],
            "skipped_no_sku": stats["skipped_no_sku"],
        },
    )


if __name__ == "__main__":
    main()
