import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _must(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def _opt(name: str, default: str = "") -> str:
    return os.getenv(name, default)

@dataclass(frozen=True)
class Config:
    # MS
    ms_base_url: str
    ms_token: str
    ms_org_id: str
    ms_agent_id_wb: str
    ms_store_id_wb: str
    ms_sales_channel_id_wb: str

    ms_status_new_id: str
    ms_status_shipped_id: str
    ms_status_delivering_id: str
    ms_status_delivered_id: str
    ms_status_cancelled_id: str
    ms_status_cancelled_by_seller_id: str

    # WB
    wb_base_url: str
    wb_token: str
    wb_warehouse_id: int

    test_mode: bool

    # runtime
    log_level: str
    http_timeout_sec: int

def load_config() -> Config:
    return Config(
        ms_base_url=_opt("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2"),
        ms_token=_must("MS_TOKEN"),
        ms_org_id=_must("MS_ORG_ID"),
        ms_agent_id_wb=_must("MS_AGENT_ID_WB"),
        ms_store_id_wb=_must("MS_STORE_ID_WB"),
        ms_sales_channel_id_wb=_must("MS_SALES_CHANNEL_ID_WB"),
        ms_status_new_id=_opt("MS_STATUS_NEW_ID"),
        ms_status_shipped_id=_opt("MS_STATUS_SHIPPED_ID"),
        ms_status_delivering_id=_opt("MS_STATUS_DELIVERING_ID"),
        ms_status_delivered_id=_opt("MS_STATUS_DELIVERED_ID"),
        ms_status_cancelled_id=_opt("MS_STATUS_CANCELLED_ID"),
        ms_status_cancelled_by_seller_id=_opt("MS_STATUS_CANCELLED_BY_SELLER_ID"),
        wb_base_url=_opt("WB_BASE_URL", "https://marketplace-api.wildberries.ru"),
        wb_token=_must("WB_TOKEN"),
        wb_warehouse_id=int(_must("WB_WAREHOUSE_ID")),
        log_level=_opt("LOG_LEVEL", "INFO"),
        http_timeout_sec=int(_opt("HTTP_TIMEOUT_SEC", "30")),
        test_mode=_opt("TEST_MODE", "0") == "1",
    )
