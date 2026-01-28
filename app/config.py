import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=True)

def _env(name: str, default=None, required=False):
    v = os.getenv(name, default)
    if required and v is None:
        raise RuntimeError(f"ENV {name} is required")
    return v

def _must(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _opt(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off", ""}:
        return False
    return True

@dataclass
class Config:
    # MoySklad
    ms_base_url: str
    ms_token: str
    ms_store_id_wb: str
    ms_status_new_id: str
    ms_status_confirm_id: str
    ms_status_confirm2_id: str
    ms_status_shipped_id: str
    ms_status_delivering_id: str
    ms_status_delivered_id: str
    ms_status_cancelled_id: str
    ms_status_cancelled_by_seller_id: str
    ms_demand_status_id: str

    # WB
    wb_base_url: str
    wb_token: str
    wb_warehouse_id: int
    wb_content_base_url: str
    wb_content_token: str

    # runtime
    test_mode: bool
    log_level: str
    http_timeout_sec: int

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            ms_base_url=_env("MS_BASE_URL", required=True),
            ms_token=_env("MS_TOKEN", required=True),
            ms_store_id_wb=_env("MS_STORE_ID_WB", required=True),

            ms_status_new_id=_env("MS_STATUS_NEW_ID"),
            ms_status_confirm_id=_env("MS_STATUS_CONFIRM_ID"),
            ms_status_confirm2_id=_env("MS_STATUS_CONFIRM2_ID"),
            ms_status_shipped_id=_env("MS_STATUS_SHIPPED_ID"),
            ms_status_delivering_id=_env("MS_STATUS_DELIVERING_ID"),
            ms_status_delivered_id=_env("MS_STATUS_DELIVERED_ID"),
            ms_status_cancelled_id=_env("MS_STATUS_CANCELLED_ID"),
            ms_status_cancelled_by_seller_id=_env("MS_STATUS_CANCELLED_BY_SELLER_ID"),
            ms_demand_status_id=_env("MS_DEMAND_STATUS_ID"),

            wb_base_url=_env("WB_BASE_URL", required=True),
            wb_token=_env("WB_TOKEN", required=True),
            wb_warehouse_id=int(_env("WB_WAREHOUSE_ID", required=True)),

            wb_content_base_url=_env("WB_CONTENT_BASE_URL", "https://content-api.wildberries.ru"),
            wb_content_token=_env("WB_CONTENT_TOKEN", required=True),

            test_mode=_env("TEST_MODE", "false").lower() == "true",
            log_level=_env("LOG_LEVEL", "INFO"),
            http_timeout_sec=int(_env("HTTP_TIMEOUT_SEC", "30")),
        )
        
def load_config() -> Config:
    return Config(
        ms_base_url=_opt("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2"),
        ms_token=_must("MS_TOKEN"),
        ms_store_id_wb=_must("MS_STORE_ID_WB"),
        ms_status_new_id=_opt("MS_STATUS_NEW_ID", ""),
        ms_status_confirm_id=_opt("MS_STATUS_CONFIRM_ID", ""),
        ms_status_confirm2_id=_opt("MS_STATUS_CONFIRM2_ID", ""),
        ms_status_shipped_id=_opt("MS_STATUS_SHIPPED_ID", ""),
        ms_status_delivering_id=_opt("MS_STATUS_DELIVERING_ID", ""),
        ms_status_delivered_id=_opt("MS_STATUS_DELIVERED_ID", ""),
        ms_status_cancelled_id=_opt("MS_STATUS_CANCELLED_ID", ""),
        ms_status_cancelled_by_seller_id=_opt("MS_STATUS_CANCELLED_BY_SELLER_ID", ""),
        ms_demand_status_id=_opt("MS_DEMAND_STATUS_ID", ""),

        wb_base_url=_opt("WB_BASE_URL", "https://marketplace-api.wildberries.ru"),
        wb_token=_must("WB_TOKEN"),
        wb_warehouse_id=int(_must("WB_WAREHOUSE_ID")),

        wb_content_base_url=_opt("WB_CONTENT_BASE_URL", "https://content-api.wildberries.ru"),
        wb_content_token=_must("WB_CONTENT_TOKEN"),

        log_level=_opt("LOG_LEVEL", "INFO"),
        http_timeout_sec=int(_opt("HTTP_TIMEOUT_SEC", "30")),
        test_mode=_bool("TEST_MODE", default=False),
    )
