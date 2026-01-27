import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env", override=True)


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


@dataclass(frozen=True)
class Config:
    # MS
    ms_base_url: str
    ms_token: str
    ms_store_id_wb: str

    # WB Marketplace
    wb_base_url: str
    wb_token: str
    wb_warehouse_id: int

    # WB Content
    wb_content_base_url: str
    wb_content_token: str

    test_mode: bool
    log_level: str
    http_timeout_sec: int


def load_config() -> Config:
    return Config(
        ms_base_url=_opt("MS_BASE_URL", "https://api.moysklad.ru/api/remap/1.2"),
        ms_token=_must("MS_TOKEN"),
        ms_store_id_wb=_must("MS_STORE_ID_WB"),

        wb_base_url=_opt("WB_BASE_URL", "https://marketplace-api.wildberries.ru"),
        wb_token=_must("WB_TOKEN"),
        wb_warehouse_id=int(_must("WB_WAREHOUSE_ID")),

        wb_content_base_url=_opt("WB_CONTENT_BASE_URL", "https://content-api.wildberries.ru"),
        wb_content_token=_must("WB_CONTENT_TOKEN"),

        log_level=_opt("LOG_LEVEL", "INFO"),
        http_timeout_sec=int(_opt("HTTP_TIMEOUT_SEC", "30")),
        test_mode=_bool("TEST_MODE", default=False),
    )
