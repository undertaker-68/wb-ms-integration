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
class FBWConfig:
    # WB supplies
    wb_supplies_base_url: str
    wb_supplies_token: str

    # MS
    ms_sales_channel_id_fbw: str
    ms_status_customerorder_id: str
    ms_status_move_id: str
    ms_status_demand_id: str

    ms_store_source_id: str
    ms_store_wb_id: str

    # runtime
    fbw_timezone_offset: str
    state_file: str
    lookback_days: int


def load_fbw_config() -> FBWConfig:
    return FBWConfig(
        wb_supplies_base_url=_opt("WB_SUPPLIES_BASE_URL", "https://supplies-api.wildberries.ru"),
        # Prefer WB_SUPPLIES_TOKEN; if not set, fallback to WB_TOKEN
        wb_supplies_token=_opt("WB_SUPPLIES_TOKEN") or _must("WB_TOKEN"),

        ms_sales_channel_id_fbw=_must("MS_SALES_CHANNEL_ID_FBW"),
        ms_status_customerorder_id=_must("MS_FBW_STATUS_CUSTOMERORDER_ID"),
        ms_status_move_id=_must("MS_FBW_STATUS_MOVE_ID"),
        ms_status_demand_id=_must("MS_FBW_STATUS_DEMAND_ID"),

        ms_store_source_id=_must("MS_FBW_STORE_SOURCE_ID"),
        ms_store_wb_id=_must("MS_FBW_STORE_WB_ID"),

        fbw_timezone_offset=_opt("FBW_TIMEZONE_OFFSET", "+03:00"),
        state_file=_opt("FBW_STATE_FILE", "/root/wb_ms_integration/fbw_state.json"),
        lookback_days=int(_opt("FBW_LOOKBACK_DAYS", "30")),
    )
