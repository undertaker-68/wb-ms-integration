import os
from datetime import timedelta

WB_SUPPLIES_BASE_URL = os.getenv("WB_SUPPLIES_BASE_URL", "https://supplies-api.wildberries.ru")
WB_SUPPLIES_TOKEN = os.getenv("WB_SUPPLIES_TOKEN") or os.getenv("WB_TOKEN")

MS_SALES_CHANNEL_ID = os.getenv("MS_SALES_CHANNEL_ID_FBW")
MS_STATUS_CUSTOMERORDER_ID = os.getenv("MS_FBW_STATUS_CUSTOMERORDER_ID")
MS_STATUS_MOVE_ID = os.getenv("MS_FBW_STATUS_MOVE_ID")
MS_STATUS_DEMAND_ID = os.getenv("MS_FBW_STATUS_DEMAND_ID")

MS_STORE_SOURCE_ID = os.getenv("MS_FBW_STORE_SOURCE_ID")
MS_STORE_WB_ID = os.getenv("MS_FBW_STORE_WB_ID")

STATE_FILE = os.getenv("FBW_STATE_FILE", "fbw_state.json")

SYNC_LOOKBACK_DAYS = int(os.getenv("FBW_LOOKBACK_DAYS", "3"))
