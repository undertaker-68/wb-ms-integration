import requests
from datetime import datetime, timedelta
from .config_fbw import WB_SUPPLIES_BASE_URL, WB_SUPPLIES_TOKEN

HEADERS = {
    "Authorization": WB_SUPPLIES_TOKEN,
    "Content-Type": "application/json"
}

class WBSuppliesClient:

    def list_supplies(self, date_from):
        url = f"{WB_SUPPLIES_BASE_URL}/api/v1/supplies"
        payload = {
            "dateFrom": date_from.isoformat(),
            "limit": 1000
        }
        r = requests.post(url, json=payload, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json().get("supplies", [])

    def get_supply(self, supply_id):
        url = f"{WB_SUPPLIES_BASE_URL}/api/v1/supplies/{supply_id}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()

    def get_goods(self, supply_id):
        url = f"{WB_SUPPLIES_BASE_URL}/api/v1/supplies/{supply_id}/goods"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json().get("goods", [])
