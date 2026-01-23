from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from app.http import HttpClient


class WBSuppliesClient:
    """WB FBW Supplies API client (supplies-api.wildberries.ru)."""

    def __init__(self, http: HttpClient):
        self.http = http

    def list_supplies(self, date_from: datetime, *, limit: int = 1000) -> List[Dict[str, Any]]:
        # WB expects RFC3339/ISO string
        payload = {
            "dateFrom": date_from.isoformat(),
            "limit": limit,
        }
        data = self.http.request("POST", "/api/v1/supplies", json_body=payload)
        return (data or {}).get("supplies", [])

    def get_supply(self, supply_id: int | str) -> Dict[str, Any]:
        return self.http.request("GET", f"/api/v1/supplies/{supply_id}")

    def get_goods(self, supply_id: int | str) -> List[Dict[str, Any]]:
        data = self.http.request("GET", f"/api/v1/supplies/{supply_id}/goods")
        return (data or {}).get("goods", [])
