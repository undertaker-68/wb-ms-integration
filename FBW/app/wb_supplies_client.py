from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from app.http import HttpClient


class WBSuppliesClient:
    """WB FBW Supplies API client (supplies-api.wildberries.ru)."""

    def __init__(self, http: HttpClient):
        self.http = http

    def list_supplies(self, date_from):
        url = "/api/v1/supplies"
        payload = {
            "dateFrom": date_from.isoformat(),
            "limit": 1000
        }
        data = self.http.request("POST", url, json_body=payload)

        # WB иногда возвращает список, а иногда объект с ключом supplies
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("supplies", [])
        return []

    def get_supply(self, supply_id: int | str) -> Dict[str, Any]:
        return self.http.request("GET", f"/api/v1/supplies/{supply_id}")

    def get_goods(self, supply_id: int | str) -> List[Dict[str, Any]]:
        data = self.http.request("GET", f"/api/v1/supplies/{supply_id}/goods")
        return (data or {}).get("goods", [])
