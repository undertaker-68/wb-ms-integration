import logging
from typing import Any, Dict, List, Optional

from .http import HttpClient

log = logging.getLogger("wb")

class WBClient:
    """
    WB Marketplace API (FBS).
    Docs: /api/v3/orders/new, /api/v3/orders, /api/v3/orders/status and PUT /api/v3/stocks/{warehouseId}. :contentReference[oaicite:2]{index=2}
    """
    def __init__(self, http: HttpClient):
        self.http = http

    def get_new_orders(self) -> List[Dict[str, Any]]:
        data = self.http.request("GET", "/api/v3/orders/new")
        return data.get("orders", []) if isinstance(data, dict) else []

    def list_orders(self, *, limit: int = 1000, next_: int = 0,
                    date_from: Optional[int] = None, date_to: Optional[int] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "next": next_}
        if date_from is not None:
            params["dateFrom"] = date_from
        if date_to is not None:
            params["dateTo"] = date_to
        return self.http.request("GET", "/api/v3/orders", params=params)

    def get_orders_status(self, ids):
        # WB ждёт именно список чисел в поле orders
        payload = {"orders": [int(x) for x in ids]}
        data = self.http.request("POST", "/api/v3/orders/status", json_body=payload)
        if isinstance(data, dict):
            return data.get("orders", []) or []
        return []

    def set_stocks(self, warehouse_id: int, stocks: List[Dict[str, Any]]) -> Any:
        """
        stocks: [{"sku": "vendorCode-or-barcode", "amount": 10}, ...]
        Важно: на вашей стороне решаем, что "sku" = vendorCode/артикул товара в WB.
        """
        body = {"stocks": stocks}
        return self.http.request("PUT", f"/api/v3/stocks/{warehouse_id}", json_body=body)
