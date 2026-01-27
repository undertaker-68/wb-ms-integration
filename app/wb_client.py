import logging
from typing import Any, Dict, List

from .http import HttpClient

log = logging.getLogger("wb")


class WBClient:
    def __init__(self, http: HttpClient):
        self.http = http

    def set_stocks_by_chrt(self, warehouse_id: int, stocks: List[Dict[str, Any]]) -> Any:
        """
        stocks: [{"chrtId": 123, "amount": 10}, ...]
        """
        body = {"stocks": stocks}
        return self.http.request("PUT", f"/api/v3/stocks/{warehouse_id}", json_body=body, raise_for_status=False)
