import logging
from typing import Any, Dict, Optional

log = logging.getLogger("ms")


class MSClient:
    """
    Минимальный, но полный клиент МойСклад,
    ровно под то, что использует orders_sync.py
    """

    def __init__(self, http, token=None):
        # http — это HttpClient, в нём уже есть base_url и auth
        self.http = http

    # -------------------------
    # helpers
    # -------------------------
    def _state_href(self, entity: str, state_id: str) -> str:
        base = (self.http.base_url or "").rstrip("/")
        return f"{base}/entity/{entity}/metadata/states/{state_id}"

    # -------------------------
    # finders
    # -------------------------
    def find_product_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        article = (article or "").strip()
        if not article:
            return None

        r = self.http.request(
            "GET",
            "/entity/product",
            params={"filter": f"article={article}", "limit": 1},
            raise_for_status=False,
        )
        rows = (r or {}).get("rows") if isinstance(r, dict) else None
        return rows[0] if rows else None

    def find_customer_order_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        r = self.http.request(
            "GET",
            "/entity/customerorder",
            params={"filter": f"externalCode={external_code}", "limit": 1},
            raise_for_status=False,
        )
        rows = (r or {}).get("rows") if isinstance(r, dict) else None
        return rows[0] if rows else None

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        r = self.http.request(
            "GET",
            "/entity/demand",
            params={"filter": f"externalCode={external_code}", "limit": 1},
            raise_for_status=False,
        )
        rows = (r or {}).get("rows") if isinstance(r, dict) else None
        return rows[0] if rows else None

    # -------------------------
    # updates
    # -------------------------
    def update_customer_order_state(self, order: Dict[str, Any], state_id: str) -> Dict[str, Any]:
        order_href = order["meta"]["href"]

        body = {
            "state": {
                "meta": {
                    "href": self._state_href("customerorder", state_id),
                    "type": "state",
                    "mediaType": "application/json",
                }
            }
        }

        return self.http.request("PUT", order_href, json_body=body)

    def update_demand_state(self, demand: Dict[str, Any], state_id: str) -> Dict[str, Any]:
        demand_href = demand["meta"]["href"]

        body = {
            "state": {
                "meta": {
                    "href": self._state_href("demand", state_id),
                    "type": "state",
                    "mediaType": "application/json",
                }
            }
        }

        return self.http.request("PUT", demand_href, json_body=body)
