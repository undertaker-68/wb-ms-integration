from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .http import HttpClient


@dataclass
class MSClient:
    """Minimal MoySklad client used by this project.

    Implements only the methods required by:
      - app/orders_sync.py
      - app/stocks_sync.py
    """

    base_url: str
    token: str

    def __post_init__(self) -> None:
        self.http = HttpClient(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {self.token}"},
        )

    # ----------- helpers -----------
    def _state_href(self, entity: str, state_id: str) -> str:
        # e.g. /entity/customerorder/metadata/states/<id>
        return f"{self.base_url}/entity/{entity}/metadata/states/{state_id}"

    def _first_row(self, resp: dict) -> Optional[dict]:
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    # ----------- reports / stocks -----------
    def report_stock_by_store(self, store_id: str) -> dict:
        return self.http.request(
            "GET",
            "/report/stock/bystore",
            params={"store.id": store_id},
        )

    def get_by_href(self, href: str) -> dict:
        # MoySklad returns absolute hrefs.
        return self.http.request("GET", href)

    # ----------- products lookup -----------
    def find_product_by_article(self, article: str) -> Optional[dict]:
        """Find product (or variant) by seller article.

        In MoySklad "article" is a field of Product.
        For Variants the closest stable field is "code" (modification code).
        Some users store article in code for variants, so we try both.
        """

        # 1) Product by article
        r = self.http.request(
            "GET",
            "/entity/product",
            params={"filter": f"article={article}", "limit": 1},
        )
        hit = self._first_row(r)
        if hit:
            return hit

        # 2) Variant by code (common setup)
        r = self.http.request(
            "GET",
            "/entity/variant",
            params={"filter": f"code={article}", "limit": 1},
            raise_for_status=False,
        )
        if isinstance(r, dict) and not r.get("errors"):
            hit = self._first_row(r)
            if hit:
                return hit

        # 3) Product by code (fallback)
        r = self.http.request(
            "GET",
            "/entity/product",
            params={"filter": f"code={article}", "limit": 1},
        )
        return self._first_row(r)

    # ----------- customer orders -----------
    def find_customer_order_by_external_code(self, external_code: str) -> Optional[dict]:
        r = self.http.request(
            "GET",
            "/entity/customerorder",
            params={"filter": f"externalCode={external_code}", "limit": 1},
        )
        return self._first_row(r)

    def create_customer_order(self, payload: dict) -> dict:
        return self.http.request("POST", "/entity/customerorder", json_body=payload)

    def update_customer_order_state(self, order_href: str, state_id: str) -> dict:
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

    def get_customer_order_positions(self, order_href: str) -> dict:
        # order_href is absolute, positions endpoint is {href}/positions
        return self.http.request("GET", f"{order_href}/positions")

    # ----------- demands (shipments) -----------
    def find_demand_by_external_code(self, external_code: str) -> Optional[dict]:
        r = self.http.request(
            "GET",
            "/entity/demand",
            params={"filter": f"externalCode={external_code}", "limit": 1},
        )
        return self._first_row(r)

    def create_demand(self, payload: dict) -> dict:
        return self.http.request("POST", "/entity/demand", json_body=payload)

    def set_demand_applicable(self, demand_href: str, applicable: bool) -> dict:
        return self.http.request("PUT", demand_href, json_body={"applicable": applicable})

    def update_demand_state(self, demand_href: str, state_id: str) -> dict:
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
