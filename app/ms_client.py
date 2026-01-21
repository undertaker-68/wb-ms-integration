import logging
from typing import Any, Dict, List, Optional

from .http import HttpClient

log = logging.getLogger("ms")

class MSClient:
    def __init__(self, http: HttpClient):
        self.http = http

    # ---- Products ----
    def find_product_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        # Ищем по article (в МС это "article"), fallback — по code.
        # Через filter=article=xxx
        data = self.http.request("GET", "/entity/product", params={"filter": f"article={article}"})
        rows = data.get("rows", [])
        if rows:
            return rows[0]
        data = self.http.request("GET", "/entity/product", params={"filter": f"code={article}"})
        rows = data.get("rows", [])
        return rows[0] if rows else None

    def get_product_sale_price(self, product: Dict[str, Any]) -> Optional[float]:
        # Берём минимально: если есть salePrices — берём первую.
        prices = product.get("salePrices") or []
        if prices:
            # MS хранит в копейках (value) и currency.
            value = prices[0].get("value")
            if value is not None:
                return float(value) / 100
        return None

    # ---- Stock report ----
    def report_stock_by_store(self, store_id: str) -> List[Dict[str, Any]]:
        # /report/stock/bystore?store.id=...
        data = self.http.request("GET", "/report/stock/bystore", params={"store.id": store_id})
        return data.get("rows", []) if isinstance(data, dict) else []

    # ---- CustomerOrder ----
    def find_customer_order_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/customerorder", params={"filter": f"externalCode={external_code}"})
        rows = data.get("rows", [])
        return rows[0] if rows else None

    def create_customer_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/customerorder", json_body=payload)

    def update_customer_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("PUT", f"/entity/customerorder/{order_id}", json_body=payload)

    # ---- Demand (shipment) ----
    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/demand", params={"filter": f"externalCode={external_code}"})
        rows = data.get("rows", [])
        return rows[0] if rows else None

    def create_demand(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/demand", json_body=payload)

    def search_products(self, text: str, limit: int = 1000) -> List[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/product", params={"search": text, "limit": limit})
        return data.get("rows", []) if isinstance(data, dict) else []

    def get_by_href(self, href: str) -> Dict[str, Any]:
        # href приходит полный, типа https://api.moysklad.ru/api/remap/1.2/entity/product/<id>
        # превращаем в path для нашего HttpClient
        base = self.http.base_url.rstrip("/")
        if href.startswith(base):
            path = href[len(base):]
        else:
            # на всякий случай
            path = href
        return self.http.request("GET", path)

    def get_by_href(self, href: str) -> Dict[str, Any]:
        # href приходит полный, типа https://api.moysklad.ru/api/remap/1.2/entity/product/<id>
        # превращаем в path для нашего HttpClient
        base = self.http.base_url.rstrip("/")
        if href.startswith(base):
            path = href[len(base):]
        else:
            # на всякий случай
            path = href
        return self.http.request("GET", path)

