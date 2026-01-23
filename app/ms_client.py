import logging
from typing import Any, Dict, List, Optional

from .http import HttpClient

log = logging.getLogger("ms")


def _meta(href: str, type_: str) -> Dict[str, Any]:
    return {"meta": {"href": href, "type": type_}}


class MSClient:
    def __init__(self, http: HttpClient):
        self.http = http

    # ---------------------------
    # Helpers
    # ---------------------------
    def _href(self, path: str) -> str:
        return f"{self.http.base_url}{path}"

    def _state_meta(self, entity: str, state_id: str) -> Dict[str, Any]:
        # entity: customerorder | demand | move
        return _meta(self._href(f"/entity/{entity}/metadata/states/{state_id}"), "state")

    def _store_meta(self, store_id: str) -> Dict[str, Any]:
        return _meta(self._href(f"/entity/store/{store_id}"), "store")

    def _sales_channel_meta(self, sales_channel_id: str) -> Dict[str, Any]:
        return _meta(self._href(f"/entity/saleschannel/{sales_channel_id}"), "saleschannel")

    # ---------------------------
    # Products
    # ---------------------------
    def find_product_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        """
        Ищем по article (в МС это "article"), fallback — по code.
        """
        article = (article or "").strip()
        if not article:
            return None

        data = self.http.request("GET", "/entity/product", params={"filter": f"article={article}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        if rows:
            return rows[0]

        data = self.http.request("GET", "/entity/product", params={"filter": f"code={article}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None

    def search_products(self, text: str, limit: int = 1000) -> List[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/product", params={"search": text, "limit": limit})
        return data.get("rows", []) if isinstance(data, dict) else []

    def get_product_sale_price_value(self, product: Dict[str, Any]) -> int:
        """
        Возвращает цену продажи в МС в 'копейках' (как value в salePrices).
        Берём первую цену из salePrices (как и в текущем FBS коде).
        """
        sale_prices = product.get("salePrices") or []
        if sale_prices and sale_prices[0].get("value") is not None:
            return int(sale_prices[0]["value"])
        return 0

    def make_position(self, product: Dict[str, Any], quantity: float) -> Dict[str, Any]:
        """
        Позиция без резервов, цена = дефолтная "Цена продажи" из МС.
        """
        price_value = self.get_product_sale_price_value(product)
        return {
            "quantity": quantity,
            "price": price_value,
            "assortment": {"meta": product["meta"]},
        }

    # ---------------------------
    # Stock report
    # ---------------------------
    def report_stock_by_store(self, store_id: str) -> List[Dict[str, Any]]:
        data = self.http.request("GET", "/report/stock/bystore", params={"store.id": store_id})
        return data.get("rows", []) if isinstance(data, dict) else []

    # ---------------------------
    # Generic getters
    # ---------------------------
    def get_by_href(self, href: str) -> Dict[str, Any]:
        """
        href приходит полный, типа https://api.moysklad.ru/api/remap/1.2/entity/product/<id>
        превращаем в path для нашего HttpClient
        """
        base = self.http.base_url.rstrip("/")
        if href.startswith(base):
            path = href[len(base):]
        else:
            path = href  # на всякий случай
        return self.http.request("GET", path)

    # ---------------------------
    # CustomerOrder
    # ---------------------------
    def find_customer_order_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/customerorder", params={"filter": f"externalCode={external_code}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None

    def find_customer_order_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/customerorder", params={"filter": f"name={name}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None

    def create_customer_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/customerorder", json_body=payload)

    def update_customer_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("PUT", f"/entity/customerorder/{order_id}", json_body=payload)

    def update_customer_order_date(self, order_id: str, delivery_planned_moment: str) -> Dict[str, Any]:
        """
        Обновляем deliveryPlannedMoment (строкой ISO).
        """
        return self.update_customer_order(order_id, {"deliveryPlannedMoment": delivery_planned_moment})

    # ---------------------------
    # Demand (shipment)
    # ---------------------------
    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/demand", params={"filter": f"externalCode={external_code}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None

    def find_demand_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/demand", params={"filter": f"name={name}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None

    def create_demand(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/demand", json_body=payload)

    def update_demand(self, demand_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("PUT", f"/entity/demand/{demand_id}", json_body=payload)

    # ---------------------------
    # Move (transfer)
    # ---------------------------
    def find_move_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/move", params={"filter": f"externalCode={external_code}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None

    def find_move_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        data = self.http.request("GET", "/entity/move", params={"filter": f"name={name}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        return rows[0] if rows else None

    def create_move(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/move", json_body=payload)

    def update_move(self, move_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("PUT", f"/entity/move/{move_id}", json_body=payload)

    def find_assortment_by_article_vendorcode(self, article: str):
        """
        FBW: ищем по article сначала product, потом variant.
        Fallback: code (product/variant).
        Возвращает product/variant (assortment) с meta.
        """
        article = (article or "").strip()
        if not article:
            return None

        # product.article
        data = self.http.request("GET", "/entity/product", params={"filter": f"article={article}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        if rows:
            return rows[0]

        # variant.article
        data = self.http.request("GET", "/entity/variant", params={"filter": f"article={article}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        if rows:
            return rows[0]

        # product.code
        data = self.http.request("GET", "/entity/product", params={"filter": f"code={article}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        if rows:
            return rows[0]

        # variant.code
        data = self.http.request("GET", "/entity/variant", params={"filter": f"code={article}"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        if rows:
            return rows[0]

        return None

    # ---------------------------
    # Applicable (проведение)
    # ---------------------------
    def try_apply_customerorder(self, order_id: str) -> bool:
        try:
            self.update_customer_order(order_id, {"applicable": True})
            return True
        except Exception as e:
            log.warning("apply_failed_customerorder", extra={"order_id": order_id, "err": str(e)})
            return False

    def try_apply_move(self, move_id: str) -> bool:
        try:
            self.update_move(move_id, {"applicable": True})
            return True
        except Exception as e:
            log.warning("apply_failed_move", extra={"move_id": move_id, "err": str(e)})
            return False

    def try_apply_demand(self, demand_id: str) -> bool:
        try:
            self.update_demand(demand_id, {"applicable": True})
            return True
        except Exception as e:
            log.warning("apply_failed_demand", extra={"demand_id": demand_id, "err": str(e)})
            return False
