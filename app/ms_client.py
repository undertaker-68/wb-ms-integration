import logging
from typing import Any, Dict, Optional, List

log = logging.getLogger("ms_client")


class MSClient:
    def __init__(self, http):
        self.http = http

    def find_product_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        # Ищем по артикулу (code/article)
        resp = self.http.request("GET", "/entity/product", params={"filter": f"article={article}"})
        rows = resp.get("rows") if isinstance(resp, dict) else None
        if rows:
            return rows[0]
        # иногда артикул лежит в code
        resp = self.http.request("GET", "/entity/product", params={"filter": f"code={article}"})
        rows = resp.get("rows") if isinstance(resp, dict) else None
        if rows:
            return rows[0]
        return None

    def create_customer_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/customerorder", json_body=payload)

    def find_customer_order_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        resp = self.http.request("GET", "/entity/customerorder", params={"filter": f"externalCode={external_code}"})
        rows = resp.get("rows") if isinstance(resp, dict) else None
        if rows:
            return rows[0]
        return None

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        resp = self.http.request("GET", "/entity/demand", params={"filter": f"externalCode={external_code}"})
        rows = resp.get("rows") if isinstance(resp, dict) else None
        if rows:
            return rows[0]
        return None

    def create_demand(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/demand", json_body=payload)

    # --- НОВОЕ: получить позиции CustomerOrder ---
    def get_customer_order_positions(self, ms_order: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        В ответе списка customerorder обычно нет positions.
        Позиции надо брать отдельным запросом: /entity/customerorder/{id}/positions
        """
        href = (ms_order.get("meta") or {}).get("href") or ""
        if not href:
            return []
        # href вида: https://api.moysklad.ru/api/remap/1.2/entity/customerorder/<id>
        order_id = href.rstrip("/").split("/")[-1]
        resp = self.http.request("GET", f"/entity/customerorder/{order_id}/positions")
        rows = resp.get("rows") if isinstance(resp, dict) else None
        return rows or []

    def update_customer_order_state(self, ms_order: Dict[str, Any], state_id: str) -> Dict[str, Any]:
        """
        Идемпотентно обновляет статус CustomerOrder.
        Ничего не делает, если уже стоит нужный статус.
        """
        if not state_id:
            return ms_order

        href = (ms_order.get("meta") or {}).get("href") or ""
        if not href:
            return ms_order
        order_id = href.rstrip("/").split("/")[-1]

        current_state_href = ((ms_order.get("state") or {}).get("meta") or {}).get("href") or ""
        target_href = f"{self.http.base_url}/entity/customerorder/metadata/states/{state_id}"

        # Уже нужный статус
        if current_state_href == target_href:
            return ms_order

        payload = {"state": {"meta": {"type": "state", "href": target_href}}}
        updated = self.http.request("PUT", f"/entity/customerorder/{order_id}", json_body=payload)
        return updated or ms_order
