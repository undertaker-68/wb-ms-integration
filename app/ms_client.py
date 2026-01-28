import logging
from typing import Any, Dict, Optional, List

log = logging.getLogger("ms_client")


class MSClient:
    def __init__(self, http):
        self.http = http

    def _to_path(self, href: str) -> str:
        """
        Превращает абсолютный href МойСклад в относительный path для HttpClient,
        но также работает, если передали уже относительный путь.
        """
        href = (href or "").strip()
        if not href:
            return ""

        # уже относительный
        if href.startswith("/"):
            return href

        base = (getattr(self.http, "base_url", "") or "").rstrip("/")
        if base and href.startswith(base):
            p = href[len(base):]
            return p if p.startswith("/") else "/" + p

        # если base_url отличается, режем по стандартному маркеру
        marker = "/api/remap/1.2"
        if marker in href:
            p = href.split(marker, 1)[1]
            return p if p.startswith("/") else "/" + p

        # fallback
        return "/" + href.lstrip("/")

    def get_by_href(self, href: str) -> Dict[str, Any]:
        path = self._to_path(href)
        if not path:
            return {}
        r = self.http.request("GET", path)
        return r or {}

    def report_stock_by_store(self, store_id: str, *, limit: int = 1000) -> List[Dict[str, Any]]:
        base = (getattr(self.http, "base_url", "") or "").rstrip("/")
        store_href = f"{base}/entity/store/{store_id}"

        out: List[Dict[str, Any]] = []
        offset = 0
        while True:
            resp = self.http.request(
                "GET",
                "/report/stock/bystore",
                params={"store": store_href, "limit": limit, "offset": offset},
            )
            rows = (resp or {}).get("rows") if isinstance(resp, dict) else None
            rows = rows or []
            out.extend(rows)
            if len(rows) < limit:
                break
            offset += limit
        return out

    def find_product_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        article = (article or "").strip()
        if not article:
            return None

        # product.article
        resp = self.http.request("GET", "/entity/product", params={"filter": f"article={article}", "limit": 1})
        rows = resp.get("rows") if isinstance(resp, dict) else None
        if rows:
            return rows[0]

        # иногда кладут в code
        resp = self.http.request("GET", "/entity/product", params={"filter": f"code={article}", "limit": 1})
        rows = resp.get("rows") if isinstance(resp, dict) else None
        if rows:
            return rows[0]

        # и иногда в variant.code (если у вас модификации)
        resp = self.http.request("GET", "/entity/variant", params={"filter": f"code={article}", "limit": 1})
        rows = resp.get("rows") if isinstance(resp, dict) else None
        if rows:
            return rows[0]

        return None

    def create_customer_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/customerorder", json_body=payload) or {}

    def find_customer_order_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        resp = self.http.request(
            "GET",
            "/entity/customerorder",
            params={"filter": f"externalCode={external_code}", "limit": 1},
        )
        rows = resp.get("rows") if isinstance(resp, dict) else None
        return rows[0] if rows else None

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        resp = self.http.request(
            "GET",
            "/entity/demand",
            params={"filter": f"externalCode={external_code}", "limit": 1},
        )
        rows = resp.get("rows") if isinstance(resp, dict) else None
        return rows[0] if rows else None

    def create_demand(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.http.request("POST", "/entity/demand", json_body=payload) or {}

    def update_customer_order_state(self, ms_order: Dict[str, Any], state_id: str) -> Dict[str, Any]:
        if not state_id:
            return ms_order

        href = ((ms_order.get("meta") or {}).get("href")) or ""
        if not href:
            return ms_order

        order_id = href.rstrip("/").split("/")[-1]

        base = (getattr(self.http, "base_url", "") or "").rstrip("/")
        target_href = f"{base}/entity/customerorder/metadata/states/{state_id}"

        current_href = (((ms_order.get("state") or {}).get("meta") or {}).get("href")) or ""
        if current_href == target_href:
            return ms_order

        payload = {"state": {"meta": {"type": "state", "href": target_href}}}

        # ✅ важно: тут отправляем относительный path, а не полный href
        updated = self.http.request("PUT", f"/entity/customerorder/{order_id}", json_body=payload)
        return updated or ms_order

    def get_customer_order_positions(self, ms_order: Dict[str, Any]) -> List[Dict[str, Any]]:
        href = ((ms_order.get("meta") or {}).get("href")) or ""
        if not href:
            return []
        order_id = href.rstrip("/").split("/")[-1]
        resp = self.http.request("GET", f"/entity/customerorder/{order_id}/positions")
        rows = resp.get("rows") if isinstance(resp, dict) else None
        return rows or []

    def set_demand_applicable(self, demand: Dict[str, Any], applicable: bool) -> Dict[str, Any]:
        href = ((demand.get("meta") or {}).get("href")) or ""
        if not href:
            return demand
        demand_id = href.rstrip("/").split("/")[-1]
        payload = {"applicable": bool(applicable)}
        updated = self.http.request("PUT", f"/entity/demand/{demand_id}", json_body=payload)
        return updated or demand

    def update_demand_state(self, demand: Dict[str, Any], state_id: str) -> Dict[str, Any]:
        if not state_id:
            return demand

        href = ((demand.get("meta") or {}).get("href")) or ""
        if not href:
            return demand

        demand_id = href.rstrip("/").split("/")[-1]

        base = (getattr(self.http, "base_url", "") or "").rstrip("/")
        target_href = f"{base}/entity/demand/metadata/states/{state_id}"

        current_href = (((demand.get("state") or {}).get("meta") or {}).get("href")) or ""
        if current_href == target_href:
            return demand

        payload = {"state": {"meta": {"type": "state", "href": target_href}}}
        updated = self.http.request("PUT", f"/entity/demand/{demand_id}", json_body=payload)
        return updated or demand
