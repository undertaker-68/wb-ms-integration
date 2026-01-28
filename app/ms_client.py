from __future__ import annotations
from typing import Any, Dict, Iterable, Optional
from .http import HttpClient

class MSClient:
    """
    Обёртка над MoySklad Remap API.
    ВАЖНО: HttpClient сам склеивает url = base_url + path,
    поэтому сюда всегда передаём PATH (начинающийся с /...),
    либо "href", который мы нормализуем до path.
    """

    def __init__(self, http: HttpClient):
        self.http = http
        self.base_url = http.base_url.rstrip("/")

    # ---------------- helpers ----------------

    def _to_path(self, maybe_href_or_path: str) -> str:
        """
        Принимает:
          - "/entity/..." (path)
          - "entity/..." (path без слэша)
          - "https://api.moysklad.ru/api/remap/1.2/entity/..." (href)
        Возвращает path, начинающийся с "/".
        """
        s = (maybe_href_or_path or "").strip()
        if not s:
            raise ValueError("Empty path/href")

        # already full URL
        if s.startswith("http://") or s.startswith("https://"):
            # если это наш base_url + /..., отрежем base_url
            if s.startswith(self.base_url):
                s = s[len(self.base_url):]
            else:
                # на всякий случай: ищем "/entity/..." и режем по нему
                idx = s.find("/entity/")
                if idx >= 0:
                    s = s[idx:]
                else:
                    # fallback: берём всё после домена
                    idx2 = s.find("/", 8)
                    s = s[idx2:] if idx2 >= 0 else "/"

        if not s.startswith("/"):
            s = "/" + s
        return s

    def _state_href(self, entity: str, state_id: str) -> str:
        # В body МС ждёт href в meta.state
        return f"{self.base_url}/entity/{entity}/metadata/states/{state_id}"

    def _meta_href(self, entity: str, entity_id: str) -> str:
        return f"{self.base_url}/entity/{entity}/{entity_id}"

    # ---------------- products ----------------

    def prefetch_products_by_articles(self, articles: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        """
        Возвращает dict: article -> product_json
        Ищем только в /entity/product по "article".
        (варианты отдельно берём точечно через find_variant_by_article)
        """
        out: Dict[str, Dict[str, Any]] = {}
        for a in {x.strip() for x in articles if x and str(x).strip()}:
            p = self.find_product_by_article(a)
            if p:
                out[a] = p
        return out

    def find_product_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        article = str(article).strip()
        if not article:
            return None
        # МС фильтр: article=<...> для product работает
        r = self.http.request("GET", "/entity/product", params={"filter": f"article={article}", "limit": 1})
        rows = (r or {}).get("rows") or []
        return rows[0] if rows else None

    def find_variant_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        """
        В МС у variant НЕ всегда есть поле article (у тебя раньше 412 было).
        Но сейчас в логах variant 200 проходит, значит у тебя уже поправлено/разрешено.
        Если снова словишь 412 — просто перестаньте дергать variant по article,
        или замените на поиск по code/баркоду.
        """
        article = str(article).strip()
        if not article:
            return None
        r = self.http.request("GET", "/entity/variant", params={"filter": f"article={article}", "limit": 1})
        rows = (r or {}).get("rows") or []
        return rows[0] if rows else None

    # ---------------- orders / demands ----------------

    def find_customerorder_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        external_code = str(external_code).strip()
        if not external_code:
            return None
        r = self.http.request(
            "GET",
            "/entity/customerorder",
            params={"filter": f"externalCode={external_code}", "limit": 1},
        )
        rows = (r or {}).get("rows") or []
        return rows[0] if rows else None

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        external_code = str(external_code).strip()
        if not external_code:
            return None
        r = self.http.request(
            "GET",
            "/entity/demand",
            params={"filter": f"externalCode={external_code}", "limit": 1},
        )
        rows = (r or {}).get("rows") or []
        return rows[0] if rows else None

    def update_customer_order_state(self, order: Dict[str, Any], state_id: str) -> Dict[str, Any]:
        """
        order может содержать:
          - meta.href (полный URL)
          - id
        """
        if not state_id:
            return order

        order_href = None
        meta = (order or {}).get("meta") or {}
        if meta.get("href"):
            order_href = meta["href"]
        elif order.get("id"):
            order_href = self._meta_href("customerorder", order["id"])
        else:
            raise RuntimeError("update_customer_order_state: cannot determine order href/id")

        path = self._to_path(order_href)

        body = {"state": {"meta": {"href": self._state_href("customerorder", state_id), "type": "state"}}}
        return self.http.request("PUT", path, json_body=body)

    def update_demand_state(self, demand: Dict[str, Any], state_id: str) -> Dict[str, Any]:
        if not state_id:
            return demand

        demand_href = None
        meta = (demand or {}).get("meta") or {}
        if meta.get("href"):
            demand_href = meta["href"]
        elif demand.get("id"):
            demand_href = self._meta_href("demand", demand["id"])
        else:
            raise RuntimeError("update_demand_state: cannot determine demand href/id")

        path = self._to_path(demand_href)
        body = {"state": {"meta": {"href": self._state_href("demand", state_id), "type": "state"}}}
        return self.http.request("PUT", path, json_body=body)
