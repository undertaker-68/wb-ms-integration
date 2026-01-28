import logging
from typing import Any, Dict, Optional, List

log = logging.getLogger("ms_client")


class MSClient:
    def __init__(self, http):
        self.http = http

    def get_by_href(self, href: str) -> Dict[str, Any]:
        """GET по абсолютному href (meta.href) из МойСклад."""
        href = (href or "").strip()
        if not href:
            return {}

        base = (self.http.base_url or "").rstrip("/")

        if href.startswith("/"):
            path = href
        else:
            if base and href.startswith(base):
                path = href[len(base):]
            else:
                marker = "/api/remap/1.2"
                if marker in href:
                    path = href.split(marker, 1)[1]
                else:
                    path = "/" + href.lstrip("/")

        return self.http.request("GET", path) or {}

    def report_stock_by_store(self, store_id: str, *, limit: int = 1000) -> List[Dict[str, Any]]:
        """Отчет МС: остатки по складу (/report/stock/bystore) с пагинацией."""
        base = (self.http.base_url or "").rstrip("/")
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
        """Найти товар/вариант в МС по артикулу (article). Возвращает 1-й найденный."""
        article = (article or "").strip()
        if not article:
            return None

        # 1) Сначала ищем по товарам
        r = self.http.request(
            "GET",
            "/entity/product",
            params={"filter": f"article={article}", "limit": 1},
        )
        rows = (r or {}).get("rows") if isinstance(r, dict) else None
        if rows:
            return rows[0]

        # 2) Потом по вариантам (если артикул на variant)
        r = self.http.request(
            "GET",
            "/entity/variant",
            params={"filter": f"article={article}", "limit": 1},
        )
        rows = (r or {}).get("rows") if isinstance(r, dict) else None
        if rows:
            return rows[0]

        return None
