cat > app/http.py <<'PY'
import logging
import time
from typing import Any, Dict, Optional

import requests

log = logging.getLogger("http")


class HttpClient:
    def __init__(self, base_url: str, headers: Dict[str, str], timeout: int):
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers)
        self.timeout = timeout
        self.session = requests.Session()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        raise_for_status: bool = True,
    ) -> Any:
        url = f"{self.base_url}{path}"

        max_retries = 6
        last_resp = None
        for attempt in range(max_retries):
            t0 = time.time()
            resp = self.session.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=json_body,
                timeout=self.timeout,
            )
            last_resp = resp
            dt_ms = int((time.time() - t0) * 1000)

            log.info("http_request", extra={
                "method": method, "url": url, "status": resp.status_code, "ms": dt_ms, "attempt": attempt
            })

            if resp.status_code in (429, 502, 503, 504):
                sleep_s = min(2 ** attempt, 30)
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = max(sleep_s, int(ra))
                    except Exception:
                        pass
                log.warning("http_retry", extra={"method": method, "url": url, "status": resp.status_code, "sleep_s": sleep_s})
                time.sleep(sleep_s)
                continue

            if resp.status_code == 204 or not resp.text:
                return None

            try:
                body = resp.json()
            except Exception:
                body = resp.text[:2000]

            if resp.status_code >= 400:
                log.error("http_error", extra={"method": method, "url": url, "status": resp.status_code, "body": body})
                if raise_for_status:
                    resp.raise_for_status()
                return {"status": resp.status_code, "body": body}

            return body

        if last_resp is not None:
            last_resp.raise_for_status()
        raise RuntimeError("HTTP request failed without response")
PY
