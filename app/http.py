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

    def request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None,
                json_body: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"

        max_retries = 6
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
            dt_ms = int((time.time() - t0) * 1000)

            log.info("http_request", extra={
                "method": method, "url": url, "status": resp.status_code, "ms": dt_ms, "attempt": attempt
            })

            # retry on rate limit / transient
            if resp.status_code in (429, 502, 503, 504):
                sleep_s = min(2 ** attempt, 30)
                # если MS прислал Retry-After — используем
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = max(sleep_s, int(ra))
                    except Exception:
                        pass

                log.warning("http_retry", extra={
                    "method": method, "url": url, "status": resp.status_code, "sleep_s": sleep_s
                })
                time.sleep(sleep_s)
                continue

            if resp.status_code >= 400:
                try:
                    body = resp.json()
                except Exception:
                    body = resp.text[:2000]
                log.error("http_error", extra={
                    "method": method, "url": url, "status": resp.status_code, "body": body
                })
                resp.raise_for_status()

            if resp.status_code == 204 or not resp.text:
                return None
            return resp.json()

        # если все ретраи исчерпаны
        resp.raise_for_status()
