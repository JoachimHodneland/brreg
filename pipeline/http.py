"""Delt HTTP-sesjon med retry og rate limiting."""
import time
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

_RETRY = Retry(
    total=3,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False,
)


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    adapter = HTTPAdapter(max_retries=_RETRY)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_last_call: float = 0.0


def api_get(
    session: requests.Session,
    url: str,
    params: dict = None,
    delay: float = 0.15,
) -> requests.Response:
    """GET med rate limiting. Venter om nødvendig før kallet."""
    global _last_call
    since = time.monotonic() - _last_call
    if since < delay:
        time.sleep(delay - since)
    resp = session.get(url, params=params, timeout=30)
    _last_call = time.monotonic()
    return resp
