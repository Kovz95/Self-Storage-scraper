from __future__ import annotations

from dataclasses import field
import time
from dataclasses import dataclass

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass(slots=True)
class HttpClient:
    timeout_seconds: int = 45
    min_delay_seconds: float = 0.75
    session: Session = field(init=False)
    _last_request_at: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        self.session = build_session()

    def get(self, url: str, **kwargs: object) -> Response:
        self._polite_delay()
        response = self.session.get(url, timeout=self.timeout_seconds, **kwargs)
        response.raise_for_status()
        return response

    def _polite_delay(self) -> None:
        delta = time.time() - self._last_request_at
        if delta < self.min_delay_seconds:
            time.sleep(self.min_delay_seconds - delta)
        self._last_request_at = time.time()


def build_session() -> Session:
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
