from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import os
import time
import random
import requests


@dataclass(frozen=True)
class FredConfig:
    api_key: str
    base_url: str = "https://api.stlouisfed.org/fred"
    timeout_s: int = 30

    # conservative pacing
    min_delay_s: float = 0.25
    max_retries: int = 6
    backoff_base_s: float = 0.7


class FredClient:
    def __init__(self, cfg: FredConfig):
        if not cfg.api_key:
            raise RuntimeError("FRED_API_KEY is missing/empty in environment.")
        self.cfg = cfg
        self._session = requests.Session()
        self._last_call_ts = 0.0

    @staticmethod
    def from_env() -> "FredClient":
        key = os.getenv("FRED_API_KEY", "").strip()
        return FredClient(FredConfig(api_key=key))

    def _sleep_rate_limit(self) -> None:
        now = time.time()
        dt = now - self._last_call_ts
        if dt < self.cfg.min_delay_s:
            time.sleep(self.cfg.min_delay_s - dt)
        self._last_call_ts = time.time()

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.cfg.base_url}/{path.lstrip('/')}"
        params = dict(params)
        params["api_key"] = self.cfg.api_key
        params["file_type"] = "json"

        for attempt in range(self.cfg.max_retries + 1):
            self._sleep_rate_limit()
            try:
                r = self._session.get(url, params=params, timeout=self.cfg.timeout_s)
                if r.status_code == 200:
                    return r.json()

                if r.status_code in (429, 500, 502, 503, 504):
                    delay = (self.cfg.backoff_base_s * (2 ** attempt)) + random.random() * 0.25
                    time.sleep(delay)
                    continue

                raise RuntimeError(f"FRED error {r.status_code}: {r.text[:400]}")

            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt >= self.cfg.max_retries:
                    raise RuntimeError(f"FRED request failed after retries: {e}") from e
                delay = (self.cfg.backoff_base_s * (2 ** attempt)) + random.random() * 0.25
                time.sleep(delay)

        raise RuntimeError("Unreachable: retry loop exhausted")

    # ---- endpoints ----

    def series(self, series_id: str) -> Dict[str, Any]:
        return self._request("series", {"series_id": series_id})

    def series_observations(
        self,
        series_id: str,
        *,
        observation_start: str = "1970-01-01",
        observation_end: Optional[str] = None,
        limit: int = 100000,
        offset: int = 0,
        sort_order: str = "asc",
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "series_id": series_id,
            "observation_start": observation_start,
            "limit": limit,
            "offset": offset,
            "sort_order": sort_order,
        }
        if observation_end:
            params["observation_end"] = observation_end
        return self._request("series/observations", params)