"""Klien tipis untuk OpenAQ API v3: auth, paginasi, retry, dan rate limit."""
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

BASE_URL = "https://api.openaq.org/v3"
ROOT = Path(__file__).resolve().parent.parent
RETRY_STATUS = {429, 500, 502, 503, 504}


class OpenAQClient:
    def __init__(self, api_key: str | None = None, max_retries: int = 5, timeout: float = 30):
        load_dotenv(ROOT / ".env")
        api_key = api_key or os.getenv("OPENAQ_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAQ_API_KEY tidak ditemukan di environment / .env")
        self.max_retries = max_retries
        self._http = httpx.Client(
            base_url=BASE_URL, headers={"X-API-Key": api_key}, timeout=timeout
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "OpenAQClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def get(self, path: str, **params: Any) -> dict:
        """GET satu halaman. Retry dengan backoff pada 429/5xx dan error jaringan."""
        params = {k: v for k, v in params.items() if v is not None}
        for attempt in range(self.max_retries + 1):
            try:
                resp = self._http.get(path, params=params)
            except httpx.TransportError:
                if attempt == self.max_retries:
                    raise
                time.sleep(2**attempt)
                continue

            if resp.status_code in RETRY_STATUS and attempt < self.max_retries:
                # x-ratelimit-reset = detik sampai jendela rate limit direset
                reset = resp.headers.get("x-ratelimit-reset")
                wait = float(reset) + 1 if reset and resp.status_code == 429 else 2**attempt
                time.sleep(wait)
                continue

            resp.raise_for_status()
            # Hindari kena 429: kalau sisa kuota habis, tunggu reset dulu.
            if resp.headers.get("x-ratelimit-remaining") == "0":
                time.sleep(float(resp.headers.get("x-ratelimit-reset", 1)) + 1)
            return resp.json()
        raise RuntimeError("unreachable")

    def paginate(self, path: str, limit: int = 1000, **params: Any) -> Iterator[dict]:
        """Iterasi semua item `results` dari endpoint berpaginasi."""
        page = 1
        while True:
            body = self.get(path, limit=limit, page=page, **params)
            results = body.get("results", [])
            yield from results
            if len(results) < limit:
                return
            page += 1

    # --- endpoint spesifik ---------------------------------------------
    def countries(self) -> Iterator[dict]:
        return self.paginate("/countries")

    def locations(self, countries_id: int) -> Iterator[dict]:
        return self.paginate("/locations", countries_id=countries_id)

    def hours(
        self, sensor_id: int, datetime_from: str | None = None, datetime_to: str | None = None
    ) -> Iterator[dict]:
        """Agregat per jam sebuah sensor. Tanggal format ISO, mis. '2025-01-01'."""
        return self.paginate(
            f"/sensors/{sensor_id}/hours",
            datetime_from=datetime_from,
            datetime_to=datetime_to,
        )
