"""HTTP helper with timeouts, bounded retries, and redirect re-validation.

Redirects are not followed automatically. Each Location is checked against the
host allowlist and address policy before the next request.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from app.core.config import Settings
from app.core.exceptions import PermanentProcessingError, TransientProcessingError
from app.core.security import assert_url_allowed, next_redirect_url


def _retry_delay(response: httpx.Response | None, attempt: int, settings: Settings) -> float:
    if response is not None:
        retry_after = response.headers.get("retry-after")
        if retry_after and retry_after.isdigit():
            return min(float(retry_after), 30.0)
    return min(settings.http_retry_backoff_seconds * (2**attempt), 30.0)


def request_json(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    settings: Settings,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    current = assert_url_allowed(url, settings)
    redirects = 0
    last_error: Exception | None = None
    for attempt in range(settings.http_max_retries + 1):
        try:
            response = client.request(
                method,
                current,
                params=params,
                headers=headers,
                timeout=settings.http_timeout_seconds,
                follow_redirects=False,
            )
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            last_error = exc
            if attempt >= settings.http_max_retries:
                raise TransientProcessingError(
                    "Source API request failed after retries", category="network"
                ) from exc
            time.sleep(_retry_delay(None, attempt, settings))
            continue
        if response.status_code in {301, 302, 303, 307, 308}:
            if redirects >= settings.max_redirects:
                raise PermanentProcessingError(
                    "Source API redirected too many times", category="network"
                )
            location = response.headers.get("location")
            if not location:
                raise PermanentProcessingError(
                    "Redirect was missing a Location header", category="network"
                )
            current = next_redirect_url(current, location, settings)
            redirects += 1
            params = None
            continue
        if response.status_code == 429 or response.status_code >= 500:
            last_error = TransientProcessingError(
                f"Source API returned HTTP {response.status_code}",
                category="rate_limit" if response.status_code == 429 else "upstream",
            )
            if attempt >= settings.http_max_retries:
                raise last_error
            time.sleep(_retry_delay(response, attempt, settings))
            continue
        if response.status_code >= 400:
            raise PermanentProcessingError(
                f"Source API returned HTTP {response.status_code}", category="upstream"
            )
        if len(response.content) > settings.max_response_bytes:
            raise PermanentProcessingError(
                "Source API response exceeds the configured size limit",
                category="payload_too_large",
            )
        try:
            return response.json()
        except Exception as exc:
            raise PermanentProcessingError(
                "Source API response was not JSON", category="malformed_json"
            ) from exc
    raise TransientProcessingError(
        "Source API request failed after retries", category="network"
    ) from last_error
