"""Retry policy for external HTTP calls.

Used to wrap functions that make a single HTTP request to an external API.
Transient errors (timeouts, connection failures, 429, 5xx) are retried with
exponential backoff and jitter. Structural errors (4xx other than 429, parse
errors) are re-raised immediately.

Do NOT wrap Anthropic SDK calls — the SDK retries internally. Same for the
OpenAI SDK.
"""
import logging

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception,
    before_sleep_log,
)

_log = logging.getLogger(__name__)

TRANSIENT_STATUS = {408, 425, 429, 500, 502, 503, 504}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError,
                        httpx.ConnectError, httpx.RemoteProtocolError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in TRANSIENT_STATUS
    return False


def retry_external(max_attempts: int = 3, max_delay: float = 30.0):
    """Decorator for a function that makes one HTTP call to an external API.

    Caller must ensure response.raise_for_status() is invoked so that
    5xx / 429 surface as httpx.HTTPStatusError (not silent failures).
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential_jitter(initial=1, max=max_delay),
        retry=retry_if_exception(_is_retryable),
        before_sleep=before_sleep_log(_log, logging.WARNING),
        reraise=True,
    )
