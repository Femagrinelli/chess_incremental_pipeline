"""
Chess.com API client.
Designed for daily incremental ingestion with retry, backoff and conditional GET.
"""

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.chess.com/pub"
VALID_TITLES = ["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]


@dataclass
class ChessApiResponse:
    status_code: int
    data: dict
    etag: Optional[str] = None
    last_modified: Optional[str] = None


def _get_headers() -> dict:
    return {
        "User-Agent": os.environ.get(
            "CHESS_API_USER_AGENT",
            "chess-data-platform/1.0 (contact: you@example.com)",
        ),
        "Accept": "application/json",
    }


def _get_delay() -> float:
    return float(os.environ.get("CHESS_API_DELAY", "0.5"))


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _get(
    url: str,
    extra_headers: Optional[dict] = None,
    allow_not_modified: bool = False,
) -> ChessApiResponse:
    headers = _get_headers()
    if extra_headers:
        headers.update(extra_headers)

    logger.info("GET %s", url)
    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        logger.warning("Rate limited. Sleeping %s seconds.", retry_after)
        time.sleep(retry_after)
        response.raise_for_status()

    if response.status_code == 304 and allow_not_modified:
        time.sleep(_get_delay())
        return ChessApiResponse(
            status_code=304,
            data={},
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )

    if response.status_code == 404:
        logger.warning("404 Not Found: %s", url)
        time.sleep(_get_delay())
        return ChessApiResponse(
            status_code=404,
            data={},
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )

    response.raise_for_status()
    time.sleep(_get_delay())
    return ChessApiResponse(
        status_code=response.status_code,
        data=response.json(),
        etag=response.headers.get("ETag"),
        last_modified=response.headers.get("Last-Modified"),
    )


def get_titled_players(title: str) -> dict:
    if title not in VALID_TITLES:
        raise ValueError(f"Invalid title '{title}'. Must be one of {VALID_TITLES}")
    return _get(f"{BASE_URL}/titled/{title}").data


def get_player_archives(username: str) -> dict:
    return _get(f"{BASE_URL}/player/{username}/games/archives").data


def get_games_for_month_if_changed(
    username: str,
    year: str,
    month: str,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
) -> ChessApiResponse:
    extra_headers = {}
    if etag:
        extra_headers["If-None-Match"] = etag
    if last_modified:
        extra_headers["If-Modified-Since"] = last_modified

    return _get(
        f"{BASE_URL}/player/{username}/games/{year}/{str(month).zfill(2)}",
        extra_headers=extra_headers or None,
        allow_not_modified=True,
    )


def extract_archive_month(url: str) -> Optional[str]:
    try:
        parts = url.rstrip("/").split("/")
        year = parts[-2]
        month = parts[-1].zfill(2)
        return f"{year}-{month}"
    except IndexError:
        logger.warning("Could not parse archive URL: %s", url)
        return None


def extract_archive_months(archive_urls: list[str]) -> list[str]:
    months = []
    for url in archive_urls:
        month_key = extract_archive_month(url)
        if month_key:
            months.append(month_key)
    return sorted(set(months))
