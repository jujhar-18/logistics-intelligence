"""Small shared helpers for API scripts that run locally or as Databricks scripts."""

from __future__ import annotations

import os
import time
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import requests


def output_directory(kind: str, cli_value: str | None = None) -> Path:
    """Choose an explicit folder first, then LOGISTICS_RAW_DIR, then a local folder."""
    if cli_value:
        root = Path(cli_value)
    elif os.environ.get("LOGISTICS_RAW_DIR"):
        root = Path(os.environ["LOGISTICS_RAW_DIR"])
    else:
        root = Path("data/raw")
    folder = root if root.name == kind else root / kind
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def get_json_text(url: str, attempts: int = 5, timeout_seconds: int = 30) -> str:
    """Return the provider's original JSON text after checking it parses."""
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                timeout=timeout_seconds,
                headers={"User-Agent": "Innov8-Student-Logistics-Pipeline/1.0"},
            )
            response.raise_for_status()
            # Parse once to catch an HTML error page or broken JSON, but return
            # the original response text so the raw layer does not reformat it.
            json.loads(response.text)
            return response.text
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            if status_code is not None and 400 <= status_code < 500 and status_code != 429:
                raise RuntimeError(
                    f"The API rejected this request with HTTP {status_code}. "
                    "Check the endpoint, country code, and year."
                ) from exc
            if attempt < attempts:
                retry_after = response.headers.get("Retry-After") if response is not None else None
                wait_seconds = min(5 * (2 ** (attempt - 1)), 60)
                if retry_after:
                    try:
                        wait_seconds = max(1, min(300, int(float(retry_after))))
                    except ValueError:
                        try:
                            retry_time = parsedate_to_datetime(retry_after)
                            wait_seconds = max(
                                1,
                                min(300, int((retry_time - datetime.now(timezone.utc)).total_seconds())),
                            )
                        except (TypeError, ValueError):
                            pass
                print(
                    f"Could not read {url} (attempt {attempt}/{attempts}). "
                    f"Waiting {wait_seconds} seconds, then trying again."
                )
                time.sleep(wait_seconds)
    raise RuntimeError(
        f"Could not download API data after {attempts} attempts. "
        "Check the internet connection, API address, and date range. "
        f"Last error: {last_error}"
    ) from last_error


def get_json(url: str, attempts: int = 4, timeout_seconds: int = 30) -> Any:
    """Convenience helper for code that needs parsed JSON rather than raw text."""
    return json.loads(get_json_text(url, attempts, timeout_seconds))
