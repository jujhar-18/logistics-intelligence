"""Download historical Brazil holidays from the Nager.Date public API v3."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import get_json_text, output_directory


def fetch_holidays(output: str | None = None, years: range = range(2016, 2019)) -> list[Path]:
    """Save each year response as raw JSON so it can be reloaded repeatably."""
    folder = output_directory("holidays", output)
    written: list[Path] = []
    for year in years:
        # API v4 is limited to the current year and five years ahead. This
        # project needs 2016-2018, so it uses v3 while that endpoint remains live.
        url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/BR"
        try:
            destination = folder / f"holidays_BR_{year}.json"
            if destination.exists():
                try:
                    prior = json.loads(destination.read_text(encoding="utf-8"))
                    if isinstance(prior, list) and prior:
                        written.append(destination)
                        print(f"Already have Brazil holiday data for {year}; skipping.")
                        continue
                except (json.JSONDecodeError, OSError):
                    pass
            raw_response = get_json_text(url)
            response_json = json.loads(raw_response)
            if not isinstance(response_json, list):
                raise ValueError("The API response was not a JSON list of holidays.")
            destination.write_text(raw_response, encoding="utf-8")
            written.append(destination)
            print(f"Saved Brazil holiday records for {year}: {destination}")
        except Exception as exc:
            raise RuntimeError(
                f"Holiday download failed for Brazil in {year}. "
                f"Check internet access and retry. Details: {exc}"
            ) from exc
    print(f"Finished: saved {len(written)} yearly files to {folder}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", help="Output folder, for example /Volumes/workspace/logistics/raw/holidays")
    args = parser.parse_args()
    fetch_holidays(args.output)


if __name__ == "__main__":
    main()
