"""Download one Open-Meteo daily weather point for each Brazilian state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import get_json_text, output_directory

# Approximate state-capital coordinates make state-level weather comparison
# understandable. They are not a complete model of weather across a large state.
BRAZIL_STATE_POINTS = {
    "AC": (-9.9754, -67.8249), "AL": (-9.6658, -35.7350),
    "AP": (0.0349, -51.0694), "AM": (-3.1190, -60.0217),
    "BA": (-12.9777, -38.5016), "CE": (-3.7319, -38.5267),
    "DF": (-15.7939, -47.8828), "ES": (-20.3155, -40.3128),
    "GO": (-16.6869, -49.2648), "MA": (-2.5307, -44.3068),
    "MT": (-15.6014, -56.0979), "MS": (-20.4697, -54.6201),
    "MG": (-19.9167, -43.9345), "PA": (-1.4558, -48.4902),
    "PB": (-7.1195, -34.8450), "PR": (-25.4284, -49.2733),
    "PE": (-8.0476, -34.8770), "PI": (-5.0892, -42.8016),
    "RJ": (-22.9068, -43.1729), "RN": (-5.7945, -35.2110),
    "RS": (-30.0346, -51.2177), "RO": (-8.7612, -63.9004),
    "RR": (2.8235, -60.6758), "SC": (-27.5954, -48.5480),
    "SP": (-23.5505, -46.6333), "SE": (-10.9472, -37.0731),
    "TO": (-10.1840, -48.3336),
}


def fetch_weather(output: str | None = None, start_date: str = "2016-01-01",
                 end_date: str = "2018-12-31") -> list[Path]:
    """Save each API response unchanged as one JSON file, named by state code."""
    folder = output_directory("weather", output)
    written: list[Path] = []
    for state_code, (latitude, longitude) in BRAZIL_STATE_POINTS.items():
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={latitude}&longitude={longitude}"
            f"&start_date={start_date}&end_date={end_date}"
            "&daily=precipitation_sum,temperature_2m_max,wind_speed_10m_max"
            "&timezone=America%2FSao_Paulo"
        )
        try:
            destination = folder / f"weather_{state_code}.json"
            if destination.exists():
                try:
                    prior = json.loads(destination.read_text(encoding="utf-8"))
                    dates = prior.get("daily", {}).get("time", [])
                    if dates and dates[0] == start_date and dates[-1] == end_date:
                        written.append(destination)
                        print(f"Already have complete daily weather for {state_code}; skipping.")
                        continue
                except (json.JSONDecodeError, OSError, AttributeError):
                    pass
            raw_response = get_json_text(url)
            # Keep the exact response text; filename gives Bronze the state.
            destination.write_text(raw_response, encoding="utf-8")
            written.append(destination)
            print(f"Saved daily weather for {state_code}: {destination}")
        except Exception as exc:
            raise RuntimeError(
                f"Weather download failed for state {state_code}. "
                f"Check your internet and retry. Details: {exc}"
            ) from exc
    print(f"Finished: saved {len(written)} state files to {folder}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", help="Output folder, for example /Volumes/workspace/logistics/raw/weather")
    parser.add_argument("--start-date", default="2016-01-01")
    parser.add_argument("--end-date", default="2018-12-31")
    args = parser.parse_args()
    fetch_weather(args.output, args.start_date, args.end_date)


if __name__ == "__main__":
    main()
