"""
Pre-generate rides CSVs for each benchmark scale.

Run this once before run_benchmarks.py. Saves one rides_{n}.csv per scale
into benchmarks/data/. These files are gitignored (large generated data).

Usage (inside Docker container):
    cd /opt
    python benchmarks/generate_benchmark_data.py [--scales 90,900,4500,9000]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import create_source_data as gen

BENCHMARK_DATA_DIR = PROJECT_ROOT / "benchmarks" / "data"
DEFAULT_SCALES = [90, 900, 4500, 9000]


def generate_for_scale(fleet_size: int) -> None:
    out_path = BENCHMARK_DATA_DIR / f"rides_{fleet_size}.csv"
    if out_path.exists():
        print(f"  rides_{fleet_size}.csv already exists — skipping (delete to regenerate)")
        return

    print(f"  Generating {fleet_size} scooters...", flush=True)

    gen.SCOOTER_IDS = np.arange(1, fleet_size + 1, dtype=int)
    third = fleet_size // 3
    gen.CITY_MAP = {
        (1, third): ("Tallinn", "Estonia"),
        (third + 1, 2 * third): ("Helsinki", "Finland"),
        (2 * third + 1, fleet_size): ("Riga", "Latvia"),
    }

    rides_df = gen.generate_rides_df(
        seed=42,
        ride_start=date(2026, 1, 1),
        ride_end_exclusive=date(2027, 1, 1),
    )

    for col in ["start_time", "end_time"]:
        rides_df[col] = rides_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    BENCHMARK_DATA_DIR.mkdir(parents=True, exist_ok=True)
    rides_df.to_csv(out_path, index=False)
    print(f"  Saved {len(rides_df):,} rides → benchmarks/data/rides_{fleet_size}.csv")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pre-generate rides CSVs for benchmark scales"
    )
    parser.add_argument(
        "--scales",
        type=str,
        default=",".join(str(s) for s in DEFAULT_SCALES),
        help=f"Comma-separated fleet sizes to generate (default: {DEFAULT_SCALES})",
    )
    args = parser.parse_args()
    scales = [int(s.strip()) for s in args.scales.split(",")]

    print(f"Generating benchmark data for scales: {scales}")
    print(f"Output: benchmarks/data/\n")

    for fleet_size in scales:
        generate_for_scale(fleet_size)

    print("\nDone. Run benchmarks with:")
    print("  python benchmarks/run_benchmarks.py --scales " + ",".join(str(s) for s in scales))


if __name__ == "__main__":
    main()
