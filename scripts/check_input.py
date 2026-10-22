import argparse
import csv
import glob
import json
from pathlib import Path


REQUIRED_HEADERS = {
    "traffic": {
        "holiday",
        "temp",
        "rain_1h",
        "snow_1h",
        "clouds_all",
        "weather_main",
        "date_time",
        "traffic_volume",
    },
    "air_quality": {
        "year",
        "month",
        "day",
        "hour",
        "PM2.5",
        "PM10",
        "NO2",
        "CO",
        "O3",
        "TEMP",
        "RAIN",
        "WSPM",
        "station",
    },
    "accident": {
        "collision_index",
        "collision_year",
        "longitude",
        "latitude",
        "collision_severity",
        "number_of_vehicles",
        "number_of_casualties",
        "date",
        "time",
    },
}


def read_header(path: Path) -> set[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return set(next(csv.reader(stream)))


def require_headers(kind: str, path: Path, errors: list[str]) -> None:
    missing = REQUIRED_HEADERS[kind] - read_header(path)
    if missing:
        errors.append(f"{path}: missing columns {sorted(missing)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Metropolia raw inputs")
    parser.add_argument("--data-root", default="/opt/airflow/data")
    args = parser.parse_args()

    root = Path(args.data_root)
    traffic = root / "raw" / "traffic" / "Metro_Interstate_Traffic_Volume.csv"
    air_pattern = str(
        root
        / "raw"
        / "air_quality"
        / "stations"
        / "PRSA_Data_20130301-20170228"
        / "*.csv"
    )
    air_files = [Path(path) for path in sorted(glob.glob(air_pattern))]
    accident_files = sorted((root / "raw" / "accident").glob("collisions_*.csv"))
    references = [
        root / "reference" / "zone_reference.csv",
        root / "reference" / "source_zone_mapping.csv",
        root / "reference" / "simulation_calendar.csv",
        root / "reference" / "pipeline_periods.csv",
    ]

    errors: list[str] = []
    if not traffic.exists():
        errors.append(f"Missing traffic file: {traffic}")
    else:
        require_headers("traffic", traffic, errors)

    if len(air_files) != 12:
        errors.append(f"Expected 12 air-quality files, found {len(air_files)}")
    for path in air_files:
        require_headers("air_quality", path, errors)

    accident_years = set()
    for path in accident_files:
        require_headers("accident", path, errors)
        accident_years.add(path.stem.rsplit("_", 1)[-1])
    expected_years = {"2022", "2023", "2024", "2025"}
    if accident_years != expected_years:
        errors.append(
            f"Expected accident years {sorted(expected_years)}, "
            f"found {sorted(accident_years)}"
        )

    for path in references:
        if not path.exists():
            errors.append(f"Missing reference file: {path}")

    summary = {
        "data_root": str(root),
        "traffic_files": int(traffic.exists()),
        "air_quality_files": len(air_files),
        "accident_files": len(accident_files),
        "status": "FAILED" if errors else "OK",
        "errors": errors,
    }
    print(json.dumps(summary, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
