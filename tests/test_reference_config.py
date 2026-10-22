import csv
import unittest
from datetime import date, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "data" / "reference"


def read_csv(name: str):
    with (REFERENCE / name).open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


class ReferenceConfigTest(unittest.TestCase):
    def test_simulation_periods_have_equal_duration(self):
        rows = read_csv("simulation_calendar.csv")
        self.assertEqual({row["source_type"] for row in rows}, {"traffic", "air_quality", "accident"})
        for row in rows:
            source_days = (
                date.fromisoformat(row["source_end_date"])
                - date.fromisoformat(row["source_start_date"])
            ).days
            simulation_days = (
                date.fromisoformat(row["simulation_end_date"])
                - date.fromisoformat(row["simulation_start_date"])
            ).days
            self.assertEqual(source_days, simulation_days, row["source_type"])

    def test_pipeline_phases_are_contiguous(self):
        phases = read_csv("pipeline_periods.csv")
        backfill, daily, streaming = phases
        self.assertEqual(
            date.fromisoformat(backfill["end_date"]) + timedelta(days=1),
            date.fromisoformat(daily["start_date"]),
        )
        self.assertEqual(
            date.fromisoformat(daily["end_date"]) + timedelta(days=1),
            date.fromisoformat(streaming["start_date"]),
        )

    def test_zone_and_station_mapping(self):
        zones = read_csv("zone_reference.csv")
        mappings = read_csv("source_zone_mapping.csv")
        zone_ids = {row["zone_id"] for row in zones}
        self.assertEqual(zone_ids, {"Z01", "Z02", "Z03", "Z04"})
        mapped_stations = {
            row["source_location"]
            for row in mappings
            if row["source_type"] == "air_quality"
        }
        self.assertEqual(len(mapped_stations), 12)
        self.assertTrue(all(row["zone_id"] in zone_ids for row in mappings))


if __name__ == "__main__":
    unittest.main()
