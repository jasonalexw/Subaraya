import argparse
import csv
import glob
import os
from datetime import date, datetime
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    concat,
    concat_ws,
    count,
    countDistinct,
    current_timestamp,
    date_format,
    date_trunc,
    dayofweek,
    first,
    from_unixtime,
    greatest,
    hour,
    least,
    lit,
    lpad,
    make_timestamp,
    pmod,
    round as spark_round,
    sha2,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    unix_timestamp,
    when,
    xxhash64,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TRAFFIC_COLUMNS = [
    "event_id",
    "event_hour",
    "source_event_time",
    "zone_id",
    "traffic_volume",
    "average_speed",
    "temperature_c",
    "rain_1h",
    "snow_1h",
    "clouds_all",
    "weather_main",
    "is_weekend",
    "is_rush_hour",
    "congestion_level",
    "ingested_at",
]

AIR_COLUMNS = [
    "event_id",
    "event_hour",
    "source_event_time",
    "zone_id",
    "pm25",
    "pm10",
    "no2",
    "co",
    "o3",
    "temperature_c",
    "rainfall",
    "wind_speed",
    "station_count",
    "ingested_at",
]

ACCIDENT_COLUMNS = [
    "event_id",
    "event_hour",
    "source_event_time",
    "zone_id",
    "accident_count",
    "fatal_count",
    "serious_count",
    "slight_count",
    "casualty_count",
    "vehicle_count",
    "ingested_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Metropolia batch ETL")
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily", "stream_seed"],
        required=True,
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--data-root", default=os.getenv("DATA_ROOT", "/opt/airflow/data"))
    return parser.parse_args()


def read_calendar(data_root: Path) -> dict[str, dict[str, str]]:
    path = data_root / "reference" / "simulation_calendar.csv"
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return {row["source_type"]: row for row in csv.DictReader(stream)}


def day_shift(rule: dict[str, str]) -> int:
    source_start = date.fromisoformat(rule["source_start_date"])
    simulation_start = date.fromisoformat(rule["simulation_start_date"])
    return (simulation_start - source_start).days


def shift_timestamp(column_name: str, offset_days: int):
    offset_seconds = offset_days * 86400
    return from_unixtime(
        unix_timestamp(col(column_name)) + lit(offset_seconds)
    ).cast("timestamp")


def in_process_period(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    return df.filter(to_date(col("event_hour")).between(start_date, end_date))


def write_quarantine(
    invalid: DataFrame,
    data_root: Path,
    source_name: str,
    run_id: str,
) -> int:
    invalid_count = invalid.count()
    if invalid_count:
        output = str(data_root / "quarantine" / source_name / f"run={run_id}")
        invalid.coalesce(4).write.mode("overwrite").json(output)
    return invalid_count


def write_canonical(
    df: DataFrame,
    data_root: Path,
    source_name: str,
    run_id: str,
) -> None:
    output = str(data_root / "canonical" / source_name / f"run={run_id}")
    # Keep the number of simultaneous Parquet writers small.  The ETL runs in
    # local mode inside a memory-constrained Airflow container, so allowing one
    # writer per upstream partition can exhaust the JVM heap.
    df.coalesce(4).write.mode("overwrite").parquet(output)


def write_stream_seed(
    df: DataFrame,
    data_root: Path,
    topic_name: str,
    run_id: str,
) -> None:
    output = str(data_root / "stream" / "seed" / topic_name / f"run={run_id}")
    (
        df.orderBy("event_hour", "zone_id")
        .coalesce(4)
        .write.mode("overwrite")
        .json(output)
    )


def load_references(spark: SparkSession, data_root: Path) -> tuple[DataFrame, DataFrame]:
    zone_schema = StructType(
        [
            StructField("zone_id", StringType(), False),
            StructField("zone_name", StringType(), False),
            StructField("sim_latitude", DoubleType(), False),
            StructField("sim_longitude", DoubleType(), False),
            StructField("road_capacity", IntegerType(), False),
            StructField("speed_limit_kmh", IntegerType(), False),
        ]
    )
    mapping_schema = StructType(
        [
            StructField("source_type", StringType(), False),
            StructField("source_location", StringType(), False),
            StructField("zone_id", StringType(), False),
            StructField("volume_factor", DoubleType(), True),
        ]
    )
    zones = (
        spark.read.option("header", True)
        .schema(zone_schema)
        .csv(str(data_root / "reference" / "zone_reference.csv"))
    )
    mapping = (
        spark.read.option("header", True)
        .schema(mapping_schema)
        .csv(str(data_root / "reference" / "source_zone_mapping.csv"))
    )
    return zones, mapping


def transform_traffic(
    spark: SparkSession,
    data_root: Path,
    zones: DataFrame,
    mapping: DataFrame,
    calendar: dict[str, dict[str, str]],
    start_date: str,
    end_date: str,
    run_id: str,
) -> tuple[DataFrame, int]:
    schema = StructType(
        [
            StructField("holiday", StringType(), True),
            StructField("temp", DoubleType(), True),
            StructField("rain_1h", DoubleType(), True),
            StructField("snow_1h", DoubleType(), True),
            StructField("clouds_all", DoubleType(), True),
            StructField("weather_main", StringType(), True),
            StructField("weather_description", StringType(), True),
            StructField("date_time", StringType(), True),
            StructField("traffic_volume", DoubleType(), True),
        ]
    )
    raw = (
        spark.read.option("header", True)
        .schema(schema)
        .csv(str(data_root / "raw" / "traffic" / "Metro_Interstate_Traffic_Volume.csv"))
    )
    traffic_mapping = (
        mapping.filter(col("source_type") == "traffic")
        .select("source_location", "zone_id", "volume_factor")
    )
    prepared = (
        raw.withColumn("source_location", lit("ATR-301"))
        .join(traffic_mapping, "source_location", "left")
        .join(zones, "zone_id", "left")
        .withColumn("source_event_time", to_timestamp("date_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn(
            "event_time",
            shift_timestamp("source_event_time", day_shift(calendar["traffic"])),
        )
        .withColumn("event_hour", date_trunc("hour", col("event_time")))
        .withColumn(
            "traffic_volume_sim",
            spark_round(col("traffic_volume") * col("volume_factor"), 0),
        )
        .withColumn("temperature_c", col("temp") - lit(273.15))
    )
    prepared = in_process_period(prepared, start_date, end_date)
    valid_condition = (
        col("source_event_time").isNotNull()
        & col("event_hour").isNotNull()
        & col("zone_id").isNotNull()
        & col("traffic_volume_sim").isNotNull()
        & (col("traffic_volume_sim") >= 0)
    )
    invalid = prepared.filter(~valid_condition)
    valid = prepared.filter(valid_condition)
    quarantine_count = write_quarantine(invalid, data_root, "traffic", run_id)

    hourly = (
        valid.groupBy("event_hour", "zone_id")
        .agg(
            first("source_event_time", ignorenulls=True).alias("source_event_time"),
            spark_round(avg("traffic_volume_sim"), 0).cast("int").alias("traffic_volume"),
            spark_round(avg("temperature_c"), 2).alias("temperature_c"),
            spark_round(avg("rain_1h"), 2).alias("rain_1h"),
            spark_round(avg("snow_1h"), 2).alias("snow_1h"),
            spark_round(avg("clouds_all"), 2).alias("clouds_all"),
            first("weather_main", ignorenulls=True).alias("weather_main"),
            first("road_capacity").alias("road_capacity"),
            first("speed_limit_kmh").alias("speed_limit_kmh"),
        )
        .withColumn(
            "average_speed",
            spark_round(
                greatest(
                    lit(5.0),
                    col("speed_limit_kmh")
                    * (
                        lit(1.0)
                        - least(
                            col("traffic_volume") / col("road_capacity"), lit(1.0)
                        )
                        * lit(0.75)
                    ),
                ),
                2,
            ),
        )
        .withColumn(
            "congestion_level",
            when(col("traffic_volume") / col("road_capacity") >= 0.8, "HIGH")
            .when(col("traffic_volume") / col("road_capacity") >= 0.5, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("is_weekend", when(dayofweek("event_hour").isin(1, 7), 1).otherwise(0))
        .withColumn(
            "is_rush_hour",
            when(hour("event_hour").isin(7, 8, 9, 16, 17, 18), 1).otherwise(0),
        )
        .withColumn(
            "event_id",
            sha2(concat_ws("|", lit("traffic"), col("zone_id"), col("event_hour")), 256),
        )
        .withColumn("ingested_at", current_timestamp())
        .select(*TRAFFIC_COLUMNS)
    )
    return hourly, quarantine_count


def transform_air_quality(
    spark: SparkSession,
    data_root: Path,
    mapping: DataFrame,
    calendar: dict[str, dict[str, str]],
    start_date: str,
    end_date: str,
    run_id: str,
) -> tuple[DataFrame, int]:
    schema = StructType(
        [
            StructField("No", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("hour", IntegerType(), True),
            StructField("PM2.5", DoubleType(), True),
            StructField("PM10", DoubleType(), True),
            StructField("SO2", DoubleType(), True),
            StructField("NO2", DoubleType(), True),
            StructField("CO", DoubleType(), True),
            StructField("O3", DoubleType(), True),
            StructField("TEMP", DoubleType(), True),
            StructField("PRES", DoubleType(), True),
            StructField("DEWP", DoubleType(), True),
            StructField("RAIN", DoubleType(), True),
            StructField("wd", StringType(), True),
            StructField("WSPM", DoubleType(), True),
            StructField("station", StringType(), True),
        ]
    )
    raw_path = str(
        data_root
        / "raw"
        / "air_quality"
        / "stations"
        / "PRSA_Data_20130301-20170228"
        / "*.csv"
    )
    raw = spark.read.option("header", True).schema(schema).csv(raw_path)
    air_mapping = (
        mapping.filter(col("source_type") == "air_quality")
        .select(col("source_location").alias("station"), "zone_id")
    )
    prepared = (
        raw.join(air_mapping, "station", "left")
        .withColumn(
            "source_event_time",
            make_timestamp("year", "month", "day", "hour", lit(0), lit(0)),
        )
        .withColumn(
            "event_time",
            shift_timestamp("source_event_time", day_shift(calendar["air_quality"])),
        )
        .withColumn("event_hour", date_trunc("hour", col("event_time")))
        .withColumn("pm25", col("`PM2.5`"))
        .withColumn("pm10", col("PM10"))
        .withColumn("no2", col("NO2"))
        .withColumn("co", col("CO"))
        .withColumn("o3", col("O3"))
        .withColumn("temperature_c", col("TEMP"))
        .withColumn("rainfall", col("RAIN"))
        .withColumn("wind_speed", col("WSPM"))
    )
    prepared = in_process_period(prepared, start_date, end_date)
    valid_condition = (
        col("source_event_time").isNotNull()
        & col("event_hour").isNotNull()
        & col("zone_id").isNotNull()
        & (col("pm25").isNull() | (col("pm25") >= 0))
    )
    invalid = prepared.filter(~valid_condition)
    valid = prepared.filter(valid_condition)
    quarantine_count = write_quarantine(invalid, data_root, "air_quality", run_id)

    hourly = (
        valid.groupBy("event_hour", "zone_id")
        .agg(
            first("source_event_time", ignorenulls=True).alias("source_event_time"),
            spark_round(avg("pm25"), 2).alias("pm25"),
            spark_round(avg("pm10"), 2).alias("pm10"),
            spark_round(avg("no2"), 2).alias("no2"),
            spark_round(avg("co"), 2).alias("co"),
            spark_round(avg("o3"), 2).alias("o3"),
            spark_round(avg("temperature_c"), 2).alias("temperature_c"),
            spark_round(avg("rainfall"), 2).alias("rainfall"),
            spark_round(avg("wind_speed"), 2).alias("wind_speed"),
            countDistinct("station").cast("int").alias("station_count"),
        )
        .withColumn(
            "event_id",
            sha2(concat_ws("|", lit("air_quality"), col("zone_id"), col("event_hour")), 256),
        )
        .withColumn("ingested_at", current_timestamp())
        .select(*AIR_COLUMNS)
    )
    return hourly, quarantine_count


def read_accident_files(spark: SparkSession, data_root: Path) -> DataFrame:
    paths = sorted(glob.glob(str(data_root / "raw" / "accident" / "collisions_*.csv")))
    selected = [
        "collision_index",
        "collision_year",
        "longitude",
        "latitude",
        "collision_severity",
        "number_of_vehicles",
        "number_of_casualties",
        "date",
        "time",
    ]
    frames = []
    for path in paths:
        # Keep date and time as their original strings. Schema inference can
        # interpret them inconsistently across yearly collision files and make
        # the formatted timestamp expression return NULL.
        frame = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(path)
        )
        frames.append(frame.select(*selected))
    if not frames:
        raise FileNotFoundError("No collision CSV files found")
    return reduce(lambda left, right: left.unionByName(right), frames)


def transform_accidents(
    spark: SparkSession,
    data_root: Path,
    zones: DataFrame,
    calendar: dict[str, dict[str, str]],
    start_date: str,
    end_date: str,
    run_id: str,
) -> tuple[DataFrame, int]:
    raw = read_accident_files(spark, data_root)
    prepared = (
        raw.withColumn("collision_index", col("collision_index").cast("string"))
        .withColumn(
            "source_event_time",
            to_timestamp(
                concat(trim(col("date")), lit(" "), trim(col("time"))),
                "dd/MM/yyyy HH:mm",
            ),
        )
        .withColumn(
            "event_time",
            shift_timestamp("source_event_time", day_shift(calendar["accident"])),
        )
        .withColumn("event_hour", date_trunc("hour", col("event_time")))
        .withColumn(
            "zone_number",
            (pmod(spark_abs(xxhash64("collision_index")), lit(4)) + lit(1)).cast("string"),
        )
        .withColumn("zone_id", concat(lit("Z"), lpad("zone_number", 2, "0")))
        .join(zones.select("zone_id"), "zone_id", "left")
        .withColumn("collision_severity", col("collision_severity").cast("int"))
        .withColumn("number_of_vehicles", col("number_of_vehicles").cast("int"))
        .withColumn("number_of_casualties", col("number_of_casualties").cast("int"))
    )
    prepared = in_process_period(prepared, start_date, end_date)
    valid_condition = (
        col("collision_index").isNotNull()
        & col("source_event_time").isNotNull()
        & col("event_hour").isNotNull()
        & col("zone_id").isNotNull()
    )
    invalid = prepared.filter(~valid_condition)
    valid = prepared.filter(valid_condition)
    quarantine_count = write_quarantine(invalid, data_root, "accident", run_id)

    hourly = (
        valid.groupBy("event_hour", "zone_id")
        .agg(
            first("source_event_time", ignorenulls=True).alias("source_event_time"),
            count("collision_index").cast("int").alias("accident_count"),
            spark_sum(when(col("collision_severity") == 1, 1).otherwise(0))
            .cast("int")
            .alias("fatal_count"),
            spark_sum(when(col("collision_severity") == 2, 1).otherwise(0))
            .cast("int")
            .alias("serious_count"),
            spark_sum(when(col("collision_severity") == 3, 1).otherwise(0))
            .cast("int")
            .alias("slight_count"),
            spark_sum("number_of_casualties").cast("int").alias("casualty_count"),
            spark_sum("number_of_vehicles").cast("int").alias("vehicle_count"),
        )
        .withColumn(
            "event_id",
            sha2(concat_ws("|", lit("accident"), col("zone_id"), col("event_hour")), 256),
        )
        .withColumn("ingested_at", current_timestamp())
        .select(*ACCIDENT_COLUMNS)
    )
    return hourly, quarantine_count


def write_staging(
    df: DataFrame,
    table: str,
    jdbc_url: str,
    properties: dict[str, str],
) -> None:
    df.write.jdbc(
        url=jdbc_url,
        table=table,
        mode="overwrite",
        properties=properties,
    )


def main() -> None:
    args = parse_args()
    if date.fromisoformat(args.end_date) < date.fromisoformat(args.start_date):
        raise ValueError("end-date must be on or after start-date")

    data_root = Path(args.data_root)
    run_id = f"{args.mode}_{args.start_date}_{args.end_date}"
    batch_id = f"metropolia-{run_id}"

    spark = (
        SparkSession.builder.appName("MetropoliaBatchETL")
        # The Airflow/PySpark image does not include the PostgreSQL JDBC driver
        # on Spark's classpath.  This is the same setup used by the reference
        # etl-metropolia project that successfully ran previously.
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.hadoop.parquet.block.size", "33554432")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        calendar = read_calendar(data_root)
        zones, mapping = load_references(spark, data_root)

        traffic, traffic_quarantine = transform_traffic(
            spark,
            data_root,
            zones,
            mapping,
            calendar,
            args.start_date,
            args.end_date,
            run_id,
        )
        air_quality, air_quarantine = transform_air_quality(
            spark,
            data_root,
            mapping,
            calendar,
            args.start_date,
            args.end_date,
            run_id,
        )
        accidents, accident_quarantine = transform_accidents(
            spark,
            data_root,
            zones,
            calendar,
            args.start_date,
            args.end_date,
            run_id,
        )

        traffic_rows = traffic.count()
        air_rows = air_quality.count()
        accident_rows = accidents.count()
        quarantine_rows = traffic_quarantine + air_quarantine + accident_quarantine

        write_canonical(traffic, data_root, "traffic_events", run_id)
        write_canonical(air_quality, data_root, "air_quality_events", run_id)
        write_canonical(accidents, data_root, "accident_events", run_id)

        if args.mode == "stream_seed":
            write_stream_seed(traffic, data_root, "metropolia.traffic", run_id)
            write_stream_seed(
                air_quality,
                data_root,
                "metropolia.air-quality",
                run_id,
            )
            write_stream_seed(accidents, data_root, "metropolia.accident", run_id)
            print(
                {
                    "batch_id": batch_id,
                    "mode": args.mode,
                    "traffic_rows": traffic_rows,
                    "air_quality_rows": air_rows,
                    "accident_rows": accident_rows,
                    "quarantine_rows": quarantine_rows,
                    "loaded_to_warehouse": False,
                }
            )
            return

        jdbc_url = os.getenv(
            "JDBC_URL",
            "jdbc:postgresql://target-postgres:5432/metropolia_warehouse",
        )
        properties = {
            "user": os.getenv("JDBC_USER", "de_user"),
            "password": os.getenv("JDBC_PASSWORD", "password123"),
            "driver": "org.postgresql.Driver",
        }

        write_staging(traffic, "staging_traffic_hourly", jdbc_url, properties)
        write_staging(air_quality, "staging_air_quality_hourly", jdbc_url, properties)
        write_staging(accidents, "staging_accident_hourly", jdbc_url, properties)

        log_schema = StructType(
            [
                StructField("batch_id", StringType(), False),
                StructField("mode", StringType(), False),
                StructField("process_start_date", StringType(), False),
                StructField("process_end_date", StringType(), False),
                StructField("traffic_rows", IntegerType(), False),
                StructField("air_quality_rows", IntegerType(), False),
                StructField("accident_rows", IntegerType(), False),
                StructField("quarantine_rows", IntegerType(), False),
                StructField("status", StringType(), False),
                StructField("processed_at", TimestampType(), False),
            ]
        )
        batch_log = spark.createDataFrame(
            [
                (
                    batch_id,
                    args.mode,
                    args.start_date,
                    args.end_date,
                    traffic_rows,
                    air_rows,
                    accident_rows,
                    quarantine_rows,
                    "SUCCESS",
                    datetime.utcnow(),
                )
            ],
            schema=log_schema,
        ).select(
            "batch_id",
            "mode",
            to_date("process_start_date").alias("process_start_date"),
            to_date("process_end_date").alias("process_end_date"),
            "traffic_rows",
            "air_quality_rows",
            "accident_rows",
            "quarantine_rows",
            "status",
            "processed_at",
        )
        write_staging(batch_log, "staging_etl_batch_log", jdbc_url, properties)

        print(
            {
                "batch_id": batch_id,
                "traffic_rows": traffic_rows,
                "air_quality_rows": air_rows,
                "accident_rows": accident_rows,
                "quarantine_rows": quarantine_rows,
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
