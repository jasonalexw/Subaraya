#!/usr/bin/env python3
"""Kafka -> Spark Structured Streaming -> Metropolia PostgreSQL warehouse."""

import json
import os
from collections import defaultdict

import psycopg2
from psycopg2.extras import Json, execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "metropolia.events")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "target-postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "metropolia_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/metropolia/checkpoints")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "2 seconds")


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("zone_id", StringType(), True),
        StructField("emitted_at", StringType(), True),
        StructField("traffic_volume", IntegerType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("rainfall", DoubleType(), True),
        StructField("weather_main", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("co", DoubleType(), True),
        StructField("o3", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("severity", StringType(), True),
        StructField("casualty_count", IntegerType(), True),
        StructField("vehicle_count", IntegerType(), True),
    ]
)


ZONE_CAPACITY = {
    "Z01": (6000, 50),
    "Z02": (5000, 60),
    "Z03": (4000, 40),
    "Z04": (3000, 30),
}


def database_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def last_by_key(rows: list[dict]) -> list[dict]:
    latest = {}
    for row in rows:
        latest[(row["event_hour"], row["zone_id"])] = row
    return list(latest.values())


def aggregate_accidents(rows: list[dict]) -> list[dict]:
    grouped = {}
    for row in rows:
        key = (row["event_hour"], row["zone_id"])
        if key not in grouped:
            grouped[key] = {
                "event_id": row["event_id"],
                "event_hour": row["event_hour"],
                "source_event_time": row["source_event_time"],
                "zone_id": row["zone_id"],
                "accident_count": 0,
                "fatal_count": 0,
                "serious_count": 0,
                "slight_count": 0,
                "casualty_count": 0,
                "vehicle_count": 0,
                "ingested_at": row["ingested_at"],
            }
        item = grouped[key]
        item["event_id"] = row["event_id"]
        item["accident_count"] += 1
        item["fatal_count"] += int(row["severity"] == "FATAL")
        item["serious_count"] += int(row["severity"] == "SERIOUS")
        item["slight_count"] += int(row["severity"] == "SLIGHT")
        item["casualty_count"] += row["casualty_count"] or 0
        item["vehicle_count"] += row["vehicle_count"] or 0
    return list(grouped.values())


def insert_new_event_log(cursor, rows: list[dict]) -> list[dict]:
    accepted = []
    sql = """
        INSERT INTO etl_stream_event_log (
            event_id, event_type, topic, kafka_partition, kafka_offset,
            event_hour, zone_id, emitted_at, processed_at, payload
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING event_id
    """
    for row in rows:
        cursor.execute(
            sql,
            (
                row["event_id"],
                row["event_type"],
                row["topic"],
                row["kafka_partition"],
                row["kafka_offset"],
                row["event_hour"],
                row["zone_id"],
                row["emitted_at"],
                row["ingested_at"],
                Json(json.loads(row["raw_json"])),
            ),
        )
        if cursor.fetchone():
            accepted.append(row)
    return accepted


def upsert_traffic(cursor, rows: list[dict]) -> None:
    rows = last_by_key(rows)
    if not rows:
        return
    values = []
    for row in rows:
        capacity, speed_limit = ZONE_CAPACITY[row["zone_id"]]
        ratio = min(row["traffic_volume"] / capacity, 1.0)
        average_speed = round(max(5.0, speed_limit * (1.0 - ratio * 0.75)), 2)
        congestion = "HIGH" if ratio >= 0.8 else "MEDIUM" if ratio >= 0.5 else "LOW"
        is_weekend = int(row["event_hour"].isoweekday() in (6, 7))
        is_rush_hour = int(row["event_hour"].hour in (7, 8, 9, 16, 17, 18))
        values.append(
            (
                row["event_id"], row["event_hour"], row["source_event_time"],
                row["zone_id"], row["traffic_volume"], average_speed,
                row["temperature_c"], row["rainfall"], 0.0, 0.0,
                row["weather_main"] or "Unknown", is_weekend, is_rush_hour,
                congestion, row["ingested_at"],
            )
        )
    execute_values(
        cursor,
        """
        INSERT INTO fact_traffic_hourly (
            event_id, event_hour, source_event_time, zone_id, traffic_volume,
            average_speed, temperature_c, rain_1h, snow_1h, clouds_all,
            weather_main, is_weekend, is_rush_hour, congestion_level, ingested_at
        ) VALUES %s
        ON CONFLICT (event_hour, zone_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            source_event_time = EXCLUDED.source_event_time,
            traffic_volume = EXCLUDED.traffic_volume,
            average_speed = EXCLUDED.average_speed,
            temperature_c = EXCLUDED.temperature_c,
            rain_1h = EXCLUDED.rain_1h,
            weather_main = EXCLUDED.weather_main,
            is_weekend = EXCLUDED.is_weekend,
            is_rush_hour = EXCLUDED.is_rush_hour,
            congestion_level = EXCLUDED.congestion_level,
            ingested_at = EXCLUDED.ingested_at
        """,
        values,
    )


def upsert_air_quality(cursor, rows: list[dict]) -> None:
    rows = last_by_key(rows)
    if not rows:
        return
    values = [
        (
            row["event_id"], row["event_hour"], row["source_event_time"],
            row["zone_id"], row["pm25"], row["pm10"], row["no2"],
            row["co"], row["o3"], row["temperature_c"], row["rainfall"],
            row["wind_speed"], 1, row["ingested_at"],
        )
        for row in rows
    ]
    execute_values(
        cursor,
        """
        INSERT INTO fact_air_quality_hourly (
            event_id, event_hour, source_event_time, zone_id, pm25, pm10, no2,
            co, o3, temperature_c, rainfall, wind_speed, station_count, ingested_at
        ) VALUES %s
        ON CONFLICT (event_hour, zone_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            source_event_time = EXCLUDED.source_event_time,
            pm25 = EXCLUDED.pm25,
            pm10 = EXCLUDED.pm10,
            no2 = EXCLUDED.no2,
            co = EXCLUDED.co,
            o3 = EXCLUDED.o3,
            temperature_c = EXCLUDED.temperature_c,
            rainfall = EXCLUDED.rainfall,
            wind_speed = EXCLUDED.wind_speed,
            station_count = EXCLUDED.station_count,
            ingested_at = EXCLUDED.ingested_at
        """,
        values,
    )


def upsert_accidents(cursor, rows: list[dict]) -> None:
    rows = aggregate_accidents(rows)
    if not rows:
        return
    values = [
        (
            row["event_id"], row["event_hour"], row["source_event_time"],
            row["zone_id"], row["accident_count"], row["fatal_count"],
            row["serious_count"], row["slight_count"], row["casualty_count"],
            row["vehicle_count"], row["ingested_at"],
        )
        for row in rows
    ]
    execute_values(
        cursor,
        """
        INSERT INTO fact_accident_hourly (
            event_id, event_hour, source_event_time, zone_id, accident_count,
            fatal_count, serious_count, slight_count, casualty_count,
            vehicle_count, ingested_at
        ) VALUES %s
        ON CONFLICT (event_hour, zone_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            accident_count = fact_accident_hourly.accident_count + EXCLUDED.accident_count,
            fatal_count = fact_accident_hourly.fatal_count + EXCLUDED.fatal_count,
            serious_count = fact_accident_hourly.serious_count + EXCLUDED.serious_count,
            slight_count = fact_accident_hourly.slight_count + EXCLUDED.slight_count,
            casualty_count = fact_accident_hourly.casualty_count + EXCLUDED.casualty_count,
            vehicle_count = fact_accident_hourly.vehicle_count + EXCLUDED.vehicle_count,
            ingested_at = EXCLUDED.ingested_at
        """,
        values,
    )


def refresh_mart(cursor, keys: set[tuple]) -> None:
    if not keys:
        return
    execute_values(
        cursor,
        """
        INSERT INTO mart_metropolia_hourly (
            event_hour, zone_id, traffic_volume, average_speed, congestion_level,
            pm25, pm10, temperature_c, rainfall, wind_speed, accident_count,
            casualty_count, refreshed_at
        )
        SELECT
            k.event_hour, k.zone_id, t.traffic_volume, t.average_speed,
            t.congestion_level, a.pm25, a.pm10,
            COALESCE(a.temperature_c, t.temperature_c),
            COALESCE(a.rainfall, t.rain_1h), a.wind_speed,
            COALESCE(c.accident_count, 0), COALESCE(c.casualty_count, 0),
            CURRENT_TIMESTAMP
        FROM (VALUES %s) AS k(event_hour, zone_id)
        LEFT JOIN fact_traffic_hourly t USING (event_hour, zone_id)
        LEFT JOIN fact_air_quality_hourly a USING (event_hour, zone_id)
        LEFT JOIN fact_accident_hourly c USING (event_hour, zone_id)
        ON CONFLICT (event_hour, zone_id) DO UPDATE SET
            traffic_volume = EXCLUDED.traffic_volume,
            average_speed = EXCLUDED.average_speed,
            congestion_level = EXCLUDED.congestion_level,
            pm25 = EXCLUDED.pm25,
            pm10 = EXCLUDED.pm10,
            temperature_c = EXCLUDED.temperature_c,
            rainfall = EXCLUDED.rainfall,
            wind_speed = EXCLUDED.wind_speed,
            accident_count = EXCLUDED.accident_count,
            casualty_count = EXCLUDED.casualty_count,
            refreshed_at = EXCLUDED.refreshed_at
        """,
        sorted(keys),
    )


def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
    records = [row.asDict(recursive=True) for row in batch_df.toLocalIterator()]
    if not records:
        return

    connection = database_connection()
    try:
        with connection.cursor() as cursor:
            accepted = insert_new_event_log(cursor, records)
            by_type = defaultdict(list)
            for row in accepted:
                by_type[row["event_type"]].append(row)

            upsert_traffic(cursor, by_type["traffic"])
            upsert_air_quality(cursor, by_type["air_quality"])
            upsert_accidents(cursor, by_type["accident"])
            changed_keys = {(row["event_hour"], row["zone_id"]) for row in accepted}
            refresh_mart(cursor, changed_keys)

            cursor.execute(
                """
                INSERT INTO etl_stream_batch_log (
                    batch_id, received_rows, accepted_rows, traffic_rows,
                    air_quality_rows, accident_rows, status, processed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, 'SUCCESS', CURRENT_TIMESTAMP)
                ON CONFLICT (batch_id) DO UPDATE SET
                    received_rows = EXCLUDED.received_rows,
                    accepted_rows = EXCLUDED.accepted_rows,
                    traffic_rows = EXCLUDED.traffic_rows,
                    air_quality_rows = EXCLUDED.air_quality_rows,
                    accident_rows = EXCLUDED.accident_rows,
                    status = EXCLUDED.status,
                    processed_at = EXCLUDEDd.processed_at
                """,
                (
                    batch_id,
                    len(records),
                    len(accepted),
                    len(by_type["traffic"]),
                    len(by_type["air_quality"]),
                    len(by_type["accident"]),
                ),
            )
        connection.commit()
        print(
            f"Micro-batch {batch_id}: received={len(records)}, "
            f"accepted={len(accepted)}, mart_keys={len(changed_keys)}"
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def main() -> None:
    packages = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "org.postgresql:postgresql:42.6.0"
    )
    spark = (
        SparkSession.builder.appName("MetropoliaStructuredStreaming")
        .master("local[2]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "768m"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "768m"))
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_stream.select(
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("payload", F.from_json("raw_json", EVENT_SCHEMA))
        .select("topic", "kafka_partition", "kafka_offset", "raw_json", "payload.*")
        .withColumn("event_hour", F.date_trunc("hour", F.to_timestamp("event_time")))
        .withColumn("source_event_time", F.to_timestamp("event_time"))
        .withColumn("emitted_at", F.to_timestamp("emitted_at"))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(
            F.col("event_id").isNotNull()
            & F.col("event_type").isin("traffic", "air_quality", "accident")
            & F.col("event_hour").isNotNull()
            & F.col("zone_id").isin(*ZONE_CAPACITY.keys())
        )
        .filter(
            ((F.col("event_type") == "traffic") & (F.col("traffic_volume") >= 0))
            | ((F.col("event_type") == "air_quality") & (F.col("pm25") >= 0))
            | (
                (F.col("event_type") == "accident")
                & F.col("severity").isin("FATAL", "SERIOUS", "SLIGHT")
            )
        )
    )

    query = (
        parsed.writeStream.foreachBatch(write_micro_batch)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )
    print(
        f"Streaming aktif: topic={KAFKA_TOPIC}, trigger={TRIGGER_INTERVAL}, "
        f"database={POSTGRES_DB}"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
