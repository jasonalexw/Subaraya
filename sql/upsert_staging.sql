BEGIN;

INSERT INTO fact_traffic_hourly
SELECT * FROM staging_traffic_hourly
ON CONFLICT (event_hour, zone_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    source_event_time = EXCLUDED.source_event_time,
    traffic_volume = EXCLUDED.traffic_volume,
    average_speed = EXCLUDED.average_speed,
    temperature_c = EXCLUDED.temperature_c,
    rain_1h = EXCLUDED.rain_1h,
    snow_1h = EXCLUDED.snow_1h,
    clouds_all = EXCLUDED.clouds_all,
    weather_main = EXCLUDED.weather_main,
    is_weekend = EXCLUDED.is_weekend,
    is_rush_hour = EXCLUDED.is_rush_hour,
    congestion_level = EXCLUDED.congestion_level,
    ingested_at = EXCLUDED.ingested_at;

INSERT INTO fact_air_quality_hourly
SELECT * FROM staging_air_quality_hourly
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
    ingested_at = EXCLUDED.ingested_at;

INSERT INTO fact_accident_hourly
SELECT * FROM staging_accident_hourly
ON CONFLICT (event_hour, zone_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    source_event_time = EXCLUDED.source_event_time,
    accident_count = EXCLUDED.accident_count,
    fatal_count = EXCLUDED.fatal_count,
    serious_count = EXCLUDED.serious_count,
    slight_count = EXCLUDED.slight_count,
    casualty_count = EXCLUDED.casualty_count,
    vehicle_count = EXCLUDED.vehicle_count,
    ingested_at = EXCLUDED.ingested_at;

WITH changed_keys AS (
    SELECT event_hour, zone_id FROM staging_traffic_hourly
    UNION
    SELECT event_hour, zone_id FROM staging_air_quality_hourly
    UNION
    SELECT event_hour, zone_id FROM staging_accident_hourly
), refreshed AS (
    SELECT
        k.event_hour,
        k.zone_id,
        t.traffic_volume,
        t.average_speed,
        t.congestion_level,
        a.pm25,
        a.pm10,
        COALESCE(a.temperature_c, t.temperature_c) AS temperature_c,
        COALESCE(a.rainfall, t.rain_1h) AS rainfall,
        a.wind_speed,
        COALESCE(c.accident_count, 0) AS accident_count,
        COALESCE(c.casualty_count, 0) AS casualty_count,
        CURRENT_TIMESTAMP AS refreshed_at
    FROM changed_keys k
    LEFT JOIN fact_traffic_hourly t USING (event_hour, zone_id)
    LEFT JOIN fact_air_quality_hourly a USING (event_hour, zone_id)
    LEFT JOIN fact_accident_hourly c USING (event_hour, zone_id)
)
INSERT INTO mart_metropolia_hourly
SELECT * FROM refreshed
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
    refreshed_at = EXCLUDED.refreshed_at;

INSERT INTO etl_batch_log
SELECT * FROM staging_etl_batch_log
ON CONFLICT (batch_id) DO UPDATE SET
    traffic_rows = EXCLUDED.traffic_rows,
    air_quality_rows = EXCLUDED.air_quality_rows,
    accident_rows = EXCLUDED.accident_rows,
    quarantine_rows = EXCLUDED.quarantine_rows,
    status = EXCLUDED.status,
    processed_at = EXCLUDED.processed_at;

COMMIT;
