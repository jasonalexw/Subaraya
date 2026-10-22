CREATE TABLE IF NOT EXISTS dim_zone (
    zone_id VARCHAR(10) PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    road_capacity INTEGER NOT NULL,
    speed_limit_kmh INTEGER NOT NULL
);

INSERT INTO dim_zone (
    zone_id, zone_name, latitude, longitude, road_capacity, speed_limit_kmh
) VALUES
    ('Z01', 'Pusat Kota', -7.2575, 112.7521, 6000, 50),
    ('Z02', 'Kawasan Industri', -7.2450, 112.7200, 5000, 60),
    ('Z03', 'Permukiman Utara', -7.2100, 112.7400, 4000, 40),
    ('Z04', 'Area Sekolah', -7.2750, 112.7700, 3000, 30)
ON CONFLICT (zone_id) DO UPDATE SET
    zone_name = EXCLUDED.zone_name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    road_capacity = EXCLUDED.road_capacity,
    speed_limit_kmh = EXCLUDED.speed_limit_kmh;

CREATE TABLE IF NOT EXISTS fact_traffic_hourly (
    event_id VARCHAR(64) NOT NULL,
    event_hour TIMESTAMP NOT NULL,
    source_event_time TIMESTAMP,
    zone_id VARCHAR(10) NOT NULL REFERENCES dim_zone(zone_id),
    traffic_volume INTEGER NOT NULL,
    average_speed DOUBLE PRECISION,
    temperature_c DOUBLE PRECISION,
    rain_1h DOUBLE PRECISION,
    snow_1h DOUBLE PRECISION,
    clouds_all DOUBLE PRECISION,
    weather_main VARCHAR(100),
    is_weekend INTEGER NOT NULL,
    is_rush_hour INTEGER NOT NULL,
    congestion_level VARCHAR(20) NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (event_hour, zone_id),
    UNIQUE (event_id)
);

CREATE TABLE IF NOT EXISTS fact_air_quality_hourly (
    event_id VARCHAR(64) NOT NULL,
    event_hour TIMESTAMP NOT NULL,
    source_event_time TIMESTAMP,
    zone_id VARCHAR(10) NOT NULL REFERENCES dim_zone(zone_id),
    pm25 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    co DOUBLE PRECISION,
    o3 DOUBLE PRECISION,
    temperature_c DOUBLE PRECISION,
    rainfall DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    station_count INTEGER NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (event_hour, zone_id),
    UNIQUE (event_id)
);

CREATE TABLE IF NOT EXISTS fact_accident_hourly (
    event_id VARCHAR(64) NOT NULL,
    event_hour TIMESTAMP NOT NULL,
    source_event_time TIMESTAMP,
    zone_id VARCHAR(10) NOT NULL REFERENCES dim_zone(zone_id),
    accident_count INTEGER NOT NULL,
    fatal_count INTEGER NOT NULL,
    serious_count INTEGER NOT NULL,
    slight_count INTEGER NOT NULL,
    casualty_count INTEGER NOT NULL,
    vehicle_count INTEGER NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (event_hour, zone_id),
    UNIQUE (event_id)
);

CREATE TABLE IF NOT EXISTS mart_metropolia_hourly (
    event_hour TIMESTAMP NOT NULL,
    zone_id VARCHAR(10) NOT NULL REFERENCES dim_zone(zone_id),
    traffic_volume INTEGER,
    average_speed DOUBLE PRECISION,
    congestion_level VARCHAR(20),
    pm25 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    temperature_c DOUBLE PRECISION,
    rainfall DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    accident_count INTEGER NOT NULL DEFAULT 0,
    casualty_count INTEGER NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_hour, zone_id)
);

CREATE TABLE IF NOT EXISTS etl_batch_log (
    batch_id VARCHAR(160) PRIMARY KEY,
    mode VARCHAR(30) NOT NULL,
    process_start_date DATE NOT NULL,
    process_end_date DATE NOT NULL,
    traffic_rows INTEGER NOT NULL,
    air_quality_rows INTEGER NOT NULL,
    accident_rows INTEGER NOT NULL,
    quarantine_rows INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE OR REPLACE VIEW mart_metropolia_daily AS
SELECT
    event_hour::date AS event_date,
    zone_id,
    SUM(traffic_volume) AS traffic_volume_total,
    AVG(average_speed) AS average_speed,
    AVG(pm25) AS average_pm25,
    MAX(pm25) AS maximum_pm25,
    SUM(accident_count) AS accident_count,
    SUM(casualty_count) AS casualty_count
FROM mart_metropolia_hourly
GROUP BY event_hour::date, zone_id;

CREATE OR REPLACE VIEW mart_metropolia_monthly AS
SELECT
    DATE_TRUNC('month', event_hour)::date AS event_month,
    zone_id,
    AVG(traffic_volume) AS average_hourly_traffic,
    AVG(average_speed) AS average_speed,
    AVG(pm25) AS average_pm25,
    SUM(accident_count) AS accident_count,
    SUM(casualty_count) AS casualty_count
FROM mart_metropolia_hourly
GROUP BY DATE_TRUNC('month', event_hour)::date, zone_id;

CREATE OR REPLACE VIEW model_training_dataset AS
SELECT
    m.*,
    LEAD(traffic_volume, 1) OVER (
        PARTITION BY zone_id ORDER BY event_hour
    ) AS traffic_volume_next_1h,
    LEAD(pm25, 1) OVER (
        PARTITION BY zone_id ORDER BY event_hour
    ) AS pm25_next_1h,
    CASE
        WHEN LEAD(accident_count, 1) OVER (
            PARTITION BY zone_id ORDER BY event_hour
        ) > 0 THEN 1 ELSE 0
    END AS accident_next_1h
FROM mart_metropolia_hourly m;
