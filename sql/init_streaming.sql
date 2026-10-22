CREATE TABLE IF NOT EXISTS etl_stream_event_log (
    event_id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(30) NOT NULL,
    topic VARCHAR(200) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    event_hour TIMESTAMP NOT NULL,
    zone_id VARCHAR(10) NOT NULL REFERENCES dim_zone(zone_id),
    emitted_at TIMESTAMP,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL,
    UNIQUE (topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_stream_event_processed_at
    ON etl_stream_event_log(processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_stream_event_hour_zone
    ON etl_stream_event_log(event_hour, zone_id);

CREATE TABLE IF NOT EXISTS etl_stream_batch_log (
    batch_id BIGINT PRIMARY KEY,
    received_rows INTEGER NOT NULL,
    accepted_rows INTEGER NOT NULL,
    traffic_rows INTEGER NOT NULL,
    air_quality_rows INTEGER NOT NULL,
    accident_rows INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW v_metropolia_stream_status AS
SELECT
    event_type,
    COUNT(*) AS total_events,
    MAX(event_hour) AS latest_event_hour,
    MAX(emitted_at) AS latest_emitted_at,
    MAX(processed_at) AS latest_processed_at
FROM etl_stream_event_log
GROUP BY event_type;
