-- Idempotent CREATE DATABASE — running this file twice must not error.
SELECT 'CREATE DATABASE tsdb' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tsdb')\gexec

SELECT 'CREATE DATABASE tsdb_secondary' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tsdb_secondary')\gexec

\c tsdb;

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE regular_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),
    value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    device_id INTEGER NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    location VARCHAR(100)
);

SELECT create_hypertable(
    'sensor_data',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    partitioning_column => 'device_id',
    number_partitions => 4
);

CREATE TABLE system_metrics (
    time TIMESTAMPTZ NOT NULL,
    host VARCHAR(100) NOT NULL,
    cpu_usage DOUBLE PRECISION,
    memory_usage DOUBLE PRECISION,
    disk_io BIGINT,
    network_io BIGINT
);

SELECT create_hypertable(
    'system_metrics',
    'time',
    chunk_time_interval => INTERVAL '1 hour'
);

INSERT INTO sensor_data (time, device_id, temperature, humidity, pressure, location)
SELECT
    time_bucket('1 minute', generate_series(
        NOW() - INTERVAL '7 days',
        NOW(),
        INTERVAL '1 minute'
    )) AS time,
    (random() * 10)::INT AS device_id,
    20 + (random() * 10) AS temperature,
    40 + (random() * 40) AS humidity,
    1000 + (random() * 50) AS pressure,
    CASE
        WHEN random() < 0.25 THEN 'Building A'
        WHEN random() < 0.5 THEN 'Building B'
        WHEN random() < 0.75 THEN 'Building C'
        ELSE 'Building D'
    END AS location;

INSERT INTO system_metrics (time, host, cpu_usage, memory_usage, disk_io, network_io)
SELECT
    generate_series(
        NOW() - INTERVAL '3 days',
        NOW(),
        INTERVAL '5 minutes'
    ) AS time,
    CASE
        WHEN random() < 0.33 THEN 'server-1'
        WHEN random() < 0.66 THEN 'server-2'
        ELSE 'server-3'
    END AS host,
    random() * 100 AS cpu_usage,
    random() * 100 AS memory_usage,
    (random() * 1000000)::BIGINT AS disk_io,
    (random() * 1000000)::BIGINT AS network_io;

CREATE VIEW sensor_latest AS
SELECT
    device_id,
    location,
    temperature,
    humidity,
    pressure,
    time
FROM sensor_data
WHERE time > NOW() - INTERVAL '1 hour'
ORDER BY time DESC;

CREATE SCHEMA analytics;

CREATE VIEW analytics.location_summary AS
SELECT
    location,
    COUNT(DISTINCT device_id) AS device_count,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity
FROM sensor_data
WHERE time > NOW() - INTERVAL '1 day'
GROUP BY location;

ANALYZE;

-- `timescaledb_all_db.yml` filters to [tsdb, tsdb_secondary]; without this
-- secondary database the multi-DB iteration path in PostgresSource isn't
-- actually exercised. Keep it minimal — one hypertable is enough.
\c tsdb_secondary;

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE events (
    time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB
);

SELECT create_hypertable(
    'events',
    'time',
    chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO events (time, event_type, payload)
SELECT
    generate_series(
        NOW() - INTERVAL '1 day',
        NOW(),
        INTERVAL '1 hour'
    ) AS time,
    'sample_event' AS event_type,
    '{"source": "tsdb_secondary"}'::jsonb AS payload;

ANALYZE;