-- 01-setup.sql - Initial database and table setup
-- Use SELECT to avoid errors if databases already exist
SELECT 'CREATE DATABASE tsdb' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tsdb')\gexec

SELECT 'CREATE DATABASE tsdb_secondary' 
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tsdb_secondary')\gexec

\c tsdb;

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create regular table (for comparison)
CREATE TABLE regular_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),
    value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create main sensor data table that will become a hypertable
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    device_id INTEGER NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    location VARCHAR(100)
);

-- Convert to hypertable with time and space partitioning
SELECT create_hypertable(
    'sensor_data',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    partitioning_column => 'device_id',
    number_partitions => 4
);

-- Create another hypertable for testing
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

-- Insert sample data
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

-- Create regular PostgreSQL view for comparison
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

-- Create a second schema for testing
CREATE SCHEMA analytics;

-- Create a view in the analytics schema
CREATE VIEW analytics.location_summary AS
SELECT
    location,
    COUNT(DISTINCT device_id) AS device_count,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity
FROM sensor_data
WHERE time > NOW() - INTERVAL '1 day'
GROUP BY location;

-- Analyze tables for statistics
ANALYZE;