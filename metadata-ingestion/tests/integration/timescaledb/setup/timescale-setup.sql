-- 02-timescale-setup.sql - TimescaleDB-specific features (continuous aggregates, policies, etc.)

\c tsdb;

-- Create continuous aggregate for hourly sensor metrics
CREATE MATERIALIZED VIEW sensor_hourly_avg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    device_id,
    location,
    AVG(temperature) AS avg_temperature,
    MAX(temperature) AS max_temperature,
    MIN(temperature) AS min_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(pressure) AS avg_pressure,
    COUNT(*) AS sample_count
FROM sensor_data
GROUP BY hour, device_id, location
WITH NO DATA;

-- Create continuous aggregate for daily system metrics
CREATE MATERIALIZED VIEW system_daily_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    host,
    AVG(cpu_usage) AS avg_cpu,
    MAX(cpu_usage) AS max_cpu,
    AVG(memory_usage) AS avg_memory,
    SUM(disk_io) AS total_disk_io,
    SUM(network_io) AS total_network_io
FROM system_metrics
GROUP BY day, host
WITH NO DATA;

-- Create a more complex continuous aggregate (for lineage testing)
CREATE MATERIALIZED VIEW device_location_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', s.time) AS hour,
    s.location,
    COUNT(DISTINCT s.device_id) AS unique_devices,
    AVG(s.temperature) AS avg_temp,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.humidity) AS median_humidity
FROM sensor_data s
GROUP BY hour, s.location
WITH NO DATA;

-- Add refresh policies for continuous aggregates
SELECT add_continuous_aggregate_policy('sensor_hourly_avg',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('system_daily_stats',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

SELECT add_continuous_aggregate_policy('device_location_hourly',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Configure compression for sensor_data hypertable
ALTER TABLE sensor_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id, location',
    timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy (compress chunks older than 7 days)
SELECT add_compression_policy('sensor_data', INTERVAL '7 days');

-- Add retention policy (drop chunks older than 30 days)
SELECT add_retention_policy('sensor_data', INTERVAL '30 days');

-- Configure compression for system_metrics
ALTER TABLE system_metrics SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC, host'
);

SELECT add_compression_policy('system_metrics', INTERVAL '3 days');

-- Add data retention policy for system_metrics
SELECT add_retention_policy('system_metrics', INTERVAL '14 days');

-- Create stored procedure for manual refresh
CREATE OR REPLACE PROCEDURE refresh_all_aggregates()
LANGUAGE plpgsql
AS $$
BEGIN
    CALL refresh_continuous_aggregate('sensor_hourly_avg', NULL, NULL);
    CALL refresh_continuous_aggregate('system_daily_stats', NULL, NULL);
    CALL refresh_continuous_aggregate('device_location_hourly', NULL, NULL);
    RAISE NOTICE 'All continuous aggregates refreshed successfully';
END;
$$;

-- Create function that returns table (for testing function lineage)
CREATE OR REPLACE FUNCTION get_device_stats(p_device_id INTEGER, p_hours INTEGER DEFAULT 24)
RETURNS TABLE(
    hour TIMESTAMPTZ,
    avg_temp DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        time_bucket('1 hour', time) AS hour,
        AVG(temperature) AS avg_temp,
        AVG(humidity) AS avg_humidity
    FROM sensor_data
    WHERE device_id = p_device_id
        AND time > NOW() - (p_hours || ' hours')::INTERVAL
    GROUP BY hour
    ORDER BY hour DESC;
$$;

-- Create an informational view to see job status
CREATE VIEW job_status AS
SELECT
    j.job_id,
    j.application_name,
    j.proc_name,
    j.schedule_interval,
    j.scheduled,
    j.config,
    js.last_run_started_at,
    js.last_successful_finish
FROM timescaledb_information.jobs j
LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
ORDER BY j.job_id;

-- Refresh continuous aggregates with initial data
CALL refresh_continuous_aggregate('sensor_hourly_avg', NULL, NULL);
CALL refresh_continuous_aggregate('system_daily_stats', NULL, NULL);
CALL refresh_continuous_aggregate('device_location_hourly', NULL, NULL);

-- Create a complex view that uses continuous aggregates (for lineage testing)
CREATE VIEW hourly_summary AS
SELECT
    h.hour,
    h.location,
    h.avg_temperature,
    h.avg_humidity,
    h.sample_count,
    CASE
        WHEN h.avg_temperature > 25 THEN 'Hot'
        WHEN h.avg_temperature < 15 THEN 'Cold'
        ELSE 'Moderate'
    END AS temp_category
FROM sensor_hourly_avg h
WHERE h.hour > NOW() - INTERVAL '24 hours';

-- Grant permissions (if needed for testing)
GRANT USAGE ON SCHEMA analytics TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC;

-- Final analyze to update statistics
ANALYZE;