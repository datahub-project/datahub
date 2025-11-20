-- Connect to the grafana database
\connect grafana

-- Create test metrics table
CREATE TABLE IF NOT EXISTS test_metrics (
    id SERIAL PRIMARY KEY,
    time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metric VARCHAR(50),
    value NUMERIC,
    dimension VARCHAR(50)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_test_metrics_time ON test_metrics(time);
CREATE INDEX IF NOT EXISTS idx_test_metrics_metric ON test_metrics(metric);

-- Create a function to generate random data
CREATE OR REPLACE FUNCTION generate_random_metrics()
RETURNS TABLE (
    metric_time TIMESTAMP WITH TIME ZONE,
    metric_name VARCHAR(50),
    metric_value NUMERIC,
    metric_dimension VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        NOW() - (i || ' minutes')::interval,
        (CASE (random() * 2)::integer
            WHEN 0 THEN 'response_time'
            WHEN 1 THEN 'error_rate'
            ELSE 'cpu_usage'
        END)::VARCHAR(50),
        (random() * 100)::numeric,
        (CASE (random() * 2)::integer
            WHEN 0 THEN 'api'
            WHEN 1 THEN 'web'
            ELSE 'mobile'
        END)::VARCHAR(50)
    FROM generate_series(0, 60) i;
END;
$$ LANGUAGE plpgsql;

-- Insert initial static test data
INSERT INTO test_metrics (time, metric, value, dimension) VALUES
    (NOW() - interval '1 hour', 'response_time', 100, 'api'),
    (NOW() - interval '50 minutes', 'response_time', 150, 'api'),
    (NOW() - interval '40 minutes', 'response_time', 120, 'api'),
    (NOW() - interval '30 minutes', 'response_time', 200, 'web'),
    (NOW() - interval '20 minutes', 'response_time', 180, 'web'),
    (NOW() - interval '10 minutes', 'response_time', 90, 'mobile'),
    (NOW(), 'response_time', 110, 'mobile');

-- Insert random test data
INSERT INTO test_metrics (time, metric, value, dimension)
SELECT metric_time, metric_name, metric_value, metric_dimension
FROM generate_random_metrics();

-- Create a view for common aggregations
CREATE OR REPLACE VIEW metric_summaries AS
SELECT
    metric,
    dimension,
    AVG(value) as avg_value,
    MAX(value) as max_value,
    MIN(value) as min_value,
    COUNT(*) as count
FROM test_metrics
WHERE time > NOW() - interval '1 hour'
GROUP BY metric, dimension;

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO grafana;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO grafana;