-- DataHub PostgreSQL timeseries aspect store with RANGE partitioning on event_time (timestamptz).
-- Session: SET search_path + __PGTIMESERIES_PREFIX__.
-- Primary key: entity + aspect + message_id + event_time (partition column).
-- JSON documents still carry timestampMillis; writers convert to UTC timestamptz.

CREATE TABLE IF NOT EXISTS __PGTIMESERIES_PREFIX___aspect_row (
    entity_name text NOT NULL,
    aspect_name text NOT NULL,
    urn text NOT NULL,
    message_id text NOT NULL,
    event_time timestamptz NOT NULL,
    run_id text,
    event_granularity text,
    partition_spec jsonb,
    event jsonb,
    system_metadata jsonb,
    document jsonb,
    PRIMARY KEY (entity_name, aspect_name, message_id, event_time)
) PARTITION BY RANGE (event_time);

CREATE INDEX IF NOT EXISTS idx___PGTIMESERIES_PREFIX___aspect_row_lookup
    ON __PGTIMESERIES_PREFIX___aspect_row (entity_name, aspect_name, urn, event_time DESC);

CREATE INDEX IF NOT EXISTS idx___PGTIMESERIES_PREFIX___aspect_row_time_brin
    ON __PGTIMESERIES_PREFIX___aspect_row USING brin (event_time);

COMMENT ON TABLE __PGTIMESERIES_PREFIX___aspect_row IS
    'Time-ordered aspect events; document JSONB holds full ES-shaped payload for future filter parity.';
