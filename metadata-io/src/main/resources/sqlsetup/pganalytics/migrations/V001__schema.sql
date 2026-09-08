-- pgAnalytics durable analytics store (timestamptz RANGE partitions via pg_partman).
-- Session: SET search_path + __PGANALYTICS_PREFIX__.

CREATE TABLE IF NOT EXISTS __PGANALYTICS_PREFIX___event (
    event_time timestamptz NOT NULL,
    metric_family text NOT NULL,
    event_id text NOT NULL,
    metric_name text,
    event_type text,
    actor_urn text,
    entity_urn text,
    entity_type text,
    usage_source text,
    browser_id text,
    query text,
    section text,
    action_type text,
    aspect_name text,
    dimensions jsonb,
    document jsonb,
    PRIMARY KEY (event_time, metric_family, event_id)
) PARTITION BY RANGE (event_time);

CREATE INDEX IF NOT EXISTS idx___PGANALYTICS_PREFIX___event_actor_time
    ON __PGANALYTICS_PREFIX___event (actor_urn, event_time DESC);
CREATE INDEX IF NOT EXISTS idx___PGANALYTICS_PREFIX___event_type_time
    ON __PGANALYTICS_PREFIX___event (event_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx___PGANALYTICS_PREFIX___event_time_brin
    ON __PGANALYTICS_PREFIX___event USING brin (event_time);

CREATE TABLE IF NOT EXISTS __PGANALYTICS_PREFIX___rollup (
    bucket_start timestamptz NOT NULL,
    grain text NOT NULL,
    metric_family text NOT NULL,
    metric_name text NOT NULL,
    merge_kind text NOT NULL,
    group_key text NOT NULL,
    group_dims jsonb NOT NULL DEFAULT '{}'::jsonb,
    value_sum double precision NOT NULL DEFAULT 0,
    value_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, grain, metric_family, metric_name, merge_kind, group_key)
) PARTITION BY RANGE (bucket_start);

CREATE INDEX IF NOT EXISTS idx___PGANALYTICS_PREFIX___rollup_family_metric
    ON __PGANALYTICS_PREFIX___rollup (metric_family, metric_name, grain, bucket_start);
CREATE INDEX IF NOT EXISTS idx___PGANALYTICS_PREFIX___rollup_time_brin
    ON __PGANALYTICS_PREFIX___rollup USING brin (bucket_start);

CREATE TABLE IF NOT EXISTS __PGANALYTICS_PREFIX___distinct_set (
    bucket_start timestamptz NOT NULL,
    grain text NOT NULL,
    metric_family text NOT NULL,
    metric_name text NOT NULL,
    actor_class text NOT NULL,
    usage_identity text NOT NULL,
    PRIMARY KEY (bucket_start, grain, metric_family, metric_name, actor_class, usage_identity)
) PARTITION BY RANGE (bucket_start);

CREATE INDEX IF NOT EXISTS idx___PGANALYTICS_PREFIX___distinct_time_brin
    ON __PGANALYTICS_PREFIX___distinct_set USING brin (bucket_start);

CREATE TABLE IF NOT EXISTS __PGANALYTICS_PREFIX___watermark (
    layer text NOT NULL,
    metric_family text NOT NULL,
    partition_key text NOT NULL,
    sealed_through timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, metric_family, partition_key)
);

COMMENT ON TABLE __PGANALYTICS_PREFIX___event IS
    'Raw analytics facts (primarily datahub_usage); document JSONB holds full usage-event payload.';
COMMENT ON TABLE __PGANALYTICS_PREFIX___rollup IS
    'Hour/day/month rollups keyed by metric_family + registry metric_name + merge_kind + group_key.';
COMMENT ON TABLE __PGANALYTICS_PREFIX___distinct_set IS
    'Identity sidecars for merge_kind=distinct (MAU); value_count on rollup = cardinality.';
COMMENT ON TABLE __PGANALYTICS_PREFIX___watermark IS
    'Seal ledger for progressive compaction (hour/day/month layers).';
