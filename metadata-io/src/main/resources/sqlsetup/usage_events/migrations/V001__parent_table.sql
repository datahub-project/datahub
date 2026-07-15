-- Usage events parent table (monthly partitions created at runtime by PostgresUsageEventsStore).
-- Session: SET search_path includes postgres.schema.

CREATE TABLE IF NOT EXISTS __PGUSAGEEVENTS_PARENT_TABLE__ (
  id TEXT NOT NULL,
  timestamp_ms BIGINT NOT NULL,
  event_type TEXT,
  usage_source TEXT,
  actor_urn TEXT,
  entity_urn TEXT,
  entity_type TEXT,
  browser_id TEXT,
  query TEXT,
  section TEXT,
  action_type TEXT,
  aspect_name TEXT,
  document JSONB NOT NULL,
  PRIMARY KEY (timestamp_ms, id)
) PARTITION BY RANGE (timestamp_ms);

CREATE INDEX IF NOT EXISTS idx_du_events_ts ON __PGUSAGEEVENTS_PARENT_TABLE__ (timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_du_events_frontend_ts ON __PGUSAGEEVENTS_PARENT_TABLE__ (timestamp_ms DESC)
  WHERE (usage_source IS NULL OR usage_source <> 'backend');
CREATE INDEX IF NOT EXISTS idx_du_events_actor_ts ON __PGUSAGEEVENTS_PARENT_TABLE__ (actor_urn, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_du_events_type_actor_ts ON __PGUSAGEEVENTS_PARENT_TABLE__ (event_type, actor_urn, timestamp_ms DESC);
