-- System metadata documents when elasticsearch.enabled=false (Postgres-backed).
-- Preconditions: session search_path includes postgres.schema (see PostgresSqlSetupSession).

CREATE TABLE IF NOT EXISTS __PGSYSTEMMETADATA_TABLE__ (
  doc_id TEXT PRIMARY KEY,
  urn TEXT NOT NULL,
  aspect TEXT NOT NULL,
  run_id TEXT,
  registry_name TEXT,
  registry_version TEXT,
  last_updated BIGINT,
  removed BOOLEAN NOT NULL DEFAULT FALSE,
  document JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_system_metadata_urn ON __PGSYSTEMMETADATA_TABLE__ (urn);
CREATE INDEX IF NOT EXISTS idx_system_metadata_run_id ON __PGSYSTEMMETADATA_TABLE__ (run_id);
