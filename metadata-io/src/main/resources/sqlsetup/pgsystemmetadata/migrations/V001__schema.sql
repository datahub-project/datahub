-- pgSystemMetadata: ES-shaped system_metadata_service_v1 document store (non-partitioned).
-- Session: SET search_path; table token is the unqualified table name.

CREATE TABLE IF NOT EXISTS __PGSYSTEMMETADATA_TABLE__ (
    doc_id text PRIMARY KEY,
    urn text NOT NULL,
    aspect text NOT NULL,
    run_id text,
    registry_name text,
    registry_version text,
    last_updated bigint,
    removed boolean NOT NULL DEFAULT false,
    document jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx___PGSYSTEMMETADATA_TABLE___urn
    ON __PGSYSTEMMETADATA_TABLE__ (urn);

CREATE INDEX IF NOT EXISTS idx___PGSYSTEMMETADATA_TABLE___run_id
    ON __PGSYSTEMMETADATA_TABLE__ (run_id);

CREATE INDEX IF NOT EXISTS idx___PGSYSTEMMETADATA_TABLE___aspect
    ON __PGSYSTEMMETADATA_TABLE__ (aspect);

CREATE INDEX IF NOT EXISTS idx___PGSYSTEMMETADATA_TABLE___urn_aspect
    ON __PGSYSTEMMETADATA_TABLE__ (urn, aspect);
