-- Supporting index for aspect-wide truncate / retention deletes
-- (entity_name, aspect_name, event_time) without urn in the middle.

CREATE INDEX IF NOT EXISTS idx___PGTIMESERIES_PREFIX___aspect_truncate
    ON __PGTIMESERIES_PREFIX___aspect (entity_name, aspect_name, event_time);
