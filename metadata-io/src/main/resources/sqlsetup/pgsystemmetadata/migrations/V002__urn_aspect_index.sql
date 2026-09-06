-- Supporting index for findByParams / setDocStatus lookups on (urn, aspect).

CREATE INDEX IF NOT EXISTS idx___PGSYSTEMMETADATA_TABLE___urn_aspect
    ON __PGSYSTEMMETADATA_TABLE__ (urn, aspect);
