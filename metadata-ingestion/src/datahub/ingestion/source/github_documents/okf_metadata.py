# Moved to datahub.ingestion.source.repository_documents.okf_metadata; this module keeps
# the old import path working for code built on github-documents.
from datahub.ingestion.source.repository_documents.okf_metadata import (
    parse_stamp as parse_stamp,
    parse_status as parse_status,
    parse_timestamp as parse_timestamp,
    parse_verified as parse_verified,
)
