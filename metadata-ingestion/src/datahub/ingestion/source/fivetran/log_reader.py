"""Protocols for the connector-reading subsystem of the Fivetran source.

`FivetranSource` consumes only the methods listed in
`FivetranConnectorReader`. Two implementations exist:
- `FivetranLogDbReader` (queries the Fivetran log via SQLAlchemy from the
  destination warehouse — Snowflake / BigQuery / Databricks; the default,
  backward-compatible mode)
- `FivetranLogRestReader` (reads connector metadata via the Fivetran REST
  API; opt-in via `log_source: rest_api`)

`FivetranJobsReader` is a separate, narrower contract for the hybrid
mode where the REST reader is primary but per-run sync history comes
from a database log. Splitting it out keeps `FivetranLogRestReader`
from depending on the concrete `FivetranLogDbReader` class — any future
job-fetching backend (e.g. read sync history from S3) can implement
`FivetranJobsReader` without subclassing.

Adding methods here is a contract change — every implementation must
implement them. Prefer narrow, source-facing methods over leaking the
underlying access mechanism.
"""

from typing import Dict, List, Optional, Protocol, runtime_checkable

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.fivetran.data_classes import (
    Connector,
    Job,
    TableLineage,
)


@runtime_checkable
class FivetranConnectorReader(Protocol):
    """Source-facing interface for enumerating Fivetran connectors.

    Returns `List[Connector]` populated with lineage and (mode-permitting)
    run history. Both DB-mode and REST-mode implementations satisfy this
    contract; the source doesn't care which one it has.
    """

    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        syncs_interval: int,
    ) -> List[Connector]:
        """Return all connectors that pass the supplied filters, with
        their lineage and run history populated. Implementations hold
        their own `FivetranSourceReport` reference (passed via __init__)
        rather than receiving it per-call.
        """
        ...

    def get_user_email(self, user_id: Optional[str]) -> Optional[str]:
        """Return the email for a given user_id, or None if not known."""
        ...


@runtime_checkable
class FivetranJobsReader(Protocol):
    """Narrow interface for fetching per-connector sync-history jobs.

    Used by `FivetranLogRestReader` in hybrid mode to plug in run-history
    data when the REST API itself doesn't expose it. `FivetranLogDbReader`
    structurally satisfies this Protocol; future backends (S3-based
    sync log readers, etc.) can do the same without touching the REST
    reader's source code.
    """

    def fetch_jobs_for_connectors(
        self, connector_ids: List[str], syncs_interval: int
    ) -> Dict[str, List[Job]]:
        """Return a mapping of connector_id → recent Job entries within
        `syncs_interval` days. Connectors with no recent runs may be
        absent from the dict; callers should default to `[]`.
        """
        ...


@runtime_checkable
class FivetranLineageReader(Protocol):
    """Narrow interface for fetching table+column lineage from a backup
    source — used as a fallback by `FivetranLogRestReader` when the
    Fivetran Metadata API is unavailable (e.g. plan-restricted to
    Standard+).

    With this Protocol, REST-primary hybrid mode can recover full
    column lineage from the DB log's lineage tables instead of degrading
    to the table-only schemas-endpoint fallback. `FivetranLogDbReader`
    structurally satisfies this Protocol.
    """

    def fetch_lineage_for_connectors(
        self, connector_ids: List[str]
    ) -> Dict[str, List[TableLineage]]:
        """Return a mapping of connector_id → table+column lineage
        entries. Connectors with no lineage in this backend may be
        absent from the dict; callers should default to `[]`.
        """
        ...
