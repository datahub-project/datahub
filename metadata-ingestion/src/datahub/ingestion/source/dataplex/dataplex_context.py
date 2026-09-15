"""Shared context object threaded through all Dataplex ingestion processors."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    import google.auth.transport.requests
    from google.oauth2 import service_account

    from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
    from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple


@dataclass
class DataplexContext:
    """Shared state threaded through all Dataplex ingestion processors.

    Created once in DataplexSource.__init__ and passed to every processor.
    Mutable fields are populated incrementally during the ingestion pipeline
    and consumed by downstream stages.
    """

    config: "DataplexConfig"
    credentials: Optional["service_account.Credentials"]

    # Populated during the entries stage; consumed by lineage + glossary stages.
    entry_data: List["EntryDataTuple"] = field(default_factory=list)

    # Maps Dataplex entry resource name -> DataHub entity urn, for BOTH Dataset and
    # Container entities. Consumed by the glossary term-association stage: a Dataplex
    # term can be linked to a BigQuery dataset, which this connector models as a
    # DataHub Container, not a Dataset. Deliberately separate from ``entry_data``,
    # which is a lineage-only index restricted to Dataset entities.
    entry_name_to_urn: Dict[str, str] = field(default_factory=dict)

    # Maps project_id -> project_number.
    # Required by the lookupEntryLinks REST API which demands project number (not ID)
    # inside the glossary term entry path. Populated at startup only when
    # include_glossary_term_associations=True.
    project_numbers: Dict[str, str] = field(default_factory=dict)

    # Shared AuthorizedSession for REST calls (lookupEntryLinks).
    # Populated at startup only when include_glossary_term_associations=True.
    authed_session: Optional["google.auth.transport.requests.AuthorizedSession"] = None

    def __post_init__(self) -> None:
        self._entry_data_lock = threading.Lock()

    def append_entry(self, entry: "EntryDataTuple") -> None:
        """Thread-safe append to entry_data."""
        with self._entry_data_lock:
            self.entry_data.append(entry)

    def register_entry_urn(self, dataplex_entry_name: str, datahub_urn: str) -> None:
        """Thread-safe registration of the entry-name -> DataHub entity urn mapping."""
        with self._entry_data_lock:
            self.entry_name_to_urn[dataplex_entry_name] = datahub_urn
