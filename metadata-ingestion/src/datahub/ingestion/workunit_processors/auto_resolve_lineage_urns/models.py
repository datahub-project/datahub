# NOTE: `from __future__ import annotations` keeps the SchemaInfo type hint (imported only
# under TYPE_CHECKING) as a string, so importing this module does not pull in sqlglot. This
# module is reachable from every source's get_workunit_processors() path, so module load
# must stay sqlglot-free (guarded by test_module_import_does_not_pull_sqlglot).
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Literal, Optional, Protocol, Set

from datahub.ingestion.api.workunit_processor import WorkunitProcessorReport
from datahub.metadata.schema_classes import LineageMatchTypeClass
from datahub.utilities.lossy_collections import LossyList

if TYPE_CHECKING:
    from datahub.sql_parsing.schema_resolver import SchemaInfo

# The closed set of matchType verdicts, as a Literal so the if/elif verdict chains that
# drive correctness can be typo- and exhaustiveness-checked. LineageMatchTypeClass
# renders these as plain ``str`` (codegen), so we bind Literal-typed aliases and assert
# they stay in sync with the generated class.
MatchType = Literal["EXACT", "NORMALIZED", "UNRESOLVED"]
EXACT: MatchType = "EXACT"
NORMALIZED: MatchType = "NORMALIZED"
UNRESOLVED: MatchType = "UNRESOLVED"
assert (EXACT, NORMALIZED, UNRESOLVED) == (
    LineageMatchTypeClass.EXACT,
    LineageMatchTypeClass.NORMALIZED,
    LineageMatchTypeClass.UNRESOLVED,
), "MatchType literals drifted from LineageMatchTypeClass"


@dataclass
class AutoResolveLineageUrnsProcessorReport(WorkunitProcessorReport):
    """Report for AutoResolveLineageUrnsProcessor metrics."""

    num_dataset_urns_normalized: int = 0  # Upstream dataset URNs rewritten
    num_column_urns_normalized: int = 0  # Fine-grained field URNs rewritten
    num_refs_unchanged: int = 0  # Left as-is (exact match, or out of scope)
    num_refs_unresolved: int = 0  # In scope, no unique match (flagged)
    num_exceptions: int = 0  # Failed to process a workunit
    # Lineage aspect emitted as a PATCH (not UPSERT); can't be reconciled, so skipped.
    num_patch_lineage_skipped: int = 0
    num_workunits_with_lineage_aspect: int = 0
    num_workunits_modified: int = 0
    # Bounded sample of references left UNRESOLVED, alongside the num_refs_unresolved
    # count, so the report shows *which* lineage looks broken, not just how much.
    unresolved_refs_sample: LossyList[str] = field(default_factory=LossyList)


# Frozen: one Resolution is shared by every reference that resolves to the same entity, so
# mutating one in place would silently change the others.
@dataclass(frozen=True)
class Resolution:
    """Outcome of resolving one dataset URN against the entities DataHub already stores."""

    urn: str  # The (possibly rewritten) URN to emit.
    schema: Optional[SchemaInfo]  # Schema of the resolved entity, if known.
    # EXACT / NORMALIZED / UNRESOLVED / None (no reconciliation performed).
    match_type: Optional[MatchType]


class ResolutionStrategy(Protocol):
    """How the processor turns reference URNs into the URNs DataHub actually stores.

    Implementations differ only in where the answers come from; the processor's rewrite
    and reporting logic is identical either way.
    """

    def resolve_many(
        self, *, urns: Set[str], schema_urns: Set[str]
    ) -> Dict[str, Resolution]:
        """Resolve a batch of upstream references.

        Batch-shaped rather than one URN at a time so that a strategy which pays per
        request can amortise it. ``BulkCatalogStrategy`` gains nothing from this -- its
        catalog is already in memory -- but a strategy that queries the server per lookup
        does, and widening the batch beyond a single work unit then becomes a change to the
        processor's stream handling alone.

        ``schema_urns`` is the subset of ``urns`` reached by column-level lineage, i.e. the
        only ones whose ``Resolution.schema`` needs populating. A strategy that pays per
        schema fetch can therefore skip it entirely for a table-level-only source.

        Must return an entry for *every* URN in ``urns``, using ``match_type=None`` for
        references it considers out of scope. The processor treats a missing key as a bug
        rather than as "out of scope", so that a reference it rewrites but never collected
        fails loudly instead of being silently skipped.
        """
        ...

    def finish(self) -> None:
        """Emit end-of-run reporting that only this strategy can produce."""
        ...
