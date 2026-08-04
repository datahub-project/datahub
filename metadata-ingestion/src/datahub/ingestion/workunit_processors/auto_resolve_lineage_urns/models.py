# NOTE: `from __future__ import annotations` keeps the SchemaInfo type hint (imported only
# under TYPE_CHECKING) as a string, so importing this module does not pull in sqlglot. This
# module is reachable from every source's get_workunit_processors() path, so module load
# must stay sqlglot-free (guarded by test_module_import_does_not_pull_sqlglot).
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Optional, Protocol

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


@dataclass
class Resolution:
    """Outcome of resolving one dataset URN against the entities DataHub already stores."""

    urn: str  # The (possibly rewritten) URN to emit.
    schema: Optional[SchemaInfo]  # Schema of the resolved entity, if known.
    # EXACT / NORMALIZED / UNRESOLVED / None (no reconciliation performed).
    match_type: Optional[MatchType]


class ResolutionStrategy(Protocol):
    """How the processor turns a reference URN into the URN DataHub actually stores.

    Implementations differ only in where the answer comes from; the processor's rewrite
    and reporting logic is identical either way.
    """

    def resolve(self, urn: str, *, need_schema: bool = False) -> Resolution:
        """Resolve one upstream reference.

        ``need_schema`` is set only on the column-level path, so a strategy that pays per
        schema fetch can skip it for table-level-only references.
        """
        ...

    def finish(self) -> None:
        """Emit end-of-run reporting that only this strategy can produce."""
        ...
