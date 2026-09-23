"""Cross-item / cross-workspace SQL name resolution for Fabric OneLake.

Fabric SQL endpoints let a view or query reference tables in *other* items by
display name:

- 3-part ``item.schema.table`` (e.g. ``silver_lh.dbo.customers``) - another
  Lakehouse / Warehouse in the **same workspace**.
- 4-part ``workspace.item.schema.table`` - an item in another workspace.

Dataset URNs, however, are keyed by GUIDs (``<workspaceId>.<itemId>.<schema>.<table>``),
so the SQL parser on its own would emit dangling URNs such as
``fabric-onelake,silver_lh.dbo.customers``.

Resolution is done inside the SQL parser's schema resolver
(``FabricOneLakeSchemaResolver.get_urn_for_table``), which receives the parsed
``_TableName`` for every table reference and rewrites display-name
qualifiers to GUIDs before the URN is built. This keeps the original SQL text
untouched (Query entities and view definitions show what the user wrote) and
lets column-level lineage flow through the normal sqlglot path.

A 3-part name is only meaningful relative to the workspace of the statement
being parsed, so the resolver is *scoped* to a workspace for the duration of
each parse. ``FabricSqlParsingAggregator`` sets that scope from the statement's
``default_db`` (``<workspaceId>.<itemId>``), which is also part of the parser's
cache key, so cached parse results stay consistent with the scope.
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    QuerySubjectsClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver, _TableName
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
    ViewDefinition,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.fabric.onelake.report import (
        FabricOneLakeSourceReport,
    )

# Fabric addresses a table with at most 4 parts: `workspace.item.schema.table`.
_MAX_FABRIC_NAME_PARTS = 4

logger = logging.getLogger(__name__)


def _normalize_token(token: str) -> str:
    """Case-fold an identifier and strip T-SQL / ANSI quoting.

    sqlglot already unquotes ``[name]`` / ``"name"`` identifiers, but names can
    also reach us from other paths (e.g. ``_TableName`` built by hand), so strip
    defensively.
    """
    token = token.strip()
    if len(token) >= 2 and (
        (token[0] == "[" and token[-1] == "]") or (token[0] == token[-1] == '"')
    ):
        token = token[1:-1]
    return token.casefold()


@dataclass(frozen=True)
class FabricItemRef:
    workspace_id: str
    item_id: str
    item_name: str
    item_type: str

    @property
    def default_db(self) -> str:
        """``<workspaceId>.<itemId>`` - the URN prefix for datasets in this item."""
        return f"{self.workspace_id}.{self.item_id}"


@dataclass
class _Resolution:
    item: Optional[FabricItemRef] = None
    reason: Optional[str] = None


@dataclass
class FabricItemCatalog:
    """Display-name -> GUID index of the workspaces and items being ingested."""

    _workspaces_by_name: Dict[str, Set[str]] = field(default_factory=dict)
    _workspace_ids: Dict[str, str] = field(default_factory=dict)
    _items_by_name: Dict[str, Dict[str, List[FabricItemRef]]] = field(
        default_factory=dict
    )
    _items_by_id: Dict[str, Dict[str, FabricItemRef]] = field(default_factory=dict)
    _default_dbs: Set[str] = field(default_factory=set)
    # Workspaces whose lakehouse / warehouse listing failed: their index is
    # incomplete, so a miss there must not be reported as "item not found".
    _incomplete_workspaces: Set[str] = field(default_factory=set)

    def add_workspace(self, workspace_id: str, workspace_name: str) -> None:
        self._workspace_ids[workspace_id.casefold()] = workspace_id
        self._workspaces_by_name.setdefault(
            _normalize_token(workspace_name), set()
        ).add(workspace_id)

    def add_item(
        self, workspace_id: str, item_id: str, item_name: str, item_type: str
    ) -> None:
        ref = FabricItemRef(
            workspace_id=workspace_id,
            item_id=item_id,
            item_name=item_name,
            item_type=item_type,
        )
        ws_key = workspace_id.casefold()
        by_name = self._items_by_name.setdefault(ws_key, {}).setdefault(
            _normalize_token(item_name), []
        )
        if ref not in by_name:
            by_name.append(ref)
        self._items_by_id.setdefault(ws_key, {})[item_id.casefold()] = ref
        self._default_dbs.add(ref.default_db.casefold())

    def mark_listing_failed(self, workspace_id: str) -> None:
        self._incomplete_workspaces.add(workspace_id.casefold())

    @property
    def num_items(self) -> int:
        return sum(len(items) for items in self._items_by_id.values())

    def is_item_default_db(self, database: str) -> bool:
        return database.casefold() in self._default_dbs

    def resolve_workspace(self, token: str) -> Tuple[Optional[str], Optional[str]]:
        """Return ``(workspace_id, failure_reason)`` for a display name or GUID."""
        key = _normalize_token(token)
        if key in self._workspace_ids:
            return self._workspace_ids[key], None
        candidates = self._workspaces_by_name.get(key)
        if not candidates:
            return None, "workspace not found among ingested workspaces"
        if len(candidates) > 1:
            return None, "workspace display name is ambiguous"
        return next(iter(candidates)), None

    def resolve_item(self, workspace_id: str, token: str) -> _Resolution:
        ws_key = workspace_id.casefold()
        key = _normalize_token(token)
        by_id = self._items_by_id.get(ws_key, {})
        if key in by_id:
            return _Resolution(item=by_id[key])
        candidates = self._items_by_name.get(ws_key, {}).get(key, [])
        if not candidates:
            if ws_key in self._incomplete_workspaces:
                return _Resolution(
                    reason="item not found; listing the workspace's items failed"
                )
            return _Resolution(reason="item not found in workspace")
        if len(candidates) > 1:
            return _Resolution(reason="item display name is ambiguous in workspace")
        return _Resolution(item=candidates[0])


class FabricOneLakeSchemaResolver(SchemaResolver):
    """SchemaResolver that maps Fabric display-name qualifiers to GUID URNs.

    - 2-part / unqualified references are qualified by the parser with the
      statement's ``default_db`` (``<workspaceId>.<itemId>``) and pass through.
    - 3-part ``item.schema.table`` resolves ``item`` within the scoped workspace.
    - 4-part ``workspace.item.schema.table`` resolves ``workspace`` then ``item``.

    References that look like cross-item names but cannot be resolved are
    recorded in ``unresolved_urns`` so the source can drop them instead of
    emitting dangling URNs, and a warning is reported once per reference.
    """

    def __init__(
        self,
        *,
        catalog: FabricItemCatalog,
        report: "FabricOneLakeSourceReport",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog = catalog
        self._report = report
        self._scope_workspace_id: Optional[str] = None
        self._seen_references: Set[Tuple[Optional[str], Tuple[str, ...]]] = set()
        self.unresolved_urns: Set[str] = set()
        self._unresolved_names: Set[str] = set()

    @contextlib.contextmanager
    def scoped_to_default_db(self, default_db: Optional[str]) -> Iterator[None]:
        """Scope 3-part name resolution to the workspace of ``default_db``."""
        previous = self._scope_workspace_id
        self._scope_workspace_id = (
            default_db.split(".", 1)[0] if default_db and "." in default_db else None
        )
        try:
            yield
        finally:
            self._scope_workspace_id = previous

    def is_unresolved_name(self, name: str) -> bool:
        return name.casefold() in self._unresolved_names

    def get_urn_for_table(
        self, table: _TableName, lower: bool = False, mixed: bool = False
    ) -> str:
        resolved, is_unresolved = self._resolve_fabric_table(table)
        urn = super().get_urn_for_table(resolved, lower=lower, mixed=mixed)
        if is_unresolved:
            self.unresolved_urns.add(urn)
            # Name as the aggregator's is_allowed_table hook sees it (no
            # platform instance prefix).
            self._unresolved_names.add(
                ".".join(
                    filter(
                        None, [resolved.database, resolved.db_schema, resolved.table]
                    )
                ).casefold()
            )
        return urn

    def _resolve_fabric_table(self, table: _TableName) -> Tuple[_TableName, bool]:
        """Return the (possibly rewritten) table name and whether it is unresolved."""
        parts = table.parts
        if parts and len(parts) == _MAX_FABRIC_NAME_PARTS:
            workspace_token, item_token, schema, table_name = parts
            return self._resolve_four_part(
                table, workspace_token, item_token, schema, table_name
            )
        if parts and len(parts) > _MAX_FABRIC_NAME_PARTS:
            # e.g. a linked-server style name. It can never match a GUID-keyed
            # Fabric dataset, so treat it as unresolved rather than emitting a
            # dangling URN.
            self._record(
                self._scope_workspace_id,
                parts,
                None,
                "more than 4 name parts; not a Fabric item reference",
            )
            return table, True

        database = table.database
        if not database or self.catalog.is_item_default_db(database):
            return table, False
        if "." in database:
            # A `<workspaceId>.<itemId>` default_db we did not list (should not
            # happen) - leave as-is rather than guess.
            return table, False
        return self._resolve_three_part(table, database)

    def _resolve_three_part(
        self, table: _TableName, item_token: str
    ) -> Tuple[_TableName, bool]:
        workspace_id = self._scope_workspace_id
        reference = (item_token, table.db_schema or "", table.table)
        if workspace_id is None:
            self._record(workspace_id, reference, None, "no workspace scope")
            return table, True
        resolution = self.catalog.resolve_item(workspace_id, item_token)
        self._record(workspace_id, reference, resolution.item, resolution.reason)
        if resolution.item is None:
            return table, True
        return (
            _TableName(
                database=resolution.item.default_db,
                db_schema=table.db_schema,
                table=table.table,
            ),
            False,
        )

    def _resolve_four_part(
        self,
        table: _TableName,
        workspace_token: str,
        item_token: str,
        schema: str,
        table_name: str,
    ) -> Tuple[_TableName, bool]:
        reference = (workspace_token, item_token, schema, table_name)
        workspace_id, reason = self.catalog.resolve_workspace(workspace_token)
        item: Optional[FabricItemRef] = None
        if workspace_id is not None:
            resolution = self.catalog.resolve_item(workspace_id, item_token)
            item, reason = resolution.item, resolution.reason
        self._record(self._scope_workspace_id, reference, item, reason)
        if item is None:
            return table, True
        return (
            _TableName(database=item.default_db, db_schema=schema, table=table_name),
            False,
        )

    def _record(
        self,
        scope_workspace_id: Optional[str],
        reference: Tuple[str, ...],
        item: Optional[FabricItemRef],
        reason: Optional[str],
    ) -> None:
        # get_urn_for_table is called several times per table (case variants),
        # so count and warn once per (workspace scope, reference). Tokens are
        # normalized so `[Silver_LH]` and `silver_lh` are one reference.
        key = (
            scope_workspace_id,
            tuple(_normalize_token(token) for token in reference),
        )
        if key in self._seen_references:
            return
        self._seen_references.add(key)
        if item is not None:
            self._report.num_cross_item_references_resolved += 1
            return
        self._report.num_cross_item_references_unresolved += 1
        self._report.warning(
            title="Unresolved Cross-Item SQL Reference",
            message=(
                "A view or query references a Fabric item (or workspace) by a name "
                "that does not match any ingested Lakehouse / Warehouse. Lineage and "
                "usage for this reference are skipped. Ensure the referenced item's "
                "workspace is included in workspace_pattern."
            ),
            context=(
                f"reference={'.'.join(reference)}, "
                f"workspace_id={scope_workspace_id}, reason={reason}"
            ),
            log=False,
        )
        logger.debug(
            f"Unresolved Fabric cross-item reference {'.'.join(reference)} "
            f"(workspace scope={scope_workspace_id}): {reason}"
        )


class FabricSqlParsingAggregator(SqlParsingAggregator):
    """SqlParsingAggregator that scopes the Fabric resolver to each statement.

    Observed queries are parsed synchronously in ``add_observed_query``. View
    definitions are parsed lazily in ``gen_metadata`` (after all schemas are
    registered), one ``_process_view_definition`` call per view - the only
    per-view hook the aggregator exposes - so the scope is set there.
    """

    def __init__(
        self,
        *,
        schema_resolver: FabricOneLakeSchemaResolver,
        **kwargs: Any,
    ) -> None:
        super().__init__(schema_resolver=schema_resolver, **kwargs)
        self._fabric_resolver = schema_resolver

    def add_observed_query(
        self,
        observed: ObservedQuery,
        is_known_temp_table: bool = False,
        require_out_table_schema: bool = False,
    ) -> None:
        with self._fabric_resolver.scoped_to_default_db(observed.default_db):
            super().add_observed_query(
                observed,
                is_known_temp_table=is_known_temp_table,
                require_out_table_schema=require_out_table_schema,
            )

    def _process_view_definition(
        self, view_urn: str, view_definition: ViewDefinition
    ) -> None:
        with self._fabric_resolver.scoped_to_default_db(view_definition.default_db):
            super()._process_view_definition(view_urn, view_definition)


def _dataset_of(urn: str) -> str:
    """Dataset URN for a dataset or schemaField URN (anything else unchanged)."""
    if urn.startswith("urn:li:schemaField:"):
        return SchemaFieldUrn.from_string(urn).parent
    return urn


@dataclass(frozen=True)
class UnresolvedReferenceFilterResult:
    """Outcome of :func:`drop_unresolved_references` for one MCP."""

    # ``None`` when nothing meaningful is left in the aspect.
    mcp: Optional[MetadataChangeProposalWrapper]
    # Dataset-level upstream edges removed from an ``upstreamLineage`` aspect.
    num_upstreams_dropped: int = 0


def drop_unresolved_references(
    mcp: MetadataChangeProposalWrapper, unresolved_urns: Set[str]
) -> UnresolvedReferenceFilterResult:
    """Strip unresolved cross-item URNs from lineage / query-subject aspects.

    Table- and column-level lineage are filtered with the same URN set, so no
    schemaField URN can re-create a dangling dataset. Usage and operation aspects
    are already filtered by the aggregator's ``is_allowed_table`` hook (which
    also skips lineage *for* an unresolved downstream); upstream lists are not,
    hence this pass.
    """
    if not unresolved_urns:
        return UnresolvedReferenceFilterResult(mcp=mcp)
    aspect = mcp.aspect
    if isinstance(aspect, UpstreamLineageClass):
        upstreams = [u for u in aspect.upstreams if u.dataset not in unresolved_urns]
        num_upstreams_dropped = len(aspect.upstreams) - len(upstreams)
        fine_grained = []
        for fgl in aspect.fineGrainedLineages or []:
            kept = [
                u for u in fgl.upstreams or [] if _dataset_of(u) not in unresolved_urns
            ]
            if kept or not fgl.upstreams:
                fgl.upstreams = kept
                fine_grained.append(fgl)
        if num_upstreams_dropped == 0 and len(fine_grained) == len(
            aspect.fineGrainedLineages or []
        ):
            return UnresolvedReferenceFilterResult(mcp=mcp)
        if not upstreams:
            return UnresolvedReferenceFilterResult(
                mcp=None, num_upstreams_dropped=num_upstreams_dropped
            )
        aspect.upstreams = upstreams
        aspect.fineGrainedLineages = fine_grained or None
        return UnresolvedReferenceFilterResult(
            mcp=mcp, num_upstreams_dropped=num_upstreams_dropped
        )
    if isinstance(aspect, QuerySubjectsClass):
        aspect.subjects = [
            s for s in aspect.subjects if _dataset_of(s.entity) not in unresolved_urns
        ]
    return UnresolvedReferenceFilterResult(mcp=mcp)
