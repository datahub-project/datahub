from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pydantic
from pydantic import BaseModel, Field

from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.api.report import EntityFilterReport, Report
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.lossy_collections import LossyList


class Constant:
    """
    keys used in sigma plugin
    """

    # Rest API response key constants
    REFRESH_TOKEN = "refresh_token"
    ACCESS_TOKEN = "access_token"
    EMAIL = "email"
    ENTRIES = "entries"
    MEMBERID = "memberId"
    EDGES = "edges"
    DEPENDENCIES = "dependencies"
    SOURCE = "source"
    TARGET = "target"
    WORKSPACEID = "workspaceId"
    PATH = "path"
    NAME = "name"
    URL = "url"
    ELEMENTID = "elementId"
    ID = "id"
    PARENTID = "parentId"
    TYPE = "type"
    DATASET = "dataset"
    WORKBOOK = "workbook"
    BADGE = "badge"
    NEXTPAGE = "nextPage"
    NEXTPAGETOKEN = "nextPageToken"
    DATA_MODEL = "data-model"
    DATA_MODEL_ID = "dataModelId"

    # Source Config constants
    DEFAULT_API_URL = "https://aws-api.sigmacomputing.com/v2"


class WorkspaceCounts(BaseModel):
    workbooks_count: int = 0
    datasets_count: int = 0
    elements_count: int = 0
    pages_count: int = 0
    data_models_count: int = 0
    data_model_elements_count: int = 0

    def is_empty(self) -> bool:
        return (
            self.workbooks_count == 0
            and self.datasets_count == 0
            and self.elements_count == 0
            and self.pages_count == 0
            and self.data_models_count == 0
            and self.data_model_elements_count == 0
        )

    def as_obj(self) -> dict:
        return {
            "workbooks_count": self.workbooks_count,
            "datasets_count": self.datasets_count,
            "elements_count": self.elements_count,
            "pages_count": self.pages_count,
            "data_models_count": self.data_models_count,
            "data_model_elements_count": self.data_model_elements_count,
        }


class SigmaWorkspaceEntityFilterReport(EntityFilterReport):
    type: str = "workspace"

    workspace_counts: Dict[str, WorkspaceCounts] = Field(
        default_factory=dict,
        description="Counts of workbooks, datasets, elements and pages in each workspace.",
    )

    def increment_workbooks_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].workbooks_count += 1

    def increment_datasets_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].datasets_count += 1

    def increment_elements_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].elements_count += 1

    def increment_pages_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].pages_count += 1

    def increment_data_models_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].data_models_count += 1

    def increment_data_model_elements_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].data_model_elements_count += 1

    def as_obj(self) -> dict:
        return {
            "filtered": self.dropped_entities.as_obj(),
            "processed": self.processed_entities.as_obj(),
            "workspace_counts": {
                key: item.as_obj() for key, item in self.workspace_counts.items()
            },
        }


@dataclass
class ElementDmEdgeReport(Report):
    """Workbook-to-DM-element bridge counters. Matched by element name."""

    # Edge emitted as a unique chart input.
    resolved: int = 0
    # URN already present on the chart (e.g. diamond lineage).
    deduped: int = 0
    # Multiple elements share the name; deterministic pick.
    ambiguous: int = 0
    # DM ingested but name did not match (no edge; ChartInfo.inputs requires
    # Dataset URNs, not Container URNs).
    name_unmatched_but_dm_known: int = 0
    # Lineage node had no ``name``; distinct from the rename-miss counter.
    upstream_name_missing: int = 0
    # DM not in this run.
    unresolved: int = 0
    # Sigma places the DM-reference node only in ``edges[].source`` (not as a
    # ``dependencies`` key). We synthesize the upstream using the workbook
    # element's own ``name`` (Sigma's default mirrors the DM element name).
    synthesized_from_edge_only: int = 0


@dataclass
class SigmaSourceReport(StaleEntityRemovalSourceReport):
    workspaces: SigmaWorkspaceEntityFilterReport = field(
        default_factory=SigmaWorkspaceEntityFilterReport
    )
    non_accessible_workspaces_count: int = 0

    datasets: EntityFilterReport = EntityFilterReport.field(type="dataset")
    datasets_without_workspace: int = 0

    workbooks: EntityFilterReport = EntityFilterReport.field(type="workbook")
    workbooks_without_workspace: int = 0

    data_models: EntityFilterReport = EntityFilterReport.field(type="data_model")
    data_models_without_workspace: int = 0

    number_of_files_metadata: Dict[str, int] = field(default_factory=dict)
    empty_workspaces: List[str] = field(default_factory=list)

    # Sheet upstream skipped because the upstream element was filtered out
    # of the chart map (e.g. pivot-table blocked by page-element allowlist).
    num_filtered_sheet_upstreams: int = 0

    # Chart upstream node of type=dataset had ``name=None``. The SQL-bridge
    # cannot correlate it against a warehouse table, so the chart-to-Sigma-
    # dataset edge is skipped. Pre-PR this raised ValidationError; this
    # counter restores the observability signal.
    chart_dataset_upstream_name_missing: int = 0

    # Chart InputFields — one counter fires per chart column (not per formula ref).
    # The resolver (_resolve_chart_formula_upstream) is a pure predicate: it
    # returns a resolved (urn, field) pair or None; all counting happens in
    # _build_element_input_fields so every column lands in exactly one bucket.
    # Invariant: resolved + self_ref_fallback + skipped_parameter + skipped_sibling
    #            == total chart columns processed across the workbook.
    chart_input_fields_resolved: int = 0
    # Column emitted with self-referential schemaFieldUrn because no formula ref
    # resolved (includes: no-formula column, unresolvable ref, mixed param+real
    # where the real refs fail). Keeps V2 column list visible unconditionally.
    chart_input_fields_self_ref_fallback: int = 0
    # Breakdown of the self-ref fallback bucket above, which is normally the
    # largest in the report and previously said nothing about cause.
    # No formula at all on the column -- expected, not a defect.
    chart_input_fields_self_ref_no_formula: int = 0
    # Column HAD formula refs and none resolved. This is the population worth
    # investigating; a non-trivial value here alongside
    # chart_input_fields_multi_segment_ref suggests the chart path shares the
    # Data Model join-chain parse defect.
    chart_input_fields_self_ref_unresolved_refs: int = 0
    # Chart columns carrying a join-chain-shaped ref
    # ([JoinElement/SourceElement/Column], 3+ segments). The chart resolver has
    # no schema check, so unlike the DM path a mis-split emits a wrong or
    # dangling InputField rather than being caught. Non-zero means the chart
    # path needs the same candidate-split resolution as the DM path.
    chart_input_fields_multi_segment_ref: int = 0
    # Workbook elements dropped before indexing because their type is neither
    # "table" nor "visualization", keyed by type. These never enter the workbook
    # element index, so a chart formula naming one can never resolve and falls
    # back to a self-reference. Types such as pivot-table and input-table carry
    # real data, so a large count here is a candidate explanation for the size
    # of chart_input_fields_self_ref_unresolved_refs.
    workbook_elements_skipped_by_type: Dict[str, int] = field(default_factory=dict)
    # Sub-count of chart_ref_source_normalized_ambiguous: the ref's source
    # normalizes onto real element names, but onto MORE than one distinct name,
    # so resolving would be a guess. Only reachable after the normalized retry
    # has already run, so it no longer means "the lookup is too strict".
    chart_ref_source_near_miss: int = 0
    # Of those, the ones actually resolved by the normalized lookup.
    chart_ref_source_normalized_match: int = 0
    # Normalized key collapsed two or more genuinely distinct element names;
    # left unresolved rather than guessed at.
    chart_ref_source_normalized_ambiguous: int = 0

    # Warehouse columns accepted although the element does not declare the
    # inode: it declares no inode at all (it is sourced transitively, e.g. only
    # from other Data Models) and the url_id is in this Data Model's warehouse
    # map. Kept separate so the relaxation is auditable against the
    # url_id_not_in_element_source_ids misses it replaces.
    dm_element_warehouse_transitive_inode_accepted: int = 0
    # Data Model elements for which no source_id resolved to a URN, so no
    # upstreamLineage aspect is emitted and no column lineage is even attempted.
    # The difference between data_model_elements_emitted and the number of
    # elements carrying upstreamLineage; previously that gap had no counter and
    # no log line.
    data_model_element_no_upstreams: int = 0
    # Warehouse tables recovered by asking /v2/files/{urlId} directly, because
    # the owning Data Model's /lineage did not describe them. Sigma reports
    # fewer type=table rows than its own elements reference; without this those
    # elements resolve to no warehouse table at all.
    dm_element_warehouse_recovered_by_url_id_lookup: int = 0
    # Sub-count of the above measured at the COLUMN level: a column's
    # inode-shaped columnId named a table the Data Model's /lineage omits, and
    # the direct /v2/files/{urlId} lookup supplied it. Before this the recovery
    # reached only the entity-level upstream, leaving the column that motivated
    # it unresolved -- 1,305 columns on one tenant (2026-09).
    dm_element_warehouse_column_recovered_by_lookup: int = 0
    # url_ids skipped because the connection to attribute them to could not be
    # inferred unambiguously (a /files entry carries no connectionId).
    dm_element_warehouse_connection_ambiguous: int = 0
    # A Data Model element references a warehouse table by urlId that
    # /v2/files/{urlId} returns 404 for. The file may be deleted, or merely
    # outside what the ingestion credential can see -- the API does not
    # distinguish them -- so nothing is inferred beyond "not resolvable by this
    # token". Columns naming it get no warehouse column lineage.
    dm_element_warehouse_url_id_unresolvable: int = 0
    # Paginated calls that aborted partway (HTTP error or a repeated cursor),
    # losing every entry after the failure point. The per-abort warnings group
    # under one title, so before this counter existed the report showed a single
    # warning no matter how many endpoints were truncated -- 29 aborts on one
    # tenant read as "1".
    pagination_aborted: int = 0
    # Data Models whose /columns pagination aborted. /columns is the sole source
    # of formulas and columnIds, so these models lose column lineage for reasons
    # unrelated to resolution. Check this before reading a model's missing FGL
    # as a resolver defect.
    data_model_columns_fetch_partial: int = 0
    # Entries returned by the /v2/files warehouse-table listing, which backs the
    # by-name fallback only. A value that is an exact round number (10,000)
    # would suggest a server-side cap; a live tenant returned 40,564 across 41 (2026-09)
    # pages, so the listing itself is complete.
    warehouse_files_listed: int = 0
    # Formula refs naming a warehouse TABLE the element declares, resolved to
    # that table's column. The columnId path covers pass-throughs; this covers
    # columns with an opaque columnId, whose display name is the only remaining
    # signal for the warehouse column name -- so these edges carry a reduced
    # confidence score.
    data_model_element_fgl_warehouse_table_name_resolved: int = 0
    # Warehouse column names confirmed against the schema DataHub already holds
    # for that table, rather than derived from Sigma's display-name convention
    # ("Order Ref Id" -> ORDER_REF_ID). A confirmed name is emitted verbatim and
    # scores as high as a columnId-derived one, because it IS what the warehouse
    # connector emitted.
    warehouse_column_verified_against_graph: int = 0
    # DataHub holds no schema for the table -- no graph, or the warehouse
    # connector has not ingested it. The derived name stands: the dataset is an
    # un-ingested stub, so there is no schema for a wrong name to contradict.
    warehouse_column_unverifiable_no_schema: int = 0
    # Split of the above, because the two need different responses. No graph at
    # all -- a file sink or a dry run -- is nothing an operator can act on.
    warehouse_column_no_graph_configured: int = 0
    # DataHub is reachable but has never ingested that warehouse table. This
    # one IS actionable: run the warehouse connector and these become
    # verifiable rather than guessed.
    warehouse_column_table_not_in_datahub: int = 0
    # The read itself failed -- a bad token, a network fault, GMS down. Counted
    # apart from the above because the operator response is completely
    # different, and because a run where EVERY read 401s would otherwise report
    # a tenant-wide "your warehouse is not ingested" that is simply false.
    warehouse_column_schema_unreadable: int = 0
    # DataHub HAS the schema and neither the display name nor the derived name
    # matches any field in it. The edge is still emitted at the reduced
    # confidence, but this is the population where the derived name is provably
    # a dangling field reference -- the number to watch.
    warehouse_column_absent_from_graph_schema: int = 0
    # A graph read raised. Never fatal: the derived name is what this connector
    # used before the check existed.
    warehouse_schema_lookup_failed: int = 0
    # Sub-count of the above: the table was not declared by the element either,
    # and was found by NAME in the tenant-wide /v2/files listing. Both the table
    # and the column are inferred, so these carry the lowest confidence score of
    # any warehouse edge -- watch this number if spurious edges are reported.
    data_model_element_fgl_warehouse_global_name_resolved: int = 0
    # A formula named a warehouse table that appears nowhere in the /v2/files
    # listing. Distinguishes "Sigma under-reported the element's tables" (which
    # the name index fixes) from "the ref does not name a warehouse table at
    # all" -- most of these are refs to something other than a table.
    dm_element_warehouse_name_index_miss: int = 0
    # A formula named a warehouse table that several /v2/files tables share, and
    # narrowing to the Data Model's own databases/schemas did not leave exactly
    # one. Refused rather than guessed: the wrong pick emits an edge to a real
    # but unrelated dataset. A large value means table names collide heavily on
    # this tenant and some legitimate edges are being left on the table.
    dm_element_warehouse_name_index_ambiguous: int = 0
    # A formula-named warehouse table resolved unambiguously via the /v2/files
    # name index.
    dm_element_warehouse_name_index_resolved: int = 0

    # --- Join-key lineage, from /v2/dataModels/{id}/spec ---
    # /spec could not be fetched for a Data Model (commonly a token without the
    # data model read scope). Those models get no join-key edges at all.
    data_model_spec_fetch_failed: int = 0
    # Join predicates read across all Data Models. Zero on a tenant that has
    # joins means the spec's join shape does not match what the parser expects
    # -- check data_model_join_elements_unreadable and the DM SPEC JOIN debug
    # lines, which log the descriptor's key skeleton.
    # Every joinType seen, verbatim. The outer-join tier (0.6 instead of 0.7)
    # keys off this, and the accepted vocabulary is inner / left-outer /
    # right-outer / full-outer -- verified against Sigma's own write API, which
    # rejects everything else. Any OTHER value appearing here means a real shape
    # is being mis-scored: the previous set was invented, listed four values
    # Sigma does not accept, and omitted "left-outer" entirely.
    data_model_join_types: Dict[str, int] = field(default_factory=dict)
    # Every join predicate operator seen. Sigma's vocabulary is "=", "!=", "<",
    # "<=", ">", ">=", "within", "intersects", and the key is OPTIONAL meaning
    # "=" -- all confirmed accepted by POST /dataModels/spec. An earlier note
    # recorded that predicates carry no op at all, which was wrong: the probe
    # had sent "equals", an invalid VALUE, and read the rejection as absence.
    data_model_join_predicate_ops: Dict[str, int] = field(default_factory=dict)
    # Predicates declined because their operator is not an equality. A key edge
    # asserts the two columns hold the SAME value; "<" or "within" makes a
    # column a join participant without making it equal, so an edge there would
    # over-claim. Counted so the declined volume is visible rather than silent.
    data_model_join_non_equality_predicates: int = 0
    data_model_join_key_pairs_read: int = 0
    # ``source.kind`` values seen across every Data Model /spec, with counts.
    # The only number that says which spec shapes this parser skipped, and the
    # first thing to read when a Data Model has less lineage than expected.
    data_model_spec_source_kinds: Dict[str, int] = field(default_factory=dict)
    # Elements whose source.kind is 'join' but whose predicate the parser could
    # not read. Non-zero is the signal that the shape assumption is wrong.
    data_model_join_elements_unreadable: int = 0
    # A join predicate named a column whose element or column is not part of
    # this run (filtered out by a pattern, or absent from /elements).
    data_model_join_key_partner_unresolved: int = 0
    # Elements where join predicates DID resolve into partner columns, but none
    # of the element's existing edges lands on a column a predicate names. The
    # last way join-key lineage can produce nothing, and the one a counter could
    # not otherwise distinguish from "no predicates were read at all".
    data_model_join_key_no_matching_edge: int = 0
    # Existing edges whose upstream is neither an element of this Data Model nor
    # a column any predicate names -- a warehouse table, or a foreign element
    # this model's joins never touch. Nothing to expand, but counted because the
    # skip used to be "not one of our elements", which also threw away joins
    # whose two sides BOTH live in other models.
    data_model_join_key_parent_outside_model: int = 0
    # An edge this connector built carries an upstream field URN that will not
    # parse. Non-zero means a defect in the edge builder, not in Sigma's data.
    data_model_join_key_upstream_urn_invalid: int = 0
    # Union output columns read out of /spec, before any of them is matched
    # against real columns. The denominator for the three union counters below.
    data_model_union_output_columns_read: int = 0
    # ``matches[].sourceColumns`` entries with no branch at the same index.
    # Non-zero means the positional alignment this parser assumes is wrong for
    # this tenant, which would silently pair columns across the wrong branches.
    data_model_union_branch_index_out_of_range: int = 0
    # Column edges added from a 'union' element's /spec: one per branch, for
    # every branch the output column's formula did not name. Without these a
    # union looks single-sourced no matter how many branches it stacks.
    data_model_element_fgl_union_resolved: int = 0
    # /spec named an output column that is absent from the union element's own
    # /columns response -- the two endpoints disagree, so no edge is emitted.
    data_model_union_output_column_absent: int = 0
    # A union branch names an element this Data Model does not contain. Unlike
    # a join side, a union source carries no dataModelId, so there is nothing
    # to pin a foreign element with and the branch is dropped rather than
    # guessed at.
    data_model_union_branch_element_unknown: int = 0
    # The branch element exists but has no column matching the name /spec gave,
    # by column id or by display name.
    data_model_union_branch_column_absent: int = 0
    # Predicate sides naming a warehouse table rather than an element in this
    # Data Model. /spec identifies those by connection and path and describes
    # none of their columns, so they cannot become element-to-element column
    # lineage. Expected, not a parse failure -- counted to keep the two apart.
    data_model_join_warehouse_side_predicates: int = 0
    # Column edges added because a join predicate equates the column an existing
    # edge points at with a column on the other side of the join. These are the
    # edges no formula can produce: a join's output column names only one side.
    # A formula ref naming the element's OWN name, where several elements in
    # the Data Model share that name. Sigma names a warehouse-sourced element
    # after its table, so sibling passthroughs all collide -- and resolving to
    # one of them fabricated a sibling edge (seen as a mutual A<->B cycle)
    # instead of the warehouse table /lineage reports. Counted because it says
    # how many edges the old precedence was getting wrong.
    data_model_element_fgl_self_named_siblings_skipped: int = 0
    data_model_element_fgl_join_key_resolved: int = 0
    # A single workbook element's lineage/query fetch raised. The element is
    # still emitted without upstreams; previously such an exception escaped to
    # the page handler and silently discarded every element on that page.
    workbook_element_lineage_fetch_failed: int = 0

    # --- Chart join-chain refs ([JoinElement/SourceElement/Column]) ---
    # A candidate split was validated against the resolved upstream's real
    # column list and accepted. Without this the first-slash reading names the
    # column "SourceElement/Column", which no upstream has, so the InputField
    # emitted is dangling rather than absent.
    chart_join_chain_resolved: int = 0
    # No split validated, so the legacy first-slash reading was kept. Compare
    # against chart_input_fields_multi_segment_ref to see the share still
    # falling through.
    chart_join_chain_unresolved: int = 0
    # A candidate resolved to a warehouse table, whose columns this connector
    # never learns, so the split could not be confirmed and was refused. A large
    # value means join-chain refs on the chart path point mostly at warehouse
    # tables and need a different validation source.
    chart_join_chain_upstream_schema_unavailable: int = 0
    # A join-chain ref where no candidate split validated. The legacy
    # first-slash reading WOULD have resolved, but only by naming the un-split
    # remainder ("SourceElement/Column") as the column -- a field the upstream
    # provably lacks. Emitting it produced a dangling InputField; the column now
    # falls back to a self-reference instead, matching how the Data Model path
    # treats an unknown upstream column.
    chart_join_chain_dangling_suppressed: int = 0
    # A join-chain ref where a SECOND candidate split also validated against a
    # real element and its schema. First-wins is deliberate -- candidates are
    # ordered most-specific-first, so an element genuinely named "A/B" beats an
    # unrelated "B" that happens to carry column C -- but everywhere else this
    # connector refuses on ambiguity, so a large value here means the ordering
    # is carrying more weight than it should and the rule needs revisiting.
    chart_join_chain_split_ambiguous: int = 0
    # Sub-count of data_model_element_fgl_warehouse_passthrough_deferred: the
    # element is named after its own warehouse table and the column's columnId
    # is not inode-shaped, so no pre-built warehouse edge existed. These are the
    # ones the by-name resolver then attempts; the remainder of the umbrella
    # counter is inode-shaped columnIds whose warehouse resolution failed, which
    # is a different problem with a different fix.
    data_model_element_fgl_self_named_no_passthrough: int = 0
    # Workbook lineage node types the connector does not handle, by type and
    # count. The warning is deduplicated per type, so this is the only place
    # the magnitude appears. 'union' is the one to watch: it combines inputs
    # the way 'join' does, so every element behind one loses its upstreams.
    workbook_lineage_node_types_unhandled: Dict[str, int] = field(default_factory=dict)
    # Combining nodes the BFS walked through, by type. Separate from the
    # unhandled map so a type moving from one to the other is visible.
    workbook_lineage_pass_through_nodes: Dict[str, int] = field(default_factory=dict)
    # Failed Sigma API calls by HTTP status (or exception class when there is
    # no response). Every ``except`` in the API client funnels into
    # ``_log_http_error``, which previously logged a context-free status code
    # and touched the report not at all -- so these failures were invisible
    # unless the run was in debug. On one tenant (2026-09): 11x404, 9x400, 6x409.
    api_call_failures_by_status: Dict[str, int] = field(default_factory=dict)
    # The same failures keyed by Sigma's own ``code``, which is what an operator
    # can actually act on. The status is too coarse: one tenant's nine 400s were
    # three unrelated problems needing three different fixes, and NONE of them
    # was a connector defect -- ``inode_archived`` (the model reads a warehouse
    # table or dataset that has been deleted), ``unable_to_produce_query`` and
    # ``invalid_request`` (the model is broken in Sigma, e.g. a dependency
    # cycle), ``warehouse_query_failed_user_error`` (a SQL error in the
    # customer's own model). Sigma cannot serve columns for any of them, so the
    # gap is closed by repairing or archiving the object, not by retrying.
    api_call_failures_by_sigma_code: Dict[str, int] = field(default_factory=dict)
    # Calls that ENUMERATE entities and failed, so entities are missing from
    # this run entirely. Reported as a failure, not a warning, because
    # stale-entity removal soft-deletes anything a previous run emitted and
    # this one did not -- a dead listing call makes live objects look deleted.
    # The framework skips soft-deletion when a source reports a failure; this
    # connector reported everything as a warning, so that guard never fired.
    entity_enumeration_failed: int = 0
    # Endpoints whose own ``total`` exceeded the rows pagination returned, and
    # by how many. Replaces a round-number guess that missed a 5,000 cap and
    # fired falsely on a tenant with exactly 10,000 rows.
    pagination_short_of_reported_total: Dict[str, int] = field(default_factory=dict)
    # Chart columns whose workbook's /columns fetch aborted, so no formula was
    # ever retrieved for them. Split out of chart_input_fields_self_ref_no_formula,
    # which otherwise reports a fetch failure as "Sigma has no formula" -- on one
    # tenant (2026-09) 12 workbooks aborted having retrieved ZERO entries, and
    # every column in them was attributed to the wrong cause.
    chart_input_fields_formulas_not_fetched: int = 0
    # Raw evidence for chart_input_fields_self_ref_no_formula, which is
    # otherwise a bare number and was the last silent bucket on the chart path.
    # "Sigma returned no formula for this column" and "this element got no
    # formulas at all while its workbook's /columns call SUCCEEDED" are very
    # different findings, and the counter cannot tell them apart -- so the
    # element's own formula coverage is recorded alongside each sample.
    chart_no_formula_samples: LossyList[str] = field(default_factory=LossyList)
    # Rows from GET /workbooks/{id}/columns that carried no elementId or no
    # name, so they could not be keyed and were dropped. A payload-shape change
    # would otherwise surface only as "Sigma has no formula" for whole elements.
    workbook_columns_rows_unkeyed: int = 0
    workbook_columns_unkeyed_samples: LossyList[str] = field(default_factory=LossyList)
    # Chart elements the /columns payload never mentioned, in workbooks whose
    # /columns call SUCCEEDED. This is the distinction that decides whether a
    # missing column lineage is Sigma's gap or ours: an element Sigma described
    # with null formulas has genuinely nothing to map, while one Sigma never
    # returned means the payload did not cover it and the fault is upstream of
    # our resolver entirely. Both used to land in the same counter.
    chart_elements_absent_from_columns_payload: int = 0
    # Elements with zero column formulas whose workbook fetched formulas fine.
    # A non-zero value here means the gap is per-ELEMENT, not per-workbook,
    # which no existing counter distinguishes.
    chart_elements_without_formulas_in_a_fetched_workbook: int = 0
    # Workbook /lineage element entries whose sourceIds name something other
    # than a customSQL node. The parser keeps only sourceIds it can match to a
    # customSQL name and drops the rest with no counter and no log; on one
    # tenant (2026-09) the payload carried 14,358 element entries against just
    # 84 customSQL nodes, so nearly all of that graph was discarded silently.
    # Bucketed by what the dropped id turned out to be, which is the only way
    # to tell "chart sourced from another chart" (worth consuming) from a node
    # type we genuinely have no use for.
    workbook_lineage_element_source_ids_dropped: int = 0
    # Upstream elements recovered from the workbook lineage graph that the
    # element's own upstream_sources did not already declare. These widen what
    # a chart formula ref is allowed to resolve against, and they are STATED by
    # Sigma rather than guessed -- the distinction that got name-based matching
    # removed. Watch against chart_ref_miss_reasons
    # ["element_named_but_not_a_lineage_upstream"], which is what they attack.
    chart_upstreams_added_from_lineage_graph: int = 0
    chart_lineage_graph_upstream_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    workbook_lineage_dropped_source_id_kinds: Dict[str, int] = field(
        default_factory=dict
    )
    workbook_lineage_dropped_source_id_samples: Dict[str, LossyList[str]] = field(
        default_factory=dict
    )
    # ------------------------------------------------------------------
    # Chart-GRANULARITY outcomes.
    #
    # Every other counter here is per COLUMN, and a customer reports a
    # PROBLEM PER CHART ("this chart has no column lineage"). Answering that
    # from per-column counters is not possible: 437,000 resolved columns say
    # nothing about whether one particular chart got zero. On 2026-09-11 three
    # reported chart URNs could not be explained from a 105MB log for exactly
    # this reason -- the run knew the answer and had no place to put it.
    #
    # ``no_column_lineage`` counts charts where EVERY column fell back to a
    # self-referential URN, which is what renders as "lineage at the chart
    # level only". ``partial`` counts charts where some columns resolved and
    # others did not -- invisible in both the per-column totals and in
    # no_column_lineage.
    # Independent total for the identity check below: every chart classified
    # must land in exactly one of the four buckets.
    charts_classified_total: int = 0
    charts_with_column_lineage: int = 0
    charts_with_partial_column_lineage: int = 0
    charts_with_no_column_lineage: int = 0
    # Charts Sigma described with no columns at all. Emitted as an empty
    # InputFields aspect, so indistinguishable in the UI from a chart whose
    # every column failed -- and previously counted as fully resolved.
    charts_with_no_columns: int = 0
    # InputFields is a FULL-REPLACE aspect and Sigma element ids are not unique
    # across workbooks, so several workbooks' charts collide on one chart URN
    # and the last one processed wins. On one tenant (2026-09) 23% of chart
    # element ids were claimed by more than one workbook -- one by eight -- and
    # 303 of those had a copy inside a workbook whose /columns call aborted.
    # The observed result was a chart resolving 59 of 66 columns and then being
    # overwritten minutes later by an empty aspect from a broken duplicate.
    # These count emissions refused because a richer aspect was already sent.
    chart_input_fields_regressive_emission_skipped: int = 0
    chart_regressive_emission_samples: LossyList[str] = field(default_factory=LossyList)
    # Chart URNs claimed by more than one workbook, and the worst case seen.
    # The skip above prevents the DATA LOSS; it does not make the URNs correct,
    # because two genuinely different charts still share one entity.
    chart_urns_claimed_by_multiple_workbooks: int = 0
    # Derived columns that took their upstreams from the sibling columns they
    # are computed from (Sum([Revenue (1)]), DateLookback([Revenue], ...)).
    # Largest chart-side gap measured on a real tenant: 27,037 columns, with
    # 5,246 charts losing columns for this reason alone. Counted as resolved
    # once inherited, so the per-element identity still holds.
    chart_input_fields_sibling_inherited: int = 0
    # Chart columns resolved straight from their own columnId
    # (inode-<tableUrlId>/<NATIVE_NAME>) with no formula involved. The Data
    # Model path has exploited this fact for weeks; the chart path never did,
    # leaving 2,167 columns on one tenant falling back to a self-reference
    # while carrying their own answer.
    chart_input_fields_warehouse_by_column_id: int = 0
    # Edges produced by the opt-in name-matching guess
    # (resolve_chart_refs_by_element_name). Counted separately and never folded
    # into the resolved total's sub-counts, because these are the only chart
    # edges in the connector that Sigma did not state.
    chart_ref_resolved_by_element_name_guess: int = 0
    chart_sibling_inherited_samples: LossyList[str] = field(default_factory=LossyList)
    # Sibling columns whose siblings resolved to nothing either -- a chart whose
    # raw columns have no lineage cannot give its derived columns any.
    chart_input_fields_sibling_not_inheritable: int = 0
    # customSQL charts get a SECOND InputFields aspect at drain time which
    # supersedes the one the element loop emitted, so the chart-level buckets
    # above do not describe their final state. These do. Keep them separate
    # rather than folding them in: the two are measured at different points and
    # silently merging them would make neither trustworthy.
    customsql_charts_final_with_column_lineage: int = 0
    customsql_charts_final_no_column_lineage: int = 0
    charts_with_no_columns_samples: LossyList[str] = field(default_factory=LossyList)
    # no_column_lineage split by the cause that dominated the chart's columns,
    # using the same vocabulary as the per-column counters (no_formula,
    # formulas_not_fetched, unresolved_refs, parameter, sibling, mixed). A
    # chart is filed under whichever cause claimed the most of its columns.
    charts_with_no_column_lineage_by_cause: Dict[str, int] = field(default_factory=dict)
    # One sample list PER CAUSE, never one shared list. A shared LossyList is
    # reservoir sampling, so it is proportional BY DESIGN and the rare bucket
    # -- always the interesting one -- can never be evidenced. That exact bug
    # returned zero samples for a 263-column population in an earlier run.
    charts_with_no_column_lineage_samples: Dict[str, LossyList[str]] = field(
        default_factory=dict
    )
    # Why chart formula refs failed to resolve, by cause. The aggregate
    # (chart_input_fields_self_ref_unresolved_refs) reached 17,944 on one tenant
    # while concentrating in just 87 distinct source names, so the bucket is a
    # few causes rather than thousands of distinct problems. Two synthetic keys
    # split the "unknown source" case by whether the name exists elsewhere in
    # the run: it does => our lookup scope is too narrow; it does not => the run
    # never saw that element (filtered, 409-ing, or outside the ingest).
    chart_ref_miss_reasons: Dict[str, int] = field(default_factory=dict)
    # Columns counted as "a real ref failed" that contributed NOTHING to the
    # breakdown above. This is the breakdown auditing itself: while it reads 0,
    # chart_ref_miss_reasons accounts for every unresolved column and can be
    # used as evidence about where the remaining gaps come from. Non-zero means
    # some resolution path returns without recording a reason, and the residual
    # is unattributable.
    #
    # It exists because that happened twice, and both times the counters that
    # DID fire looked healthy -- the gap was only visible as a constant 234
    # across two full runs whose totals differed. Reading it off one line beats
    # reconstructing it from arithmetic across two 100MB logs.
    chart_input_fields_unattributed: int = 0
    # Raw evidence for the above: the element, column, formula, refs and their
    # segment counts. Deliberately not summarised -- the cause is by definition
    # a path nobody anticipated, so a pre-chosen category would be the wrong
    # one. LossyList caps it, and the counter carries the true total.
    chart_ref_unattributed_samples: LossyList[str] = field(default_factory=LossyList)
    # The counters checking themselves at the end of the run. ``reconciles: 1``
    # and nothing else means both identities hold: every chart column lands in
    # exactly one bucket, and every unresolved column has a named cause. Any
    # other key is a residual that nobody has explained, with the size of the
    # discrepancy as its value.
    #
    # Both identities were originally worked out by hand across two 100MB logs.
    # Checking them in-run is the difference between noticing a gap in the next
    # report and noticing it two runs later, if at all.
    chart_column_accounting_check: Dict[str, int] = field(default_factory=dict)
    # Measurement only -- nothing below changes what is emitted.
    #
    # /workbooks/{id}/columns gives a formula as the string a user typed
    # ("[Some Element/Order Number]"), so resolving it means matching a display
    # NAME. /workbooks/{id}/schema gives the same formula parsed, with refs as
    # IDs. These count how many currently-unresolved chart columns that endpoint
    # would explain, so a resolver built on it can be justified before it is
    # written rather than measured after. The last name-based path was removed
    # having produced 1,106 edges -- a number only knowable in hindsight.
    #
    # cross_sheet is the interesting one: a stated dependency on another sheet's
    # column, by id, which is exactly what the name matcher was guessing at.
    chart_ref_schema_cross_sheet_resolvable: int = 0
    # The formula refers to a warehouse column by inode. Some of these already
    # resolve through the warehouse path; overlap is expected.
    chart_ref_schema_warehouse_resolvable: int = 0
    # Was a single "sibling_only" bucket. It was an else-branch, so it absorbed
    # every path shape this reader did not recognise and reported them as though
    # Sigma had said the column was local -- the opposite of what most of them
    # mean. Measured on a dev tenant, 45% of that bucket was misfiled. The four
    # below are mutually exclusive and replace it.
    #
    # A path of length 1: a bare column in the same sheet. The ONLY shape that
    # genuinely names no upstream.
    chart_ref_schema_local_only: int = 0
    # Head is "<dataModelId>/<elementId>" -- a Data Model element named by id,
    # which maps onto a Sigma dataset URN this connector already emits.
    chart_ref_schema_dm_element: int = 0
    # Head is an id in no structure /schema returns (not a sheet, element,
    # inode, or compound). Unidentified; the samples carry the raw head so the
    # next run can say what it is rather than discarding it silently.
    chart_ref_schema_unknown_head: int = 0
    # A path of 3+ segments, Sigma's join-chain shape.
    chart_ref_schema_join_chain: int = 0
    # Head is a 10-char element id present in /schema's own "elements" map. Only
    # "sheets" was ever consulted, so cross-element refs could not resolve.
    chart_ref_schema_element_resolvable: int = 0
    # Raw heads for chart_ref_schema_unknown_head, to identify the id space.
    chart_ref_schema_unknown_head_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # The raw head is opaque, so the counter and a sample id cannot say what the
    # id space IS. These are the properties that can be read off the response
    # without knowing it: the head's length (10 and 22 are different spaces),
    # the containing sheet's type, whether the ref points back at the column's
    # own id (a pass-through of a source column), how many columns share the
    # head (a shared head is a real upstream object, not a per-column quirk),
    # and whether the document defines the head anywhere or only cites it.
    # On a dev tenant every occurrence was a citation and nothing was defined,
    # but that tenant holds 3 data models against a customer's 891, so absence
    # there is not evidence about the id space in general.
    chart_ref_schema_unknown_head_kinds: Dict[str, int] = field(default_factory=dict)
    # Distinct heads behind those refs. On a dev tenant 24 refs resolved to 4
    # heads, so the ref count on its own overstates how many distinct objects
    # are unaccounted for -- and a handful of heavily shared heads is a very
    # different problem from thousands of one-off ones.
    chart_ref_schema_unknown_head_distinct: int = 0

    # --- GET /workbooks/{id}/sources, measurement only -----------------------
    # Sigma states which Data Model elements and warehouse tables a workbook
    # consumes. The connector reconstructs that today from per-element
    # /lineage plus name matching, which is where most unresolved chart
    # columns come from -- so the question is how much of the gap a DECLARED
    # dependency closes. Measured before building anything on it, the same way
    # /schema was, because the last resolver built on an estimate produced
    # 1,106 edges and was deleted.
    workbook_sources_fetch_failed: int = 0
    workbook_sources_workbooks_read: int = 0
    # The "type" vocabulary, which is undocumented. Reading it off a real
    # tenant is the only way to know which forms a resolver must handle --
    # the observed values are "table" and "data-model", and the hyphen is a
    # reminder that this was guessed wrong before it was measured. Nothing
    # branches on the string (the entry shape is keyed on dataModelId /
    # inodeId), precisely so an unseen value cannot silently drop an entry.
    workbook_sources_entry_types: Dict[str, int] = field(default_factory=dict)
    workbook_sources_data_model_entries: int = 0
    workbook_sources_warehouse_entries: int = 0
    # Data Model elements sources DECLARES that the workbook's own /lineage
    # never mentioned. This is the recovered set: if it is ~0 then sources
    # says nothing /lineage did not, and there is nothing here to build on.
    workbook_sources_dm_elements_declared: int = 0
    workbook_sources_dm_elements_new_vs_lineage: int = 0
    # Unresolved chart columns whose referenced column is owned by a Data
    # Model element that sources declares for this workbook -- the columns
    # this endpoint would actually explain, which is the number that decides
    # whether a resolver is worth building.
    chart_ref_sources_explains: int = 0
    chart_ref_sources_owner_not_declared: int = 0
    chart_ref_sources_column_owner_unknown: int = 0
    # Split by the cause the column was already filed under, so a single total
    # cannot be read as closing whichever gap the reader hoped for.
    chart_ref_sources_explains_by_reason: Dict[str, int] = field(default_factory=dict)
    chart_ref_sources_outcomes_by_reason: Dict[str, int] = field(default_factory=dict)
    chart_ref_sources_samples: LossyList[str] = field(default_factory=LossyList)
    # The endpoint has the column but its formula contains no reference at all.
    chart_ref_schema_no_refs: int = 0
    # /schema has no entry for that columnId -- the two endpoints disagree about
    # what the workbook contains, which is itself worth knowing.
    chart_ref_schema_column_absent: int = 0
    # The call failed, so these columns are unmeasured rather than unresolvable.
    chart_ref_schema_unavailable: int = 0
    workbook_schema_fetch_failed: int = 0
    # The cross-sheet count above, split by the cause the column was already
    # filed under. A single total is consistent with closing the 6,679
    # "element named but not a lineage upstream" refs, or the 1,065 whose name
    # exists in a loaded Data Model, or a slice of each -- and those are
    # different decisions about whether to build a resolver on this endpoint.
    chart_ref_schema_resolvable_by_reason: Dict[str, int] = field(default_factory=dict)
    # EVERY outcome by original cause, keyed "<outcome>::<reason>", not just the
    # resolvable one. If /schema covers less than hoped, the useful question
    # immediately becomes what it holds for those columns INSTEAD, and
    # "cross_sheet: small" cannot answer that. Reading both off the same run is
    # the difference between one run and three.
    chart_ref_schema_outcomes_by_reason: Dict[str, int] = field(default_factory=dict)
    # The nameRef paths /schema holds, sampled PER OUTCOME. The shape is the
    # evidence: a path of unexpected length or head means this reader is wrong,
    # no paths at all means the endpoint genuinely has nothing for that column,
    # and the two call for opposite responses.
    #
    # One shared LossyList was wrong, and produced a concrete failure: it
    # reservoir-samples 10 entries across every outcome, so a minority outcome
    # is starved in proportion to its share. `join_chain` was 263 of ~16,236
    # unresolvable columns (1.6%), giving an expected yield of 0.16 samples --
    # and it returned ZERO on a full customer run, leaving the one gap that is
    # entirely ours with a count and no evidence. Keyed by outcome, each gets
    # its own budget regardless of how rare it is.
    chart_ref_schema_samples_by_outcome: Dict[str, LossyList[str]] = field(
        default_factory=dict
    )
    # Sheets bucketed by how many elements render them, from
    # elements[<id>].viz.sheetId. A /schema cross-sheet ref names the sheet, but
    # an edge must point at a chart, which is emitted per element -- so "2+"
    # means there is no single correct target and a guess would attach correct
    # lineage to the wrong chart. "1" means the ref resolves outright. Our dev
    # tenant is 1:1 across all 19 sheets; this measures whether that holds on a
    # real tenant before a resolver is built on it.
    chart_ref_schema_sheet_element_fanout: Dict[str, int] = field(default_factory=dict)
    # The same unknown heads re-tested at END of run, once every id space is
    # complete. `space=` is decided when a head is first met, and one space it
    # tests against is accumulated as workbooks are walked -- so a head owned by
    # a workbook processed later misses for reasons of ORDER, not absence. Any
    # non-"still_unidentified" entry here is a head the first pass misreported.
    # `_heads` counts distinct ids; `_columns` counts the columns citing them,
    # which is the number that says how much lineage is at stake.
    chart_ref_schema_unknown_head_recheck_heads: Dict[str, int] = field(
        default_factory=dict
    )
    chart_ref_schema_unknown_head_recheck_columns: Dict[str, int] = field(
        default_factory=dict
    )

    # Refusals from the ID-based cross-sheet resolver. Each is a case where the
    # chain was not exact, and the resolver declines rather than guess: a wrong
    # pick attaches correct lineage to the WRONG chart.
    # Several elements render the referenced sheet, so there is no single target.
    chart_ref_schema_cross_sheet_sheet_ambiguous: int = 0
    # path[1] is not a column of the element that renders that sheet.
    chart_ref_schema_cross_sheet_column_unknown: int = 0
    # The element exists but was filtered from chart emission, so it has no URN.
    chart_ref_schema_cross_sheet_no_chart_urn: int = 0
    # Columns whose lineage was recovered from /schema by ID, replacing a
    # self-referential InputField the name-based path could not resolve.
    chart_input_fields_recovered_from_schema: int = 0
    # The ID path (/schema) now gets first refusal and the name path is the
    # fallback. These audit that decision on every column, because it was
    # justified by 609 comparisons on a 7-workbook tenant and now governs
    # ~437,000 on the customer's.
    #   agrees     -- both resolved, same URN. Expected to dominate.
    #   disagrees  -- both resolved, different URN; the ID answer wins and the
    #                 pair is sampled. A non-trivial number here means one of
    #                 the two paths is wrong and the samples say which.
    #   no_id_path -- /schema was silent, so the name path stands. Sizes how
    #                 much the fallback still carries; /schema is NOT a
    #                 superset, since some columns have no nameRef at all.
    chart_ref_schema_agrees_with_name_path: int = 0
    chart_ref_schema_disagrees_with_name_path: int = 0
    chart_ref_schema_no_id_path: int = 0
    #   not_described -- /schema has no entry for the column at all (a
    #                 constant, an aggregate, or simply omitted). Separate from
    #                 no_id_path on purpose: "described but unresolvable" and
    #                 "not described" call for opposite responses, and pooling
    #                 them repeats the catch-all else-branch that once reported
    #                 unrecognised shapes as "the column is local".
    chart_ref_schema_column_not_described: int = 0
    chart_ref_schema_disagreement_samples: LossyList[str] = field(
        default_factory=LossyList
    )
    # How a <dmUrlId>/<elementId> head breaks down. The largest
    # resolvable-looking outcome and the least understood: path[1] is a DISPLAY
    # NAME in the customer samples and an opaque COLUMN ID on our dev tenant,
    # so a handler has to try both and the mix decides how it is written. Also
    # says how often the element is not in the run at all -- true for BOTH dev
    # heads, which is why this shape cannot be cross-validated there.
    chart_ref_schema_dm_element_shape: Dict[str, int] = field(default_factory=dict)
    # Splits unknown_source_but_name_exists_in_a_data_model_this_workbook_loads
    # by whether a same-named element actually OWNS the referenced column:
    # exactly one owner means the ref is real and the candidate list was too
    # narrow; several means the name is ambiguous and cannot be resolved by
    # name at all; none means the name match is a coincidence. The single
    # counter cannot tell them apart, and they need different responses.
    chart_ref_name_in_loaded_dm_outcomes: Dict[str, int] = field(default_factory=dict)
    # 'datasheet' nodes whose nodeId is a bare element id, admitted as sheet
    # upstreams. The emit-time element lookup drops any that do not match a
    # real element, so this is an attempt count, not an emitted-edge count.
    workbook_lineage_datasheet_as_sheet: int = 0
    # 'datasheet' nodes whose nodeId is an inode. The node carries no name and
    # the lineage endpoint gives nothing else, so resolving these needs a
    # /files lookup the walk does not do.
    workbook_lineage_datasheet_inode_unresolved: int = 0
    # 'datafile' nodes: an uploaded file with no dataset on any platform behind
    # it. A genuine leaf -- counted so it is not mistaken for a gap.
    workbook_lineage_datafile_leaf: int = 0
    # 'semantic-view' nodes. They name a warehouse table but carry no
    # connectionId, so there is nothing to choose a platform with.
    workbook_lineage_semantic_view_unresolved: int = 0
    # Sub-count of chart_input_fields_warehouse_column_bridge_unresolved where
    # the native-name map DOES hold the column under different casing. That is
    # a normalisation bug, not missing data, and needs a different fix from a
    # genuine absence -- which is why the two are separated.
    chart_input_fields_bridge_case_only_miss: int = 0
    # Workspace id could not be resolved from a file path. Previously only a
    # logger.error, so it never reached the ingestion report at all.
    workspace_id_lookup_failed: int = 0
    # /workbooks/{id}/lineage non-200 responses, by HTTP status. The warning is
    # capped by LossyList, so this is the only place the distribution survives.
    workbook_lineage_non_200_by_status: Dict[str, int] = field(default_factory=dict)
    # Layout elements (control, divider, ...) returned alongside data elements.
    # They have no name and are correctly skipped; counted separately so
    # pagination_malformed_entries_dropped keeps meaning "a real entry failed
    # to parse".
    non_data_elements_skipped: Dict[str, int] = field(default_factory=dict)
    # A join predicate side naming an element in ANOTHER Data Model. Sigma
    # allows a model to join in an element it does not own, and one tenant had
    # a single shared mapping element on 9 of 10 otherwise-unresolvable
    # predicates, joined into nine models. Element ids repeat across models, so
    # the model is pinned by the side's dataModelId when Sigma sends one, else
    # by the models this one sources from.
    data_model_join_key_foreign_resolved: int = 0
    # No Data Model could be pinned for the foreign element.
    data_model_join_key_foreign_dm_unknown: int = 0
    # Several candidate models define that element id; refused, not guessed.
    data_model_join_key_foreign_ambiguous: int = 0
    # Model pinned, but it has no column by that name. A large value means the
    # wrong model is being pinned.
    data_model_join_key_foreign_column_absent: int = 0
    # A join predicate named a column this element references, but the join it
    # came from is not in the element's upstream closure -- two elements can
    # read the same key column while only one flows through the join. Expanding
    # the other would assert an equality its data path never applies, so the
    # predicate is skipped. A large value beside a small
    # data_model_element_fgl_join_key_resolved means the scoping is doing most
    # of the work; a large value with resolved at 0 means it is too tight.
    data_model_join_key_out_of_join_path: int = 0
    # Join-chain refs resolved by looking the middle segment up among the
    # siblings of the Data Model the FIRST segment resolved into, rather than
    # among the chart's own upstreams. A chart declares only the join element;
    # the table joined into it is that element's sibling and is invisible from
    # the chart side, which is why the plain candidate search cannot see it.
    # A subset of chart_join_chain_resolved, not an addition to it.
    chart_join_chain_sibling_resolved: int = 0
    # The first segment resolved, but to a chart or warehouse table rather than
    # a Data Model element, so there were no siblings to search.
    chart_join_chain_sibling_dm_unknown: int = 0
    # Two or more sibling elements in the Data Model share the middle segment's
    # name. Refused rather than guessed.
    chart_join_chain_sibling_ambiguous: int = 0
    # A uniquely-named sibling matched but does not have the referenced column.
    # A large value means this reading of the ref shape is wrong.
    chart_join_chain_sibling_column_absent: int = 0
    # Column whose formula refs are exclusively parameter refs (e.g. [P_*]).
    chart_input_fields_skipped_parameter: int = 0
    # Column whose formula refs are exclusively bare sibling refs (e.g. [col]).
    chart_input_fields_skipped_sibling: int = 0
    # Column mixing the two above and containing nothing else. Neither kind can
    # ever name an upstream and neither records a miss reason, so these used to
    # land in chart_input_fields_self_ref_unresolved_refs with no entry at all in
    # chart_ref_miss_reasons -- inflating the one bucket that is meant to mean "a
    # real ref failed to resolve, look here for a defect". Kept out of the two
    # pure counters above so each keeps answering its own question.
    chart_input_fields_skipped_param_and_sibling: int = 0
    # Extra InputFields emitted for columns whose formula resolves to more than
    # one distinct (upstream_urn, upstream_field) pair. The first resolved pair
    # is counted in chart_input_fields_resolved; each additional pair increments
    # this counter. Non-zero means some chart columns have multi-upstream lineage.
    chart_input_fields_multi_ref_extra: int = 0
    # Sub-bucket of self_ref_fallback: source name that is a case-only mismatch
    # against a workbook element name (warehouse fallback intentionally skipped).
    chart_input_fields_case_mismatch: int = 0
    # Workbooks whose /columns pagination aborted partway through. InputFields
    # for those workbooks may be missing columns that appear after the failure.
    column_formulas_fetch_partial: int = 0

    # Workbook-lineage warehouse table index for chart formula resolution.
    # A chart's inputFields[].schemaFieldUrn was resolved against a warehouse
    # table entry in the merged (per-element + per-workbook) index. Sub-category
    # of chart_input_fields_resolved.
    chart_input_fields_warehouse_qualified: int = 0
    # Resolved specifically by an entry from the workbook-level index (not the
    # per-element SQL-parser index) — quantifies the added coverage beyond what
    # the SQL parser resolves per element.
    chart_input_fields_warehouse_qualified_via_workbook_index: int = 0
    # /v2/workbooks/{id}/lineage returned None (5xx / network error). 404 is
    # handled silently (workbook deleted since listing).
    chart_input_fields_warehouse_index_lookup_failed: int = 0
    # /v2/files/{inodeId} returned None for a workbook-lineage type=table inode.
    # Only incremented on first failure per inode — the /files cache is shared
    # with the DM element path.
    chart_input_fields_warehouse_table_lookup_failed: int = 0
    # /files path didn't parse as Connection Root/<SCHEMA> (Redshift) or Connection Root/<DB>/<SCHEMA> (Snowflake/Postgres).
    # Only incremented on first occurrence per inode — _files_path_unparseable_seen
    # is shared with the DM element path.
    chart_input_fields_warehouse_path_unparseable: int = 0
    # connectionId not in registry, or is_mappable=False, for a workbook-lineage
    # type=table entry.
    chart_input_fields_warehouse_unknown_connection: int = 0

    # Chart entity-level warehouse upstream — direct BFS type=table edges resolved to warehouse Dataset URNs via the workbook table index.
    chart_warehouse_upstream_emitted: int = 0
    chart_warehouse_table_name_unmatched: int = 0
    chart_warehouse_table_node_skipped: int = 0
    chart_warehouse_table_name_ambiguous: int = 0
    # Column-name bridge: Sigma display name -> warehouse-native name.
    chart_input_fields_warehouse_column_bridged: int = 0
    # Warehouse upstream resolved but no native name found; fell back to display name.
    chart_input_fields_warehouse_column_bridge_unresolved: int = 0
    # Two warehouse upstreams on the same element exposed the same display name
    # with different native names; first-written value is kept.
    chart_input_fields_column_native_names_collision: int = 0
    # Two DataModelElementUpstream entries on the same element share a display name
    # mapping to different DM URNs; first-resolved value is kept.
    chart_input_fields_dm_upstream_name_collision: int = 0

    # DM element emission / upstream resolution.
    data_model_elements_emitted: int = 0
    data_model_element_intra_upstreams: int = 0
    data_model_element_external_upstreams: int = 0
    # Split intentionally so operators can triage "upstream dataset
    # exists but wasn't ingested in this run" (typically a pattern
    # filter or missing read perm) vs "source_id shape we do not yet
    # parse" (cross-DM refs ahead of the follow-up PR, or a future
    # Sigma shape). ``data_model_element_upstreams_unresolved`` is
    # kept as an aggregate for dashboards that already read it.
    data_model_element_upstreams_unresolved_external: int = 0
    data_model_element_upstreams_unknown_shape: int = 0
    # Blank entries in an element's ``source_ids``. Split out of
    # ``unknown_shape``, which implies a shape this parser fails to recognise
    # and so invites a hunt for a missing branch -- there is no shape here at
    # all. On one tenant (2026-09) every "unknown shape" was this.
    data_model_element_upstreams_empty_source_id: int = 0
    data_model_element_upstreams_unresolved: int = 0

    # Cross-DM element references (DM-A element pulls from DM-B). Success
    # counters bump once per unique upstream URN (diamonds deduped).
    # ``_resolved = _strict + _ambiguous + _single_element_fallback``
    # (``_ambiguous`` and ``_single_element_fallback`` are sub-shapes of
    # ``_resolved``; clean strict-name-match = _resolved - _ambiguous -
    # _single_element_fallback).
    # Failure counters bump per source_id (each is a distinct missing ref)
    # and also bump ``data_model_element_upstreams_unresolved``:
    # ``_name_unmatched_but_dm_known``: producer DM ingested, no name match
    #   (typically a consumer-side rename).
    # ``_dm_unknown``: producer DM not in this run.
    # ``_consumer_name_missing``: consumer element had a blank name; the
    #   name-bridge was never attempted.
    # ``_self_reference``: producer prefix matches the consuming DM (API
    #   payload anomaly; defensively skipped).
    data_model_element_cross_dm_upstreams_resolved: int = 0
    data_model_element_cross_dm_upstreams_ambiguous: int = 0
    data_model_element_cross_dm_upstreams_single_element_fallback: int = 0
    data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known: int = 0
    data_model_element_cross_dm_upstreams_dm_unknown: int = 0
    data_model_element_cross_dm_upstreams_consumer_name_missing: int = 0
    data_model_element_cross_dm_upstreams_self_reference: int = 0

    # Personal-space / unlisted DMs discovered via /v2/dataModels/{urlId}.
    # ``_discovered``: an unlisted DM was fetched and added to the run.
    # ``_unresolved``: fetch returned non-200 (usually 403 / 404).
    # ``_rate_limited``: fetch returned 429 after the urllib3 retry budget
    #   was exhausted -- sub-count of ``_unresolved`` surfaced separately
    #   so operators can distinguish "Sigma is rate-limiting us" (transient,
    #   re-run the job) from "the DM is genuinely forbidden or deleted"
    #   (steady-state). ``_unresolved`` is still bumped for 429 so the
    #   aggregate stays accurate.
    data_model_external_references_discovered: int = 0
    data_model_external_reference_unresolved: int = 0
    data_model_external_reference_rate_limited: int = 0

    # /columns entries with ``elementId = None`` (DM-global calculations),
    # dropped because there is no element Dataset to attach them to.
    data_model_columns_without_element_dropped: int = 0

    # Two DMs claimed the same ``urlId`` bridge key. The first wins; the
    # second is skipped at emit time to avoid an unlinked orphan. Non-zero
    # means a reissued slug; see warning log for ``(dataModelId, urlId)``.
    data_models_bridge_key_collision: int = 0

    # Duplicate ``column.name`` on a single DM element, dropped to avoid
    # ``SchemaMetadata`` with duplicate ``fieldPath`` values.
    data_model_element_columns_duplicate_fieldpath_dropped: int = 0

    # DM element column-level lineage (FGL) counters.
    # Throughout: "DM" / "dm" = data model.
    # Intra-DM FGL only; total FGL = fgl_emitted + fgl_cross_dm_resolved.
    data_model_element_fgl_emitted: int = 0
    # The DM side checking itself, mirroring chart_column_accounting_check.
    # fgl_emitted counts FGL OBJECTS while the path counters count resolutions,
    # so those two can never be reconciled directly -- the identity that IS
    # checkable is per COLUMN: every Data Model column either produced lineage
    # or recorded a reason it did not.
    dm_columns_total: int = 0
    dm_columns_with_lineage: int = 0
    dm_columns_without_lineage: int = 0
    dm_columns_without_lineage_by_reason: Dict[str, int] = field(default_factory=dict)
    # The number that matters: columns that produced nothing AND explained
    # nothing. While it reads 0, the breakdown accounts for every silent column
    # and can be used as evidence about where DM lineage is lost.
    dm_columns_without_lineage_unattributed: int = 0
    dm_unattributed_column_samples: LossyList[str] = field(default_factory=LossyList)
    dm_column_accounting_check: Dict[str, int] = field(default_factory=dict)
    # Refs where multiple sibling candidates passed the /lineage filter;
    # sorted-first URN was chosen (matches collision precedent).
    data_model_element_fgl_collision_pick_first: int = 0
    # Refs whose source element is outside this DM; deferred to cross-DM resolution.
    data_model_element_fgl_cross_dm_deferred: int = 0
    # Warehouse-passthrough refs where columnId-based resolution failed.
    # Fires for self-ref formulas (element name == formula source) AND for
    # formulas where the source is the warehouse table name but resolution fails
    # (e.g. inode-shaped columnId with /files miss or unmappable connection).
    # Note: semantics changed in this release — previously this counter fired on
    # every self-ref (a throughput signal); now it fires only on failure.
    data_model_element_fgl_warehouse_passthrough_deferred: int = 0
    # Warehouse-passthrough FGL emitted via columnId (inode-<url_id>/<COL>).
    # Covers both the self-ref case and the case where the formula source is
    # the warehouse table name rather than the element name.
    data_model_element_fgl_warehouse_resolved: int = 0
    # Refs whose source element is in this DM but not listed as an upstream by
    # /lineage, and whose cross-DM rescue (_try_emit_self_named_cross_dm_fgl)
    # also found no match; dropped to avoid orphan FGL the UI silently rejects.
    data_model_element_fgl_dropped_orphan_upstream: int = 0
    # Refs whose column name has no matching fieldPath in the upstream element's
    # schema; dropped to avoid a dangling schemaField URN.
    data_model_element_fgl_dropped_unknown_upstream_column: int = 0
    # Refs whose intra-DM sibling resolved but carries no columns at all (look
    # for a matching "Sigma paginated endpoint aborted" warning naming this DM).
    # Split out of dropped_unknown_upstream_column so a fetch problem is
    # distinguishable from a genuine column-name mismatch. A whole-DM /columns
    # failure does not land here — that empties the consuming element too, so it
    # has no columns to resolve; this fires on a partial pagination abort or a
    # genuinely column-less sibling.
    data_model_element_fgl_upstream_schema_unavailable: int = 0
    # Columns from which no bracket ref could be resolved — an empty/absent
    # formula (Sigma's shape for a pass-through column), a constant expression,
    # or a formula whose only refs are parameters (`[P_*]`) or bare sibling-column
    # refs — and whose columnId is not warehouse-shaped, so there was nothing to
    # resolve against. Dominated by intra-DM and Sigma Dataset passthroughs;
    # expected volume, not a failure signal.
    data_model_element_fgl_no_ref_unresolved: int = 0
    # Same no-resolvable-ref shape, but the columnId IS ``inode-``-shaped, so a
    # warehouse column was identified and resolution still failed (a /files miss
    # or an unmappable connection). This one is actionable. Kept apart from
    # fgl_warehouse_passthrough_deferred, which stays reachable only from
    # formulas that do produce refs, so its baseline remains comparable.
    data_model_element_fgl_no_ref_warehouse_unresolved: int = 0
    # Multi-segment ("join chain") refs of the shape
    # [JoinElement/SourceElement/Column] -- Sigma's encoding for a column reached
    # through a join -- resolved by trying alternative (source, column) splits
    # instead of the legacy first-slash split. Only counted for refs with 3+
    # segments, so single-slash refs never inflate it.
    data_model_element_fgl_join_chain_resolved: int = 0
    # Join-chain refs where no candidate split validated against a real element
    # name plus that element's schema. A SUB-COUNT of whichever residual bucket
    # the legacy path then settles on (dropped_unknown_upstream_column,
    # dropped_orphan_upstream or cross_dm_deferred) -- not an independent drop,
    # so do not add it to those totals.
    data_model_element_fgl_join_chain_unresolved: int = 0
    # Join-chain sources promoted to entity-level upstreams because Sigma's
    # element /lineage lists only the direct join element, never the transitive
    # source the formula actually names. Without the promotion the emitted
    # schemaField would reference a Dataset absent from ``upstreams``.
    data_model_element_fgl_join_chain_upstream_added: int = 0
    # Refs whose named sibling was absent from Sigma's /lineage upstreams but
    # demonstrably owns the referenced column. Accepted on the strength of the
    # schema and promoted to an entity-level upstream, the same treatment join
    # chains get. Replaces the earlier _orphan_recoverable_if_gate_relaxed
    # probe, which measured 162 of 167 such drops before the change was made.
    data_model_element_fgl_orphan_recovered: int = 0
    # Cross-DM FGL counters (DM = data model throughout).
    # Refs resolved via global bridge index and emitted as cross-DM FGL.
    # Resolution uses entity-level upstreams as a soft collision tiebreaker,
    # not a hard gate — a resolved entry does not imply entity-level confirmation.
    data_model_element_fgl_cross_dm_resolved: int = 0
    # Refs where multiple cross-DM candidates share a name; sorted-first URN chosen.
    data_model_element_fgl_cross_dm_collision_pick_first: int = 0
    # Cross-DM refs whose column is absent from the resolved upstream element's schema.
    data_model_element_fgl_cross_dm_dropped_unknown_upstream_column: int = 0
    # Cross-DM equivalent of fgl_upstream_schema_unavailable: the producer
    # element is in the bridge map but carries no columns, so the producer DM's
    # /columns fetch came back empty. Kept apart from the intra-DM counter
    # because an empty sibling and an empty cross-DM producer point at different
    # data models to investigate. A producer missing from the bridge map
    # entirely stays on fgl_cross_dm_deferred.
    data_model_element_fgl_cross_dm_upstream_schema_unavailable: int = 0

    # Entries dropped as duplicates by the pagination-level natural-key
    # dedup in ``_paginated_entries`` / lineage raw dedup. Normally 0;
    # non-zero indicates an echoed pagination cursor or server-side
    # overlap between pages -- correctness is preserved (no double
    # emission) but the signal is surfaced here so operators can spot it.
    pagination_duplicate_entries_dropped: int = 0
    # Entries dropped by per-endpoint ``ValidationError`` handling. Only
    # the first ``_MAX_MALFORMED_WARNINGS_PER_ENDPOINT`` rows per endpoint
    # emit a user-visible warning to prevent report flooding on a
    # vendor-wide regression; this counter captures the rest.
    pagination_malformed_entries_dropped: int = 0

    element_dm_edge: ElementDmEdgeReport = field(default_factory=ElementDmEdgeReport)

    # DM customSQL element SQL parsing counters.
    # Elements skipped before aggregator registration (no definition, missing /
    # unknown connection, unsupported platform, duplicate source_id).
    dm_customsql_skipped: int = 0
    dm_customsql_aggregator_invocations: int = 0
    dm_customsql_aggregator_invocation_errors: int = 0
    dm_customsql_parse_failed: int = 0
    dm_customsql_upstream_emitted: int = 0
    dm_customsql_column_lineage_emitted: int = 0
    # FGL downstream fields dropped because the SQL column name had no matching
    # Sigma display column (formula ref absent or element name mismatch).
    dm_customsql_fgl_downstream_unmapped: int = 0
    # Elements whose col mapping was populated (at least partially) via the
    # columnId path rather than formula bracket refs alone.  Non-zero confirms
    # the passthrough-column bridge is active.
    dm_customsql_col_mapping_via_columnid: int = 0
    # columnIds that could not be used as SQL column names (per-column encounter).
    # Fired for bare non-UPPER_SNAKE identifiers, non-inode slash-shaped IDs, and
    # inode entries with an empty native part.  Non-zero on Snowflake means the DM
    # has composition-formula columns (expected); non-zero on other warehouses may
    # indicate unrecognised columnId formats.  Falls back to formula-ref path.
    dm_customsql_col_mapping_columnid_rejected: int = 0

    # Workbook customSQL chart SQL parsing counters (mirrors dm_customsql_* set).
    workbook_customsql_skipped: int = 0
    workbook_customsql_aggregator_invocations: int = 0
    workbook_customsql_aggregator_invocation_errors: int = 0
    workbook_customsql_parse_failed: int = 0
    workbook_customsql_upstream_emitted: int = 0
    workbook_customsql_column_lineage_emitted: int = 0
    workbook_customsql_fgl_downstream_unmapped: int = 0

    # Connection registry counters.
    # Records whose Sigma type mapped to a known DataHub platform.
    connections_resolved: int = 0
    # Records whose Sigma type is not in the platform map -- non-zero is a
    # signal to extend SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP.
    connections_unmappable_type: int = 0
    # Records dropped because they had no connectionId / id field.
    connections_skipped_missing_id: int = 0
    # Records whose connectionId collided with one already seen; later
    # records overwrite earlier ones in by_id.
    connections_duplicate_id: int = 0

    # DM element -> warehouse table UpstreamLineage counters.
    # Success: one per unique warehouse upstream URN emitted (post-dedup).
    dm_element_warehouse_upstream_emitted: int = 0
    # connectionId not in registry, or registry record has is_mappable=False.
    # Note: this counter overlaps with data_model_element_upstreams_unresolved_external
    # when both warehouse and SD resolution fail for the same source_id — the
    # same edge is tallied in both buckets (warehouse failure sub-category +
    # aggregate unresolved). This is intentional: the new counter sub-categorizes
    # rather than replaces the existing one.
    dm_element_warehouse_unknown_connection: int = 0
    # /files/{inodeId} returned non-200 or raised an exception (first attempt
    # per inode only; cache hits of a prior failure are not double-counted).
    dm_element_warehouse_table_lookup_failed: int = 0
    # Sub-bucket of table_lookup_failed: /files returned 429 after retries.
    dm_element_warehouse_table_lookup_rate_limited: int = 0
    # /files path could not be parsed as Connection Root/<DB>/<SCHEMA>.
    dm_element_warehouse_path_unparseable: int = 0
    # type=table lineage entry missing inodeId or name; skipped to avoid
    # emitting a malformed URN.
    dm_element_warehouse_table_entry_incomplete: int = 0

    # Why columnId-based warehouse resolution returned nothing, by gate. The
    # deferred counter it feeds is normally the largest bucket in the report and
    # said nothing about cause; keys are:
    #   columnId_not_inode_shaped     -- not a warehouse passthrough at all
    #   columnId_missing_native_part  -- "inode-<id>" with no "/<COLUMN>"
    #   url_id_not_in_element_source_ids -- column claims an inode the element
    #                                    does not declare (payload drift)
    #   url_id_not_in_warehouse_map   -- /files never resolved that inode
    #   parent_urn_unresolved         -- connection unmappable / no platform
    #   connection_not_in_registry    -- connection id absent from /connections
    warehouse_passthrough_miss_reasons: Dict[str, int] = field(default_factory=dict)

    # Chart element -> workbook, for the charts above. Without this a chart URN
    # from a ticket cannot be placed in a workbook from the log at all; it took
    # a bisect of emission-order dashboard URNs to do it by hand once.
    def note_chart_column_lineage_outcome(
        self,
        *,
        chart_element_id: str,
        workbook_id: str,
        workbook_name: str,
        total_columns: int,
        self_ref_columns: int,
        causes: Dict[str, int],
    ) -> None:
        """File one chart under its column-lineage outcome.

        ``causes`` is the per-column cause tally for THIS chart, as counter
        deltas measured across the element's build.
        """
        self.charts_classified_total += 1
        if total_columns == 0:
            # A chart with no columns emits an EMPTY InputFields aspect, which
            # renders exactly like a chart whose columns all failed: lineage at
            # the chart level and nothing below it. It used to satisfy
            # "self_ref_columns == 0" and be counted as HAVING column lineage,
            # so the one shape that is indistinguishable from the reported
            # symptom was the one the report called healthy.
            self.charts_with_no_columns += 1
            self.charts_with_no_columns_samples.append(
                f"element={chart_element_id} workbook={workbook_id} "
                f"workbook_name={workbook_name!r}"
            )
            return
        if self_ref_columns == 0:
            self.charts_with_column_lineage += 1
            return
        if self_ref_columns < total_columns:
            self.charts_with_partial_column_lineage += 1
            return
        self.charts_with_no_column_lineage += 1
        cause = max(causes, key=lambda k: causes[k]) if causes else "unattributed"
        self.charts_with_no_column_lineage_by_cause[cause] = (
            self.charts_with_no_column_lineage_by_cause.get(cause, 0) + 1
        )
        self.charts_with_no_column_lineage_samples.setdefault(
            cause, LossyList()
        ).append(
            f"element={chart_element_id} workbook={workbook_id} "
            f"workbook_name={workbook_name!r} columns={total_columns}"
        )


class WarehouseConnectionConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    """Per-connection env / platform_instance overrides for warehouse URN construction.

    Maps a Sigma connectionId to the env and platform_instance of the
    corresponding DataHub warehouse connector run.  When a connection is not
    listed, the Sigma source's own env is used and platform_instance defaults
    to None — correct for single-env, single-instance deployments but
    produces dangling lineage for multi-env or multi-instance setups.
    """

    default_database: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "Default database name for this connection. Used (a) to fill the "
            "database layer when Sigma's /files path omits it (e.g. Redshift: "
            "'Connection Root/SCHEMA') and (b) as the SQL parser fallback when "
            "customSQL definitions reference unqualified tables. Set this to the "
            "database name the warehouse connector uses so the emitted URNs match "
            "(e.g. 'dev' to produce 'dev.public.table')."
        ),
    )

    default_schema: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "Default schema name for this connection. Used as the SQL parser "
            "fallback when customSQL definitions reference unqualified tables. "
            "Required for warehouses where Sigma's connection record does not "
            "include a schema field."
        ),
    )
    convert_urns_to_lowercase: bool = pydantic.Field(
        default=True,
        description=(
            "Whether to lower-case warehouse identifiers when constructing "
            "Dataset URNs. Must match the convert_urns_to_lowercase setting "
            "used by the corresponding warehouse connector recipe. Defaults "
            "to True (matching the Snowflake connector default). Set to False "
            "if the warehouse source was ingested with "
            "convert_urns_to_lowercase: false."
        ),
    )


class PlatformDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    data_source_platform: str = pydantic.Field(
        description="A chart's data sources platform name.",
    )
    default_db: Optional[str] = pydantic.Field(
        default=None,
        description="Default database name to use when parsing SQL queries. "
        "Used to generate fully qualified table URNs (e.g., 'prod' for 'prod.public.table').",
    )
    default_schema: Optional[str] = pydantic.Field(
        default=None,
        description="Default schema name to use when parsing SQL queries. "
        "Used to generate fully qualified table URNs (e.g., 'public' for 'prod.public.table').",
    )


class SigmaSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    api_url: str = pydantic.Field(
        default=Constant.DEFAULT_API_URL, description="Sigma API hosted URL."
    )
    client_id: str = pydantic.Field(description="Sigma Client ID")
    client_secret: TransparentSecretStr = pydantic.Field(
        description="Sigma Client Secret"
    )
    # Sigma workspace identifier
    workspace_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Sigma workspaces in ingestion."
        "Mention 'My documents' if personal entities also need to ingest.",
    )
    ingest_owner: Optional[bool] = pydantic.Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI.",
    )
    ingest_shared_entities: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether to ingest the shared entities or not.",
    )
    extract_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="Whether to extract lineage of workbook's elements and datasets or not.",
    )
    workbook_lineage_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter workbook's elements and datasets lineage in ingestion."
        "Requires extract_lineage to be enabled.",
    )
    chart_sources_platform_mapping: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of the sigma workspace/workbook/chart folder path to all chart's data sources platform details present inside that folder path.",
    )
    connection_to_platform_map: Dict[str, WarehouseConnectionConfig] = pydantic.Field(
        default_factory=dict,
        description=(
            "Per-connection env / platform_instance overrides for warehouse URN "
            "construction from DM element lineage. Keys are Sigma connectionIds "
            "(visible in the Sigma admin UI or /v2/connections response). "
            "When a connection is not listed, the Sigma source's own env is used "
            "and platform_instance defaults to None, which is correct for "
            "single-env, single-instance deployments. For multi-env or "
            "multi-instance warehouse setups, add an entry here so the emitted "
            "UpstreamLineage edge points at the URN the warehouse connector "
            "actually produced."
        ),
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Sigma Stateful Ingestion Config."
    )
    workbook_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Sigma workbook names in ingestion.",
    )
    ingest_data_models: bool = pydantic.Field(
        default=True,
        description="Whether to ingest Sigma Data Models. Each Data Model is emitted "
        "as a Container with one Dataset per element inside it (plus per-element "
        "``SchemaMetadata`` and, when ``extract_lineage`` is also enabled, "
        "``UpstreamLineage``). Enabling this issues ``/dataModels/{id}/elements`` and "
        "``/columns`` calls per Data Model unconditionally; the ``/lineage`` call is "
        "only issued when ``extract_lineage`` is also ``True`` (so users who opt out "
        "of lineage at the workbook surface don't get a lineage endpoint hit under a "
        "different flag).",
    )
    extract_data_model_spec_lineage: bool = pydantic.Field(
        default=True,
        description="Whether to read ``/dataModels/{id}/spec``, the only source "
        "for column lineage a formula cannot express. It governs TWO kinds of "
        "edge, so turning it off drops both:\n\n"
        "* **Join keys.** A join's output column carries a formula naming only "
        "one side, so without the ON clause the other side's key column gets no "
        "edge. These score 0.7 (0.6 under an outer join) because a predicate "
        "asserts equality rather than a value copy.\n"
        "* **Union branches.** A union's output column names at most one branch "
        "in its formula, so every other branch is invisible. These score 1.0 — "
        "a union stacks rows, so the output column IS each branch's column.\n\n"
        "Costs one extra API call per Data Model. Requires ``ingest_data_models`` "
        "and ``extract_lineage``.",
    )
    ingest_pivot_and_input_tables: bool = pydantic.Field(
        default=True,
        description="Whether to ingest ``pivot-table`` and ``input-table`` workbook "
        "elements as Charts alongside ``table`` and ``visualization``. They hold "
        "real columns that other elements' formulas reference, so excluding them "
        "leaves those references permanently unresolvable. Enabling this emits "
        "chart entities that earlier versions did not, and costs two extra API "
        "calls per newly-admitted element; set it to ``False`` to keep the "
        "previous entity set.",
    )
    resolve_chart_refs_by_element_name: bool = pydantic.Field(
        default=False,
        description="Resolve a chart formula reference whose source Sigma never "
        "declared as an upstream by matching the name against the workbook's "
        "elements and the Data Models it loads. OFF by default: measured on one "
        "tenant this guess produced 1,106 extra edges out of 440,069, and "
        "``inputFields`` carries no confidenceScore -- so a wrongly-guessed edge "
        "is byte-identical to one Sigma stated, and nothing downstream can audit "
        "or filter it. Enable only if a best-effort edge is preferable to none, "
        "accepting that some will be wrong.",
    )
    data_model_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Sigma Data Model names in ingestion. "
        "Requires ingest_data_models to be enabled.",
    )
    max_personal_dm_discovery_rounds: int = pydantic.Field(
        default=20,
        ge=1,
        description="Belt-and-braces safety cap on the number of passes the "
        "personal-space Data Model discovery loop is allowed to make. Each "
        "pass fetches ``/v2/dataModels/{urlId}`` for every newly-seen "
        "cross-DM ``<urlId>`` prefix; the loop terminates naturally when "
        "``unresolved_seen`` plateaus (monotonically growing set), so under "
        "a well-behaved API this cap is never hit. Must be ``>= 1`` -- set "
        "``ingest_shared_entities: False`` (or leave it at the default) if "
        "the goal is to disable personal-space discovery entirely; ``0`` / "
        "negative values are rejected because the first pass is required "
        "to prepopulate the bridge maps for listed DMs. Exists to protect "
        "against pathological Sigma payloads (e.g. a chain of personal-space "
        "DMs that keep referencing newly-discovered personal-space DMs) by "
        "breaking with a ``SourceReport.warning`` instead of looping "
        "unbounded.",
    )
