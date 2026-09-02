from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional, Tuple

from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from datahub.ingestion.source.unity.platform_resource_repository import (
        UnityCatalogPlatformResourceRepository,
    )


@dataclass
class UnityCatalogReport(SQLSourceReport):
    metastores: EntityFilterReport = EntityFilterReport.field(type="metastore")
    catalogs: EntityFilterReport = EntityFilterReport.field(type="catalog")
    schemas: EntityFilterReport = EntityFilterReport.field(type="schema")
    # Metric views also count as tables so soft-delete keeps tracking them.
    tables: EntityFilterReport = EntityFilterReport.field(type="table/view")
    table_profiles: EntityFilterReport = EntityFilterReport.field(type="table profile")
    notebooks: EntityFilterReport = EntityFilterReport.field(type="notebook")
    ml_models: EntityFilterReport = EntityFilterReport.field(type="ml_model")
    ml_model_versions: EntityFilterReport = EntityFilterReport.field(
        type="ml_model_version"
    )
    metric_views: EntityFilterReport = EntityFilterReport.field(type="metric_view")

    hive_metastore_catalog_found: Optional[bool] = None

    num_column_lineage_skipped_column_count: int = 0
    num_external_upstreams_lacking_permissions: int = 0
    num_external_upstreams_unsupported: int = 0
    num_external_upstreams_partition_stripped: int = 0

    # Query-linked lineage. Read these together: statements_seen is the whole
    # population fetched from system.query.history, of which read-only statements
    # (no target table) are never linkable; edges_linked plus edges_unlinked is
    # every lineage edge the connector emitted while the feature was active, so
    # edges_linked == 0 with a non-zero seen count means the feature did nothing.
    num_query_entities_emitted: int = 0
    num_lineage_edges_query_linked: int = 0
    num_lineage_edges_query_unlinked: int = 0
    num_lineage_statements_seen: int = 0
    # Statements with no usable text; statements resolved to text is
    # num_lineage_statements_seen minus this.
    num_lineage_statements_skipped: int = 0
    num_lineage_statements_unresolved_tables: int = 0
    # The query-lineage path fetches system.query.history a second time (the usage
    # path does its own fetch). Its parse and row-read problems are recorded here
    # rather than on the usage counters below, so one underlying failure is not
    # counted twice. Sharing a single fetch between the two paths is a follow-up.
    num_query_lineage_fetch_statements_missing_info: int = 0
    num_query_lineage_fetch_row_field_read_errors: int = 0

    num_queries: int = 0
    num_queries_dropped: int = 0
    num_queries_preparsed_from_lineage: int = 0
    num_queries_observed_sqlglot: int = 0
    num_queries_without_system_table_lineage: int = 0
    num_queries_skipped_without_system_table_lineage: int = 0
    num_queries_preparsed_fallback_to_sqlglot: int = 0
    num_queries_preparsed_fingerprint_fallback: int = 0
    num_lineage_tables_unresolvable: int = 0
    lineage_tables_unresolvable_sample: LossyList[str] = field(
        default_factory=LossyList
    )
    num_lineage_row_field_read_errors: int = 0
    num_usage_query_fetch_failures: int = 0

    # Usage step timers. fetch = draining query history off the warehouse cursor
    # (released as soon as the generator is consumed); parsing = sqlglot/preparsed
    # processing of the buffered queries. Splitting them makes the drain-before-parse
    # behavior measurable and surfaces eviction-risk (fetch time creeping toward
    # parse time) — mirroring BigQuery's query_log_fetch_timer / sql_parsing_sec.
    usage_query_fetch_timer: PerfTimer = field(default_factory=PerfTimer)
    usage_parsing_timer: PerfTimer = field(default_factory=PerfTimer)

    profile_table_timeouts: LossyList[str] = field(default_factory=LossyList)
    profile_table_empty: LossyList[str] = field(default_factory=LossyList)
    profile_table_errors: LossyDict[str, LossyList[Tuple[str, str]]] = field(
        default_factory=LossyDict
    )
    num_profile_missing_size_in_bytes: int = 0
    num_profile_missing_row_count: int = 0
    num_profile_failed_unsupported_column_type: int = 0
    num_profile_failed_int_casts: int = 0

    num_catalogs_missing_name: int = 0
    num_schemas_missing_name: int = 0
    num_tables_missing_name: int = 0
    num_federation_connections_list_failed: int = 0
    num_foreign_catalogs: int = 0
    num_federation_links_emitted: int = 0
    num_federation_links_failed: int = 0
    num_federation_targets_unresolved: int = 0
    num_federation_columns_backfilled: int = 0
    num_federation_columns_backfill_failed: int = 0
    num_federation_cll_skipped: int = 0
    num_federation_external_schema_fetch_failed: int = 0
    num_federation_property_defs_failed: int = 0
    num_ml_models_missing_name: int = 0
    num_columns_missing_name: int = 0
    num_queries_missing_info: int = 0
    num_metric_views_yaml_parse_failures: int = 0
    num_metric_views_yaml_shape_invalid: int = 0
    num_metric_views_no_parseable_sources: int = 0
    num_metric_views_expr_parse_failures: int = 0
    num_metric_view_joins_skipped: int = 0
    num_metric_view_unresolved_qualifiers: int = 0
    num_metric_view_unparseable_sources: int = 0
    num_metric_view_skipped_dim_measure_entries: int = 0
    num_metric_view_expr_empty_tree: int = 0
    num_metric_view_unresolved_measure_refs: int = 0
    num_metric_view_display_name_truncated: int = 0
    num_metric_view_synonyms_overflow: int = 0
    num_metric_view_synonyms_truncated: int = 0
    num_metric_view_synonyms_dropped_invalid: int = 0
    num_metric_view_format_unknown_subkeys: int = 0

    # Platform resource repository for automatic cache statistics via SupportsAsObj
    tag_urn_resolver_cache: Optional["UnityCatalogPlatformResourceRepository"] = None
