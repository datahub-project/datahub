import collections
import itertools
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import humanfriendly
from google.cloud.datacatalog import lineage_v1
from google.cloud.logging_v2.client import Client as GCPLoggingClient

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditLogEntry,
    BigQueryAuditMetadata,
    BigqueryTableIdentifier,
    BigQueryTableRef,
    QueryEvent,
    ReadEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_audit_log_api import (
    BigQueryAuditLogApi,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigQuerySchemaApi,
    BigqueryTableSnapshot,
)
from datahub.ingestion.source.bigquery_v2.common import BQ_DATETIME_FORMAT
from datahub.ingestion.source.bigquery_v2.queries import (
    BQ_FILTER_RULE_TEMPLATE_V2_LINEAGE,
    bigquery_audit_metadata_query_template_lineage,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities import memory_footprint
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.sqlglot_lineage import (
    SchemaResolver,
    SqlParsingResult,
    sqlglot_lineage,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger: logging.Logger = logging.getLogger(__name__)


@dataclass(order=True, eq=True, frozen=True)
class LineageEdgeColumnMapping:
    out_column: str
    in_columns: FrozenSet[str]


@dataclass(order=True, eq=True, frozen=True)
class LineageEdge:
    table: str  # in BigQueryTableRef format
    column_mapping: FrozenSet[LineageEdgeColumnMapping]

    auditStamp: datetime
    type: str = DatasetLineageTypeClass.TRANSFORMED
    column_confidence: float = 0.0


def _merge_lineage_edge_columns(
    a: Optional[LineageEdge], b: LineageEdge
) -> LineageEdge:
    if a is None:
        return b
    assert a.table == b.table

    merged_col_mapping: Dict[str, Set[str]] = collections.defaultdict(set)
    for col_mapping in itertools.chain(a.column_mapping, b.column_mapping):
        merged_col_mapping[col_mapping.out_column].update(col_mapping.in_columns)

    return LineageEdge(
        table=a.table,
        column_mapping=frozenset(
            LineageEdgeColumnMapping(
                out_column=out_column,
                in_columns=frozenset(in_columns),
            )
            for out_column, in_columns in merged_col_mapping.items()
        ),
        auditStamp=a.auditStamp,
        type=a.type,
        column_confidence=min(a.column_confidence, b.column_confidence),
    )


def _follow_column_lineage(
    temp: LineageEdge,
    upstream: LineageEdge,
) -> LineageEdge:
    """
    Collapse lineage of `base -> temp -> upstream` into `base -> upstream`.
    """

    # This is a mapping from temp's column -> upstream's columns.
    upstream_col_mapping: Dict[str, Set[str]] = collections.defaultdict(set)
    for col_mapping in upstream.column_mapping:
        upstream_col_mapping[col_mapping.out_column].update(col_mapping.in_columns)

    # This code can be a bit confusing.
    # - temp.column_mapping[i].out_column is a column in base.
    # - temp.column_mapping[i].in_columns are columns in temp.
    # - For each temp_col in temp.column_mapping[i].in_columns, we want to replace it with
    #   the columns in upstream that it depends on.
    return LineageEdge(
        table=upstream.table,
        column_mapping=frozenset(
            LineageEdgeColumnMapping(
                out_column=base_column_mapping.out_column,
                in_columns=frozenset(
                    col
                    for temp_col in base_column_mapping.in_columns
                    for col in upstream_col_mapping.get(temp_col, set())
                ),
            )
            for base_column_mapping in temp.column_mapping
        ),
        # We use the audit stamp and type from temp, since it's "closer" to the base.
        auditStamp=temp.auditStamp,
        type=temp.type,
        column_confidence=min(temp.column_confidence, upstream.column_confidence),
    )


def make_lineage_edges_from_parsing_result(
    sql_lineage: SqlParsingResult, audit_stamp: datetime, lineage_type: str
) -> List[LineageEdge]:
    # Note: This ignores the out_tables section of the sql parsing result.
    audit_stamp = datetime.now(timezone.utc)

    # Invariant: table_edges[k].table == k
    table_edges: Dict[str, LineageEdge] = {}

    # This isn't the most efficient code - it iterates over the same data multiple times,
    # once per in_table entry.
    for table_urn in sql_lineage.in_tables:
        column_mapping: Dict[str, FrozenSet[str]] = {}

        for column_lineage in sql_lineage.column_lineage or []:
            out_column = column_lineage.downstream.column
            column_mapping[out_column] = frozenset(
                upstream_column_info.column
                for upstream_column_info in column_lineage.upstreams
                if upstream_column_info.table == table_urn
            )

        try:
            table_name = str(
                BigQueryTableRef.from_bigquery_table(
                    BigqueryTableIdentifier.from_string_name(
                        DatasetUrn.from_string(table_urn).name
                    )
                )
            )
        except IndexError as e:
            logger.debug(f"Unable to parse table urn {table_urn}: {e}")
            continue

        table_edges[table_name] = LineageEdge(
            table=table_name,
            column_mapping=frozenset(
                LineageEdgeColumnMapping(out_column=out_column, in_columns=in_columns)
                for out_column, in_columns in column_mapping.items()
                if in_columns
            ),
            auditStamp=audit_stamp,
            type=lineage_type,
            column_confidence=sql_lineage.debug_info.confidence,
        )

    return list(table_edges.values())


def make_lineage_edge_for_snapshot(
    snapshot: BigqueryTableSnapshot,
) -> Optional[LineageEdge]:
    if snapshot.base_table_identifier:
        base_table_name = str(
            BigQueryTableRef.from_bigquery_table(snapshot.base_table_identifier)
        )
        return LineageEdge(
            table=base_table_name,
            column_mapping=frozenset(
                LineageEdgeColumnMapping(
                    out_column=column.field_path,
                    in_columns=frozenset([column.field_path]),
                )
                for column in snapshot.columns
            ),
            auditStamp=datetime.now(timezone.utc),
            type=DatasetLineageTypeClass.TRANSFORMED,
        )
    return None


class BigqueryLineageExtractor:
    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        dataset_urn_builder: Callable[[BigQueryTableRef], str],
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = None,
    ):
        self.config = config
        self.report = report
        self.dataset_urn_builder = dataset_urn_builder
        self.audit_log_api = BigQueryAuditLogApi(
            report.audit_log_api_perf,
            self.config.rate_limit,
            self.config.requests_per_min,
        )

        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = (
            self.report.lineage_start_time,
            self.report.lineage_end_time,
        ) = self.get_time_window()

    def get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        else:
            return self.config.start_time, self.config.end_time

    def error(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.error(f"{key} => {reason}")

    def _should_ingest_lineage(self) -> bool:
        if (
            self.redundant_run_skip_handler
            and self.redundant_run_skip_handler.should_skip_this_run(
                cur_start_time=self.config.start_time,
                cur_end_time=self.config.end_time,
            )
        ):
            # Skip this run
            self.report.report_warning(
                "lineage-extraction",
                "Skip this run as there was already a run for current ingestion window.",
            )
            return False

        return True

    def get_lineage_workunits(
        self,
        projects: List[str],
        sql_parser_schema_resolver: SchemaResolver,
        view_refs_by_project: Dict[str, Set[str]],
        view_definitions: FileBackedDict[str],
        snapshot_refs_by_project: Dict[str, Set[str]],
        snapshots_by_ref: FileBackedDict[BigqueryTableSnapshot],
        table_refs: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        if not self._should_ingest_lineage():
            return
        datasets_skip_audit_log_lineage: Set[str] = set()
        dataset_lineage: Dict[str, Set[LineageEdge]] = {}
        for project in projects:
            self.populate_snapshot_lineage(
                dataset_lineage,
                snapshot_refs_by_project[project],
                snapshots_by_ref,
            )

            if self.config.lineage_parse_view_ddl:
                self.populate_view_lineage_with_sql_parsing(
                    dataset_lineage,
                    view_refs_by_project[project],
                    view_definitions,
                    sql_parser_schema_resolver,
                    project,
                )

            datasets_skip_audit_log_lineage.update(dataset_lineage.keys())
            for lineage_key in dataset_lineage.keys():
                yield from self.gen_lineage_workunits_for_table(
                    dataset_lineage, BigQueryTableRef.from_string_name(lineage_key)
                )

        if self.config.use_exported_bigquery_audit_metadata:
            projects = ["*"]  # project_id not used when using exported metadata

        for project in projects:
            self.report.set_ingestion_stage(project, "Lineage Extraction")
            yield from self.generate_lineage(
                project,
                sql_parser_schema_resolver,
                datasets_skip_audit_log_lineage,
                table_refs,
            )

        if self.redundant_run_skip_handler:
            # Update the checkpoint state for this run.
            self.redundant_run_skip_handler.update_state(
                self.config.start_time, self.config.end_time
            )

    def generate_lineage(
        self,
        project_id: str,
        sql_parser_schema_resolver: SchemaResolver,
        datasets_skip_audit_log_lineage: Set[str],
        table_refs: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Generate lineage for {project_id}")
        with PerfTimer() as timer:
            try:
                if self.config.extract_lineage_from_catalog:
                    lineage = self.lineage_via_catalog_lineage_api(project_id)
                else:
                    events = self._get_parsed_audit_log_events(project_id)
                    lineage = self._create_lineage_map(
                        events, sql_parser_schema_resolver
                    )
            except Exception as e:
                if project_id:
                    self.report.lineage_failed_extraction.append(project_id)
                self.error(
                    logger,
                    "lineage",
                    f"{project_id}: {e}",
                )
                lineage = {}

            self.report.lineage_metadata_entries[project_id] = len(lineage)
            logger.info(f"Built lineage map containing {len(lineage)} entries.")
            logger.debug(f"lineage metadata is {lineage}")
            self.report.lineage_extraction_sec[project_id] = round(
                timer.elapsed_seconds(), 2
            )
            self.report.lineage_mem_size[project_id] = humanfriendly.format_size(
                memory_footprint.total_size(lineage)
            )

        for lineage_key in lineage.keys():
            # For views, we do not use the upstreams obtained by parsing audit logs
            # as they may contain indirectly referenced tables.
            if (
                lineage_key not in table_refs
                or lineage_key in datasets_skip_audit_log_lineage
            ):
                continue

            yield from self.gen_lineage_workunits_for_table(
                lineage, BigQueryTableRef.from_string_name(lineage_key)
            )

    def populate_view_lineage_with_sql_parsing(
        self,
        view_lineage: Dict[str, Set[LineageEdge]],
        view_refs: Set[str],
        view_definitions: FileBackedDict[str],
        sql_parser_schema_resolver: SchemaResolver,
        default_project: str,
    ) -> None:
        for view in view_refs:
            view_definition = view_definitions[view]
            raw_view_lineage = sqlglot_lineage(
                view_definition,
                schema_resolver=sql_parser_schema_resolver,
                default_db=default_project,
            )
            if raw_view_lineage.debug_info.table_error:
                logger.debug(
                    f"Failed to parse lineage for view {view}: {raw_view_lineage.debug_info.table_error}"
                )
                self.report.num_view_definitions_failed_parsing += 1
                self.report.view_definitions_parsing_failures.append(
                    f"Table-level sql parsing error for view {view}: {raw_view_lineage.debug_info.table_error}"
                )
                continue
            elif raw_view_lineage.debug_info.column_error:
                self.report.num_view_definitions_failed_column_parsing += 1
                self.report.view_definitions_parsing_failures.append(
                    f"Column-level sql parsing error for view {view}: {raw_view_lineage.debug_info.column_error}"
                )
            else:
                self.report.num_view_definitions_parsed += 1

            ts = datetime.now(timezone.utc)
            view_lineage[view] = set(
                make_lineage_edges_from_parsing_result(
                    raw_view_lineage,
                    audit_stamp=ts,
                    lineage_type=DatasetLineageTypeClass.VIEW,
                )
            )

    def populate_snapshot_lineage(
        self,
        snapshot_lineage: Dict[str, Set[LineageEdge]],
        snapshot_refs: Set[str],
        snapshots_by_ref: FileBackedDict[BigqueryTableSnapshot],
    ) -> None:
        for snapshot in snapshot_refs:
            lineage_edge = make_lineage_edge_for_snapshot(snapshots_by_ref[snapshot])
            if lineage_edge:
                snapshot_lineage[snapshot] = {lineage_edge}

    def gen_lineage_workunits_for_table(
        self, lineage: Dict[str, Set[LineageEdge]], table_ref: BigQueryTableRef
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self.dataset_urn_builder(table_ref)

        lineage_info = self.get_lineage_for_table(
            bq_table=table_ref,
            bq_table_urn=dataset_urn,
            lineage_metadata=lineage,
        )
        if lineage_info:
            yield from self.gen_lineage(dataset_urn, lineage_info)

    def gen_lineage(
        self,
        dataset_urn: str,
        upstream_lineage: Optional[UpstreamLineageClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if upstream_lineage is None:
            return

        if upstream_lineage is not None:
            if self.config.incremental_lineage:
                patch_builder: DatasetPatchBuilder = DatasetPatchBuilder(
                    urn=dataset_urn
                )
                for upstream in upstream_lineage.upstreams:
                    patch_builder.add_upstream_lineage(upstream)

                yield from [
                    MetadataWorkUnit(
                        id=f"upstreamLineage-for-{dataset_urn}",
                        mcp_raw=mcp,
                    )
                    for mcp in patch_builder.build()
                ]
            else:
                if not self.config.extract_column_lineage:
                    upstream_lineage.fineGrainedLineages = None

                yield from [
                    MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=upstream_lineage
                    ).as_workunit()
                ]

    def lineage_via_catalog_lineage_api(
        self, project_id: str
    ) -> Dict[str, Set[LineageEdge]]:
        """
        Uses Data Catalog API to request lineage metadata. Please take a look at the API documentation for more details.

        NOTE: It's necessary for you to enable the service API in your Google Cloud Project.

        Args:
            project_id(str): Google project id. Used to search for tables and datasets.

        Returns:
            Dict[str, Set[str]] - A dictionary, where keys are the downstream table's identifier and values is a set
            of upstream tables identifiers.
        """
        logger.info("Populating lineage info via Catalog Data Linage API")

        # Regions to search for BigQuery tables: projects/{project_id}/locations/{region}
        enabled_regions: List[str] = ["US", "EU"]

        try:
            lineage_client: lineage_v1.LineageClient = lineage_v1.LineageClient()

            data_dictionary = BigQuerySchemaApi(
                self.report.schema_api_perf, self.config.get_bigquery_client()
            )

            # Filtering datasets
            datasets = list(data_dictionary.get_datasets_for_project_id(project_id))
            project_tables = []
            for dataset in datasets:
                # Enables only tables where type is TABLE, VIEW or MATERIALIZED_VIEW (not EXTERNAL)
                project_tables.extend(
                    [
                        table
                        for table in data_dictionary.list_tables(
                            dataset.name, project_id
                        )
                        if table.table_type in ["TABLE", "VIEW", "MATERIALIZED_VIEW"]
                    ]
                )

            # Convert project tables to <project_id>.<dataset_id>.<table_id> format
            project_table_names = list(
                map(
                    lambda table: "{}.{}.{}".format(
                        table.project, table.dataset_id, table.table_id
                    ),
                    project_tables,
                )
            )

            lineage_map: Dict[str, Set[LineageEdge]] = {}
            curr_date = datetime.now()
            for table in project_table_names:
                logger.info("Creating lineage map for table %s", table)
                upstreams = set()
                downstream_table = lineage_v1.EntityReference()
                # fully_qualified_name in format: "bigquery:<project_id>.<dataset_id>.<table_id>"
                downstream_table.fully_qualified_name = f"bigquery:{table}"
                # Searches in different regions
                for region in enabled_regions:
                    location_request = lineage_v1.SearchLinksRequest(
                        target=downstream_table,
                        parent=f"projects/{project_id}/locations/{region.lower()}",
                    )
                    response = lineage_client.search_links(request=location_request)
                    upstreams.update(
                        [
                            str(lineage.source.fully_qualified_name).replace(
                                "bigquery:", ""
                            )
                            for lineage in response
                        ]
                    )

                # Downstream table identifier
                destination_table_str = str(
                    BigQueryTableRef(
                        table_identifier=BigqueryTableIdentifier(*table.split("."))
                    )
                )

                # Only builds lineage map when the table has upstreams
                if upstreams:
                    lineage_map[destination_table_str] = set(
                        [
                            LineageEdge(
                                table=str(
                                    BigQueryTableRef(
                                        table_identifier=BigqueryTableIdentifier.from_string_name(
                                            source_table
                                        )
                                    )
                                ),
                                column_mapping=frozenset(),
                                auditStamp=curr_date,
                            )
                            for source_table in upstreams
                        ]
                    )
            return lineage_map
        except Exception as e:
            self.error(
                logger,
                "lineage-exported-catalog-lineage-api",
                f"Error: {e}",
            )
            raise e

    def _get_parsed_audit_log_events(self, project_id: str) -> Iterable[QueryEvent]:
        # We adjust the filter values a bit, since we need to make sure that the join
        # between query events and read events is complete. For example, this helps us
        # handle the case where the read happens within our time range but the query
        # completion event is delayed and happens after the configured end time.
        corrected_start_time = self.start_time - self.config.max_query_duration
        corrected_end_time = self.end_time + self.config.max_query_duration
        self.report.log_entry_start_time = corrected_start_time
        self.report.log_entry_end_time = corrected_end_time

        parse_fn: Callable[[Any], Optional[Union[ReadEvent, QueryEvent]]]
        if self.config.use_exported_bigquery_audit_metadata:
            entries = self.get_exported_log_entries(
                corrected_start_time, corrected_end_time
            )
            parse_fn = self._parse_exported_bigquery_audit_metadata
        else:
            entries = self.get_log_entries_via_gcp_logging(
                project_id, corrected_start_time, corrected_end_time
            )
            parse_fn = self._parse_bigquery_log_entries

        for entry in entries:
            self.report.num_lineage_total_log_entries[project_id] += 1
            try:
                event = parse_fn(entry)
                if event:
                    self.report.num_lineage_parsed_log_entries[project_id] += 1
                    yield event
            except Exception as e:
                logger.warning(f"Unable to parse log entry `{entry}`: {e}")
                self.report.num_lineage_log_parse_failures[project_id] += 1

    def get_exported_log_entries(
        self, corrected_start_time, corrected_end_time, limit=None
    ):
        logger.info("Populating lineage info via exported GCP audit logs")
        bq_client = self.config.get_bigquery_client()
        entries = self.audit_log_api.get_exported_bigquery_audit_metadata(
            bigquery_client=bq_client,
            bigquery_audit_metadata_query_template=bigquery_audit_metadata_query_template_lineage,
            bigquery_audit_metadata_datasets=self.config.bigquery_audit_metadata_datasets,
            use_date_sharded_audit_log_tables=self.config.use_date_sharded_audit_log_tables,
            start_time=corrected_start_time,
            end_time=corrected_end_time,
            limit=limit,
        )
        return entries

    def get_log_entries_via_gcp_logging(
        self, project_id, corrected_start_time, corrected_end_time
    ):
        logger.info("Populating lineage info via exported GCP audit logs")

        logging_client = self.config.make_gcp_logging_client(project_id)
        logger.info(
            f"Start loading log entries from BigQuery for {project_id} "
            f"with start_time={corrected_start_time} and end_time={corrected_end_time}"
        )
        entries = self.audit_log_api.get_bigquery_log_entries_via_gcp_logging(
            logging_client,
            BQ_FILTER_RULE_TEMPLATE_V2_LINEAGE.format(
                start_time=corrected_start_time.strftime(BQ_DATETIME_FORMAT),
                end_time=corrected_end_time.strftime(BQ_DATETIME_FORMAT),
            ),
            self.config.log_page_size,
        )
        return entries

    # Currently we only parse JobCompleted events but in future we would want to parse other
    # events to also create field level lineage.
    def _parse_bigquery_log_entries(
        self,
        entry: AuditLogEntry,
    ) -> Optional[QueryEvent]:
        event: Optional[QueryEvent] = None

        missing_entry = QueryEvent.get_missing_key_entry(entry=entry)
        if missing_entry is None:
            event = QueryEvent.from_entry(
                entry,
                debug_include_full_payloads=self.config.debug_include_full_payloads,
            )

        missing_entry_v2 = QueryEvent.get_missing_key_entry_v2(entry=entry)
        if event is None and missing_entry_v2 is None:
            event = QueryEvent.from_entry_v2(
                entry, self.config.debug_include_full_payloads
            )

        if event is None:
            logger.warning(
                f"Unable to parse log missing {missing_entry}, missing v2 {missing_entry_v2} for {entry}",
            )
            return None
        else:
            return event

    def _parse_exported_bigquery_audit_metadata(
        self, audit_metadata: BigQueryAuditMetadata
    ) -> Optional[QueryEvent]:
        event: Optional[QueryEvent] = None

        missing_exported_audit = (
            QueryEvent.get_missing_key_exported_bigquery_audit_metadata(audit_metadata)
        )

        if missing_exported_audit is None:
            event = QueryEvent.from_exported_bigquery_audit_metadata(
                audit_metadata, self.config.debug_include_full_payloads
            )

        if event is None:
            logger.warning(
                f"Unable to parse audit metadata missing {missing_exported_audit} for {audit_metadata}",
            )
            return None
        else:
            return event

    def _create_lineage_map(
        self,
        entries: Iterable[QueryEvent],
        sql_parser_schema_resolver: SchemaResolver,
    ) -> Dict[str, Set[LineageEdge]]:
        logger.info("Entering create lineage map function")
        lineage_map: Dict[str, Set[LineageEdge]] = collections.defaultdict(set)
        for e in entries:
            self.report.num_total_lineage_entries[e.project_id] = (
                self.report.num_total_lineage_entries.get(e.project_id, 0) + 1
            )

            ts = e.end_time if e.end_time else datetime.now(tz=timezone.utc)

            # Note that we trust the audit log to have the correct destination table.
            destination_table = e.destinationTable
            destination_table_str = str(destination_table)
            if not destination_table:
                self.report.num_skipped_lineage_entries_missing_data[e.project_id] += 1
                continue

            if not is_schema_allowed(
                self.config.dataset_pattern,
                destination_table.table_identifier.dataset,
                destination_table.table_identifier.project_id,
                self.config.match_fully_qualified_names,
            ) or not self.config.table_pattern.allowed(
                destination_table.table_identifier.get_table_name()
            ):
                self.report.num_skipped_lineage_entries_not_allowed[e.project_id] += 1
                continue

            # If there is a view being referenced then bigquery sends both the view as well as underlying table
            # in the references. There is no distinction between direct/base objects accessed. Doing sql parsing
            # ensures that we only use direct objects accessed for lineage.
            #
            # Because of this, we use the SQL parser as the primary source of truth for lineage
            # and the audit log as a fallback.
            lineage_from_event: Set[LineageEdge] = set()

            # Try the sql parser first.
            if self.config.lineage_use_sql_parser:
                raw_lineage = sqlglot_lineage(
                    e.query,
                    schema_resolver=sql_parser_schema_resolver,
                    default_db=e.project_id,
                )
                if raw_lineage.debug_info.table_error:
                    logger.debug(
                        f"Sql Parser failed on query: {e.query}. It won't cause any major issues, but "
                        f"queries referencing views may contain extra lineage for the tables underlying those views. "
                        f"The error was {raw_lineage.debug_info.table_error}."
                    )
                    self.report.num_lineage_entries_sql_parser_failure[
                        e.project_id
                    ] += 1

                lineage_from_event = set(
                    make_lineage_edges_from_parsing_result(
                        raw_lineage,
                        audit_stamp=ts,
                        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )

            if not lineage_from_event:
                # Fallback to the audit log if the sql parser didn't find anything.
                if not (e.referencedTables or e.referencedViews):
                    self.report.num_skipped_lineage_entries_missing_data[
                        e.project_id
                    ] += 1
                    continue

                for ref_table_or_view in [*e.referencedTables, *e.referencedViews]:
                    if str(ref_table_or_view) != destination_table_str:
                        lineage_from_event.add(
                            LineageEdge(
                                table=str(ref_table_or_view),
                                auditStamp=ts,
                                column_mapping=frozenset(),
                                column_confidence=0.1,
                            )
                        )

            if not lineage_from_event:
                self.report.num_skipped_lineage_entries_other[e.project_id] += 1
                continue

            lineage_map[destination_table_str].update(lineage_from_event)

        logger.info("Exiting create lineage map function")
        return lineage_map

    def get_upstream_tables(
        self,
        bq_table: BigQueryTableRef,
        lineage_metadata: Dict[str, Set[LineageEdge]],
        edges_seen: Optional[Set[LineageEdge]] = None,
    ) -> Set[LineageEdge]:
        if edges_seen is None:
            edges_seen = set()

        upstreams: Dict[str, LineageEdge] = {}
        for upstream_lineage in lineage_metadata[str(bq_table)]:
            upstream_table_ref = upstream_lineage.table
            upstream_table = BigQueryTableRef.from_string_name(upstream_table_ref)
            if bq_table == upstream_table:
                # Skip self-references.
                continue

            if upstream_table.is_temporary_table(
                [self.config.temp_table_dataset_prefix]
            ):
                # making sure we don't process a table twice and not get into a recursive loop
                if upstream_lineage in edges_seen:
                    logger.debug(
                        f"Skipping table {upstream_lineage} because it was seen already"
                    )
                    continue
                edges_seen.add(upstream_lineage)

                if upstream_table_ref in lineage_metadata:
                    # `upstream_table` is a temporary table.
                    # We don't want it in the lineage, but we do want its upstreams.
                    # When following lineage for a temp table, we need to merge the column lineage.

                    for temp_table_upstream in self.get_upstream_tables(
                        upstream_table,
                        lineage_metadata=lineage_metadata,
                        edges_seen=edges_seen,
                    ):
                        ref_temp_table_upstream = temp_table_upstream.table

                        # Replace `bq_table -> upstream_table -> temp_table_upstream`
                        # with `bq_table -> temp_table_upstream`, merging the column lineage.
                        collapsed_lineage = _follow_column_lineage(
                            upstream_lineage, temp_table_upstream
                        )

                        upstreams[
                            ref_temp_table_upstream
                        ] = _merge_lineage_edge_columns(
                            upstreams.get(ref_temp_table_upstream),
                            collapsed_lineage,
                        )
            else:
                upstreams[upstream_table_ref] = _merge_lineage_edge_columns(
                    upstreams.get(upstream_table_ref),
                    upstream_lineage,
                )

        return set(upstreams.values())

    def get_lineage_for_table(
        self,
        bq_table: BigQueryTableRef,
        bq_table_urn: str,
        lineage_metadata: Dict[str, Set[LineageEdge]],
    ) -> Optional[UpstreamLineageClass]:
        upstream_list: List[UpstreamClass] = []
        fine_grained_lineages: List[FineGrainedLineageClass] = []
        # Sorting the list of upstream lineage events in order to avoid creating multiple aspects in backend
        # even if the lineage is same but the order is different.
        for upstream in sorted(self.get_upstream_tables(bq_table, lineage_metadata)):
            upstream_table = BigQueryTableRef.from_string_name(upstream.table)
            upstream_table_urn = self.dataset_urn_builder(upstream_table)

            # Generate table-level lineage.
            upstream_table_class = UpstreamClass(
                dataset=upstream_table_urn,
                type=upstream.type,
                auditStamp=AuditStampClass(
                    actor="urn:li:corpuser:datahub",
                    time=int(upstream.auditStamp.timestamp() * 1000),
                ),
            )
            if self.config.upstream_lineage_in_report:
                current_lineage_map: Set = self.report.upstream_lineage.get(
                    str(bq_table), set()
                )
                current_lineage_map.add(str(upstream_table))
                self.report.upstream_lineage[str(bq_table)] = current_lineage_map

            upstream_list.append(upstream_table_class)

            # Generate column-level lineage.
            for col_lineage_edge in upstream.column_mapping:
                fine_grained_lineage = FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        mce_builder.make_schema_field_urn(
                            bq_table_urn, col_lineage_edge.out_column
                        )
                    ],
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        mce_builder.make_schema_field_urn(
                            upstream_table_urn, upstream_col
                        )
                        for upstream_col in col_lineage_edge.in_columns
                    ],
                    confidenceScore=upstream.column_confidence,
                )
                fine_grained_lineages.append(fine_grained_lineage)

        if upstream_list:
            upstream_lineage = UpstreamLineageClass(
                upstreams=upstream_list,
                fineGrainedLineages=fine_grained_lineages,
            )
            return upstream_lineage

        return None

    def test_capability(self, project_id: str) -> None:
        if self.config.use_exported_bigquery_audit_metadata:
            for entry in self.get_exported_log_entries(
                self.start_time,
                self.end_time,
                limit=1,
            ):
                logger.debug(
                    f"Connection test got one exported_bigquery_audit_metadata {entry}"
                )
        else:
            gcp_logging_client: GCPLoggingClient = self.config.make_gcp_logging_client(
                project_id
            )
            for entry in self.audit_log_api.get_bigquery_log_entries_via_gcp_logging(
                gcp_logging_client,
                filter=BQ_FILTER_RULE_TEMPLATE_V2_LINEAGE.format(
                    start_time=self.start_time.strftime(BQ_DATETIME_FORMAT),
                    end_time=self.end_time.strftime(BQ_DATETIME_FORMAT),
                ),
                log_page_size=self.config.log_page_size,
                limit=1,
            ):
                logger.debug(f"Connection test got one audit metadata entry {entry}")

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)
