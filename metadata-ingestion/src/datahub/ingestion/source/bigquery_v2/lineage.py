import collections
import logging
import textwrap
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

import humanfriendly
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from ratelimiter import RateLimiter

from datahub.emitter import mce_builder
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    AuditLogEntry,
    BigQueryAuditMetadata,
    BigqueryTableIdentifier,
    BigQueryTableRef,
    QueryEvent,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryTable,
    BigqueryView,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
    _make_gcp_logging_client,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities import memory_footprint
from datahub.utilities.bigquery_sql_parser import BigQuerySQLParser
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)


class BigqueryLineageExtractor:
    BQ_FILTER_RULE_TEMPLATE_V2 = """
resource.type=("bigquery_project")
AND
(
    protoPayload.methodName=
        (
            "google.cloud.bigquery.v2.JobService.Query"
            OR
            "google.cloud.bigquery.v2.JobService.InsertJob"
        )
    AND
    protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
    AND NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult:*
    AND (
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables:*
        OR
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedViews:*
    )
    AND (
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/_.*/tables/anon.*"
        AND
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/.*/tables/INFORMATION_SCHEMA.*"
        AND
        protoPayload.metadata.jobChange.job.jobStats.queryStats.referencedTables !~ "projects/.*/datasets/.*/tables/__TABLES__"
        AND
        protoPayload.metadata.jobChange.job.jobConfig.queryConfig.destinationTable !~ "projects/.*/datasets/_.*/tables/anon.*"

    )

)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip()

    def __init__(self, config: BigQueryV2Config, report: BigQueryV2Report):
        self.config = config
        self.report = report
        self.lineage_metadata: Dict[str, Set[str]] = defaultdict(set)
        self.loaded_project_ids: List[str] = []

    def error(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_failure(key, reason)
        log.error(f"{key} => {reason}")

    @staticmethod
    def bigquery_audit_metadata_query_template(
        dataset: str, use_date_sharded_tables: bool, limit: Optional[int] = None
    ) -> str:
        """
        Receives a dataset (with project specified) and returns a query template that is used to query exported
        AuditLogs containing protoPayloads of type BigQueryAuditMetadata.
        Include only those that:
        - have been completed (jobStatus.jobState = "DONE")
        - do not contain errors (jobStatus.errorResults is none)
        :param dataset: the dataset to query against in the form of $PROJECT.$DATASET
        :param use_date_sharded_tables: whether to read from date sharded audit log tables or time partitioned audit log
               tables
        :param limit: set a limit for the maximum event to return. It is used for connection testing currently
        :return: a query template, when supplied start_time and end_time, can be used to query audit logs from BigQuery
        """
        query: str
        if use_date_sharded_tables:
            query = (
                f"""
            SELECT
                timestamp,
                logName,
                insertId,
                protopayload_auditlog AS protoPayload,
                protopayload_auditlog.metadataJson AS metadata
            FROM
                `{dataset}.cloudaudit_googleapis_com_data_access_*`
            """
                + """
            WHERE
                _TABLE_SUFFIX BETWEEN "{start_time}" AND "{end_time}" AND
            """
            )
        else:
            query = f"""
            SELECT
                timestamp,
                logName,
                insertId,
                protopayload_auditlog AS protoPayload,
                protopayload_auditlog.metadataJson AS metadata
            FROM
                `{dataset}.cloudaudit_googleapis_com_data_access`
            WHERE
            """

        audit_log_filter = """    timestamp >= "{start_time}"
        AND timestamp < "{end_time}"
        AND protopayload_auditlog.serviceName="bigquery.googleapis.com"
        AND JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.jobState") = "DONE"
        AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobStatus.errorResults") IS NULL
        AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL
        """

        if limit is not None:
            audit_log_filter = audit_log_filter + f" LIMIT {limit}"
        query = textwrap.dedent(query) + audit_log_filter + ";"

        return textwrap.dedent(query)

    def compute_bigquery_lineage_via_gcp_logging(
        self, project_id: str
    ) -> Dict[str, Set[str]]:
        logger.info(f"Populating lineage info via GCP audit logs for {project_id}")
        try:
            clients: GCPLoggingClient = _make_gcp_logging_client(project_id)

            log_entries: Iterable[AuditLogEntry] = self._get_bigquery_log_entries(
                clients
            )
            logger.info("Log Entries loaded")
            parsed_entries: Iterable[QueryEvent] = self._parse_bigquery_log_entries(
                log_entries
            )
            return self._create_lineage_map(parsed_entries)
        except Exception as e:
            self.error(
                logger,
                "lineage-gcp-logs",
                f"Failed to get lineage gcp logging for {project_id}. The error message was {e}",
            )
            raise e

    def compute_bigquery_lineage_via_exported_bigquery_audit_metadata(
        self,
    ) -> Dict[str, Set[str]]:
        logger.info("Populating lineage info via exported GCP audit logs")
        try:
            # For exported logs we want to submit queries with the credentials project_id.
            _client: BigQueryClient = BigQueryClient()
            exported_bigquery_audit_metadata: Iterable[
                BigQueryAuditMetadata
            ] = self._get_exported_bigquery_audit_metadata(_client)
            parsed_entries: Iterable[
                QueryEvent
            ] = self._parse_exported_bigquery_audit_metadata(
                exported_bigquery_audit_metadata
            )
            return self._create_lineage_map(parsed_entries)
        except Exception as e:
            self.error(
                logger,
                "lineage-exported-gcp-audit-logs",
                f"Error: {e}",
            )
            raise e

    def _get_bigquery_log_entries(
        self, client: GCPLoggingClient, limit: Optional[int] = None
    ) -> Union[Iterable[AuditLogEntry], Iterable[BigQueryAuditMetadata]]:
        self.report.num_total_log_entries[client.project] = 0
        # Add a buffer to start and end time to account for delays in logging events.
        start_time = (self.config.start_time - self.config.max_query_duration).strftime(
            BQ_DATETIME_FORMAT
        )
        self.report.log_entry_start_time = start_time

        end_time = (self.config.end_time + self.config.max_query_duration).strftime(
            BQ_DATETIME_FORMAT
        )
        self.report.log_entry_end_time = end_time

        filter = self.BQ_FILTER_RULE_TEMPLATE_V2.format(
            start_time=start_time,
            end_time=end_time,
        )

        logger.info(
            f"Start loading log entries from BigQuery for {client.project} with start_time={start_time} and end_time={end_time}"
        )

        if self.config.rate_limit:
            with RateLimiter(max_calls=self.config.requests_per_min, period=60):
                entries = client.list_entries(
                    filter_=filter,
                    page_size=self.config.log_page_size,
                    max_results=limit,
                )
        else:
            entries = client.list_entries(
                filter_=filter, page_size=self.config.log_page_size, max_results=limit
            )

        logger.info(
            f"Start iterating over log entries from BigQuery for {client.project}"
        )
        for entry in entries:
            self.report.num_total_log_entries[client.project] += 1
            if self.report.num_total_log_entries[client.project] % 1000 == 0:
                logger.info(
                    f"{self.report.num_total_log_entries[client.project]} log entries loaded for project {client.project} so far..."
                )
            yield entry

        logger.info(
            f"Finished loading {self.report.num_total_log_entries[client.project]} log entries from BigQuery project {client.project} so far"
        )

    def _get_exported_bigquery_audit_metadata(
        self, bigquery_client: BigQueryClient, limit: Optional[int] = None
    ) -> Iterable[BigQueryAuditMetadata]:
        if self.config.bigquery_audit_metadata_datasets is None:
            self.error(
                logger, "audit-metadata", "bigquery_audit_metadata_datasets not set"
            )
            self.report.bigquery_audit_metadata_datasets_missing = True
            return

        start_time: str = (
            self.config.start_time - self.config.max_query_duration
        ).strftime(
            BQ_DATE_SHARD_FORMAT
            if self.config.use_date_sharded_audit_log_tables
            else BQ_DATETIME_FORMAT
        )
        self.report.audit_start_time = start_time

        end_time: str = (
            self.config.end_time + self.config.max_query_duration
        ).strftime(
            BQ_DATE_SHARD_FORMAT
            if self.config.use_date_sharded_audit_log_tables
            else BQ_DATETIME_FORMAT
        )
        self.report.audit_end_time = end_time

        for dataset in self.config.bigquery_audit_metadata_datasets:
            logger.info(
                f"Start loading log entries from BigQueryAuditMetadata in {dataset}"
            )

            query: str = self.bigquery_audit_metadata_query_template(
                dataset=dataset,
                use_date_sharded_tables=self.config.use_date_sharded_audit_log_tables,
            ).format(
                start_time=start_time,
                end_time=end_time,
            )

            if limit is not None:
                query = query + f" LIMIT {limit}"
            query_job = bigquery_client.query(query)

            logger.info(
                f"Finished loading log entries from BigQueryAuditMetadata in {dataset}"
            )

            if self.config.rate_limit:
                with RateLimiter(max_calls=self.config.requests_per_min, period=60):
                    yield from query_job
            else:
                yield from query_job

    # Currently we only parse JobCompleted events but in future we would want to parse other
    # events to also create field level lineage.
    def _parse_bigquery_log_entries(
        self,
        entries: Union[Iterable[AuditLogEntry], Iterable[BigQueryAuditMetadata]],
    ) -> Iterable[QueryEvent]:
        for entry in entries:
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
                self.error(
                    logger,
                    f"{entry.log_name}-{entry.insert_id}",
                    f"Unable to parse log missing {missing_entry}, missing v2 {missing_entry_v2} for {entry}",
                )
            else:
                self.report.num_parsed_log_entries[event.project_id] = (
                    self.report.num_parsed_log_entries.get(event.project_id, 0) + 1
                )
                yield event

    def _parse_exported_bigquery_audit_metadata(
        self, audit_metadata_rows: Iterable[BigQueryAuditMetadata]
    ) -> Iterable[QueryEvent]:
        for audit_metadata in audit_metadata_rows:
            event: Optional[QueryEvent] = None

            missing_exported_audit = (
                QueryEvent.get_missing_key_exported_bigquery_audit_metadata(
                    audit_metadata
                )
            )

            if missing_exported_audit is None:
                event = QueryEvent.from_exported_bigquery_audit_metadata(
                    audit_metadata, self.config.debug_include_full_payloads
                )

            if event is None:
                self.error(
                    logger,
                    f"{audit_metadata['logName']}-{audit_metadata['insertId']}",
                    f"Unable to parse audit metadata missing {missing_exported_audit} for {audit_metadata}",
                )
            else:
                self.report.num_parsed_audit_entries[event.project_id] = (
                    self.report.num_parsed_audit_entries.get(event.project_id, 0) + 1
                )
                self.report.num_total_audit_entries[event.project_id] = (
                    self.report.num_total_audit_entries.get(event.project_id, 0) + 1
                )
                yield event

    def _create_lineage_map(self, entries: Iterable[QueryEvent]) -> Dict[str, Set[str]]:
        logger.info("Entering create lineage map function")
        lineage_map: Dict[str, Set[str]] = collections.defaultdict(set)
        for e in entries:
            self.report.num_total_lineage_entries[e.project_id] = (
                self.report.num_total_lineage_entries.get(e.project_id, 0) + 1
            )

            if e.destinationTable is None or not (
                e.referencedTables or e.referencedViews
            ):
                self.report.num_skipped_lineage_entries_missing_data[e.project_id] = (
                    self.report.num_skipped_lineage_entries_missing_data.get(
                        e.project_id, 0
                    )
                    + 1
                )
                continue
            # Skip if schema/table pattern don't allow the destination table
            try:
                destination_table = e.destinationTable.get_sanitized_table_ref()
            except Exception:
                self.report.num_skipped_lineage_entries_missing_data[e.project_id] = (
                    self.report.num_skipped_lineage_entries_missing_data.get(
                        e.project_id, 0
                    )
                    + 1
                )
                continue

            destination_table_str = destination_table.table_identifier.get_table_name()
            destination_table_str = str(
                BigQueryTableRef(table_identifier=destination_table.table_identifier)
            )

            if not self.config.dataset_pattern.allowed(
                destination_table.table_identifier.dataset
            ) or not self.config.table_pattern.allowed(
                destination_table.table_identifier.get_table_name()
            ):
                self.report.num_skipped_lineage_entries_not_allowed[e.project_id] = (
                    self.report.num_skipped_lineage_entries_not_allowed.get(
                        e.project_id, 0
                    )
                    + 1
                )
                continue
            has_table = False
            for ref_table in e.referencedTables:
                ref_table_str = str(ref_table.get_sanitized_table_ref())
                if ref_table_str != destination_table_str:
                    lineage_map[destination_table_str].add(ref_table_str)
                    has_table = True
            has_view = False
            for ref_view in e.referencedViews:
                ref_view_str = str(ref_view.get_sanitized_table_ref())
                if ref_view_str != destination_table_str:
                    lineage_map[destination_table_str].add(ref_view_str)
                    has_view = True
            if self.config.lineage_use_sql_parser and has_table and has_view:
                # If there is a view being referenced then bigquery sends both the view as well as underlying table
                # in the references. There is no distinction between direct/base objects accessed. So doing sql parsing
                # to ensure we only use direct objects accessed for lineage
                try:
                    parser = BigQuerySQLParser(e.query)
                    referenced_objs = set(
                        map(lambda x: x.split(".")[-1], parser.get_tables())
                    )
                except Exception as ex:
                    logger.debug(
                        f"Sql Parser failed on query: {e.query}. It won't cause any issue except table/view lineage can't be detected reliably. The error was {ex}."
                    )
                    self.report.num_lineage_entries_sql_parser_failure[e.project_id] = (
                        self.report.num_lineage_entries_sql_parser_failure.get(
                            e.project_id, 0
                        )
                        + 1
                    )
                    continue
                curr_lineage_str = lineage_map[destination_table_str]
                new_lineage_str = set()
                for lineage_str in curr_lineage_str:
                    name = lineage_str.split("/")[-1]
                    if name in referenced_objs:
                        new_lineage_str.add(lineage_str)
                lineage_map[destination_table_str] = new_lineage_str
            if not (has_table or has_view):
                self.report.num_skipped_lineage_entries_other[e.project_id] = (
                    self.report.num_skipped_lineage_entries_other.get(e.project_id, 0)
                    + 1
                )

        logger.info("Exiting create lineage map function")
        return lineage_map

    def parse_view_lineage(
        self, project: str, dataset: str, view: BigqueryView
    ) -> List[BigqueryTableIdentifier]:
        parsed_tables = set()
        if view.ddl:
            try:
                parser = BigQuerySQLParser(view.ddl)
                tables = parser.get_tables()
            except Exception as ex:
                logger.debug(
                    f"View {view.name} definination sql parsing failed on query: {view.ddl}. Edge from physical table to view won't be added. The error was {ex}."
                )
                return []

            for table in tables:
                parts = table.split(".")
                if len(parts) == 1:
                    parsed_tables.add(
                        BigqueryTableIdentifier(
                            project_id=project, dataset=dataset, table=table
                        )
                    )
                elif len(parts) == 2:
                    parsed_tables.add(
                        BigqueryTableIdentifier(
                            project_id=project, dataset=parts[0], table=parts[1]
                        )
                    )
                elif len(parts) == 3:
                    parsed_tables.add(
                        BigqueryTableIdentifier(
                            project_id=parts[0], dataset=parts[1], table=parts[2]
                        )
                    )
                else:
                    continue

            return list(parsed_tables)
        else:
            return []

    def _compute_bigquery_lineage(self, project_id: str) -> Dict[str, Set[str]]:
        lineage_extractor: BigqueryLineageExtractor = BigqueryLineageExtractor(
            config=self.config, report=self.report
        )
        lineage_metadata: Dict[str, Set[str]]
        try:
            if self.config.use_exported_bigquery_audit_metadata:
                # Exported bigquery_audit_metadata should contain every projects' audit metada
                if self.loaded_project_ids:
                    return {}
                lineage_metadata = (
                    lineage_extractor.compute_bigquery_lineage_via_exported_bigquery_audit_metadata()
                )
            else:
                lineage_metadata = (
                    lineage_extractor.compute_bigquery_lineage_via_gcp_logging(
                        project_id
                    )
                )
        except Exception as e:
            if project_id:
                self.report.lineage_failed_extraction.append(project_id)
            logger.error(
                f"Unable to extract lineage for project {project_id} due to error {e}"
            )
            lineage_metadata = {}

        if lineage_metadata is None:
            lineage_metadata = {}

        self.report.lineage_mem_size[project_id] = humanfriendly.format_size(
            memory_footprint.total_size(lineage_metadata)
        )
        self.report.lineage_metadata_entries[project_id] = len(lineage_metadata)
        logger.info(f"Built lineage map containing {len(lineage_metadata)} entries.")
        logger.debug(f"lineage metadata is {lineage_metadata}")
        return lineage_metadata

    def get_upstream_tables(
        self, bq_table: str, tables_seen: List[str] = []
    ) -> Set[BigQueryTableRef]:
        upstreams: Set[BigQueryTableRef] = set()
        for ref_table in self.lineage_metadata[str(bq_table)]:
            upstream_table = BigQueryTableRef.from_string_name(ref_table)
            if upstream_table.is_temporary_table(
                [self.config.temp_table_dataset_prefix]
            ):
                # making sure we don't process a table twice and not get into a recursive loop
                if ref_table in tables_seen:
                    logger.debug(
                        f"Skipping table {ref_table} because it was seen already"
                    )
                    continue
                tables_seen.append(ref_table)
                if ref_table in self.lineage_metadata:
                    upstreams = upstreams.union(
                        self.get_upstream_tables(ref_table, tables_seen=tables_seen)
                    )
            else:
                upstreams.add(upstream_table)

        return upstreams

    def get_upstream_lineage_info(
        self,
        project_id: str,
        dataset_name: str,
        table: Union[BigqueryTable, BigqueryView],
        platform: str,
    ) -> Optional[Tuple[UpstreamLineageClass, Dict[str, str]]]:
        table_identifier = BigqueryTableIdentifier(project_id, dataset_name, table.name)

        if table_identifier.project_id not in self.loaded_project_ids:
            with PerfTimer() as timer:
                self.lineage_metadata.update(
                    self._compute_bigquery_lineage(table_identifier.project_id)
                )
                self.report.lineage_extraction_sec[table_identifier.project_id] = round(
                    timer.elapsed_seconds(), 2
                )
                self.loaded_project_ids.append(table_identifier.project_id)

        if self.config.lineage_parse_view_ddl and isinstance(table, BigqueryView):
            for table_id in self.parse_view_lineage(project_id, dataset_name, table):
                if table_identifier.get_table_name() in self.lineage_metadata:
                    self.lineage_metadata[
                        str(
                            BigQueryTableRef(table_identifier).get_sanitized_table_ref()
                        )
                    ].add(str(BigQueryTableRef(table_id).get_sanitized_table_ref()))
                else:
                    self.lineage_metadata[
                        str(
                            BigQueryTableRef(table_identifier).get_sanitized_table_ref()
                        )
                    ] = {str(BigQueryTableRef(table_id).get_sanitized_table_ref())}

        bq_table = BigQueryTableRef.from_bigquery_table(table_identifier)
        if str(bq_table) in self.lineage_metadata:
            upstream_list: List[UpstreamClass] = []
            # Sorting the list of upstream lineage events in order to avoid creating multiple aspects in backend
            # even if the lineage is same but the order is different.
            for upstream_table in sorted(
                self.get_upstream_tables(str(bq_table), tables_seen=[])
            ):
                upstream_table_class = UpstreamClass(
                    mce_builder.make_dataset_urn_with_platform_instance(
                        platform,
                        f"{upstream_table.table_identifier.project_id}.{upstream_table.table_identifier.dataset}.{upstream_table.table_identifier.table}",
                        self.config.platform_instance,
                        self.config.env,
                    ),
                    DatasetLineageTypeClass.TRANSFORMED,
                )
                if self.config.upstream_lineage_in_report:
                    current_lineage_map: Set = self.report.upstream_lineage.get(
                        str(bq_table), set()
                    )
                    current_lineage_map.add(str(upstream_table))
                    self.report.upstream_lineage[str(bq_table)] = current_lineage_map
                upstream_list.append(upstream_table_class)

            if upstream_list:
                upstream_lineage = UpstreamLineageClass(upstreams=upstream_list)
                return upstream_lineage, {}
        return None

    def test_capability(self, project_id: str) -> None:
        if self.config.use_exported_bigquery_audit_metadata:
            bigquery_client: BigQueryClient = BigQueryClient(project=project_id)
            entries = self._get_exported_bigquery_audit_metadata(
                bigquery_client=bigquery_client, limit=1
            )
            for entry in entries:
                logger.debug(
                    f"Connection test got one exported_bigquery_audit_metadata {entry}"
                )
        else:
            gcp_logging_client: GCPLoggingClient = _make_gcp_logging_client(
                project_id, self.config.extra_client_options
            )
            for entry in self._get_bigquery_log_entries(gcp_logging_client, limit=1):
                logger.debug(f"Connection test got one audit metadata entry {entry}")
