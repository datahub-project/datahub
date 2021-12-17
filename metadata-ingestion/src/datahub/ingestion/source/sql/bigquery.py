import collections
import functools
import logging
import textwrap
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union
from unittest.mock import patch

# This import verifies that the dependencies are available.
import pybigquery  # noqa: F401
import pybigquery.sqlalchemy_bigquery
import pydantic
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemyConfig,
    SQLAlchemySource,
    SqlWorkUnit,
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.ingestion.source.usage.bigquery_usage import (
    BQ_DATE_SHARD_FORMAT,
    BQ_DATETIME_FORMAT,
    AuditLogEntry,
    BigQueryAuditMetadata,
    BigQueryTableRef,
    QueryEvent,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.key import DatasetKey
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)

BQ_FILTER_RULE_TEMPLATE = """
protoPayload.serviceName="bigquery.googleapis.com"
AND
(
    (
        protoPayload.methodName="jobservice.jobcompleted"
        AND
        protoPayload.serviceData.jobCompletedEvent.eventName="query_job_completed"
        AND
        protoPayload.serviceData.jobCompletedEvent.job.jobStatus.state="DONE"
        AND NOT
        protoPayload.serviceData.jobCompletedEvent.job.jobStatus.error.code:*
    )
)
AND
timestamp >= "{start_time}"
AND
timestamp < "{end_time}"
""".strip()

# The existing implementation of this method can be found here:
# https://github.com/googleapis/python-bigquery-sqlalchemy/blob/e0f1496c99dd627e0ed04a0c4e89ca5b14611be2/pybigquery/sqlalchemy_bigquery.py#L967-L974.
# The existing implementation does not use the schema parameter and hence
# does not properly resolve the view definitions. As such, we must monkey
# patch the implementation.


def bigquery_audit_metadata_query_template(
    dataset: str, use_date_sharded_tables: bool
) -> str:
    """
    Receives a dataset (with project specified) and returns a query template that is used to query exported
    AuditLogs containing protoPayloads of type BigQueryAuditMetadata.
    :param dataset: the dataset to query against in the form of $PROJECT.$DATASET
    :param use_date_sharded_tables: whether to read from date sharded audit log tables or time partitioned audit log
           tables
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
            _TABLE_SUFFIX BETWEEN "{start_date}" AND "{end_date}" AND
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
    AND JSON_EXTRACT(protopayload_auditlog.metadataJson, "$.jobChange.job.jobConfig.queryConfig") IS NOT NULL;
    """

    query = textwrap.dedent(query) + audit_log_filter

    return textwrap.dedent(query)


def get_view_definition(self, connection, view_name, schema=None, **kw):
    view = self._get_table(connection, view_name, schema)
    return view.view_query


pybigquery.sqlalchemy_bigquery.BigQueryDialect.get_view_definition = get_view_definition

# Handle the GEOGRAPHY type. We will temporarily patch the _type_map
# in the get_workunits method of the source.
GEOGRAPHY = make_sqlalchemy_type("GEOGRAPHY")
register_custom_type(GEOGRAPHY)
assert pybigquery.sqlalchemy_bigquery._type_map


class BigQueryConfig(BaseTimeWindowConfig, SQLAlchemyConfig):
    scheme: str = "bigquery"
    project_id: Optional[str] = None

    log_page_size: Optional[pydantic.PositiveInt] = 1000
    # extra_client_options, include_table_lineage and max_query_duration are relevant only when computing the lineage.
    extra_client_options: Dict[str, Any] = {}
    include_table_lineage: Optional[bool] = True
    max_query_duration: timedelta = timedelta(minutes=15)

    bigquery_audit_metadata_datasets: Optional[List[str]] = None
    use_exported_bigquery_audit_metadata: bool = False
    use_date_sharded_audit_log_tables: bool = False

    def get_sql_alchemy_url(self):
        if self.project_id:
            return f"{self.scheme}://{self.project_id}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return f"{self.scheme}://"


class BigQuerySource(SQLAlchemySource):
    config: BigQueryConfig
    lineage_metadata: Optional[Dict[str, Set[str]]] = None

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "bigquery")

    def _compute_big_query_lineage(self) -> None:
        if self.config.include_table_lineage:
            if self.config.use_exported_bigquery_audit_metadata:
                self._compute_bigquery_lineage_via_exported_bigquery_audit_metadata()
            else:
                self._compute_bigquery_lineage_via_gcp_logging()

            if self.lineage_metadata is not None:
                logger.info(
                    f"Built lineage map containing {len(self.lineage_metadata)} entries."
                )

    def _compute_bigquery_lineage_via_gcp_logging(self) -> None:
        try:
            _clients: List[GCPLoggingClient] = self._make_bigquery_client()
            log_entries: Iterable[AuditLogEntry] = self._get_bigquery_log_entries(
                _clients
            )
            parsed_entries: Iterable[QueryEvent] = self._parse_bigquery_log_entries(
                log_entries
            )
            self.lineage_metadata = self._create_lineage_map(parsed_entries)
        except Exception as e:
            logger.error(
                "Error computing lineage information using GCP logs.",
                e,
            )

    def _compute_bigquery_lineage_via_exported_bigquery_audit_metadata(self) -> None:
        try:
            _client: BigQueryClient = BigQueryClient(project=self.config.project_id)
            exported_bigquery_audit_metadata: Iterable[
                BigQueryAuditMetadata
            ] = self._get_exported_bigquery_audit_metadata(_client)
            parsed_entries: Iterable[
                QueryEvent
            ] = self._parse_exported_bigquery_audit_metadata(
                exported_bigquery_audit_metadata
            )
            self.lineage_metadata = self._create_lineage_map(parsed_entries)
        except Exception as e:
            logger.error(
                "Error computing lineage information using exported GCP audit logs.",
                e,
            )

    def _make_bigquery_client(self) -> List[GCPLoggingClient]:
        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        client_options = self.config.extra_client_options.copy()
        client_options["_use_grpc"] = False
        project_id = self.config.project_id
        if project_id is not None:
            return [GCPLoggingClient(**client_options, project=project_id)]
        else:
            return [GCPLoggingClient(**client_options)]

    def _get_bigquery_log_entries(
        self, clients: List[GCPLoggingClient]
    ) -> Iterable[AuditLogEntry]:
        # Add a buffer to start and end time to account for delays in logging events.
        filter = BQ_FILTER_RULE_TEMPLATE.format(
            start_time=(
                self.config.start_time - self.config.max_query_duration
            ).strftime(BQ_DATETIME_FORMAT),
            end_time=(self.config.end_time + self.config.max_query_duration).strftime(
                BQ_DATETIME_FORMAT
            ),
        )

        logger.debug("Start loading log entries from BigQuery")
        for client in clients:
            yield from client.list_entries(
                filter_=filter, page_size=self.config.log_page_size
            )
        logger.debug("finished loading log entries from BigQuery")

    def _get_exported_bigquery_audit_metadata(
        self, bigquery_client: BigQueryClient
    ) -> Iterable[BigQueryAuditMetadata]:
        if self.config.bigquery_audit_metadata_datasets is None:
            return

        start_time: str = (
            self.config.start_time - self.config.max_query_duration
        ).strftime(BQ_DATETIME_FORMAT)
        end_time: str = (
            self.config.end_time + self.config.max_query_duration
        ).strftime(BQ_DATETIME_FORMAT)

        for dataset in self.config.bigquery_audit_metadata_datasets:
            logger.debug(
                f"Start loading log entries from BigQueryAuditMetadata in {dataset}"
            )

            query: str
            if self.config.use_date_sharded_audit_log_tables:
                start_date: str = (
                    self.config.start_time - self.config.max_query_duration
                ).strftime(BQ_DATE_SHARD_FORMAT)
                end_date: str = (
                    self.config.end_time + self.config.max_query_duration
                ).strftime(BQ_DATE_SHARD_FORMAT)

                query = bigquery_audit_metadata_query_template(
                    dataset, self.config.use_date_sharded_audit_log_tables
                ).format(
                    start_time=start_time,
                    end_time=end_time,
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                query = bigquery_audit_metadata_query_template(
                    dataset, self.config.use_date_sharded_audit_log_tables
                ).format(start_time=start_time, end_time=end_time)
            query_job = bigquery_client.query(query)

            logger.debug(
                f"Finished loading log entries from BigQueryAuditMetadata in {dataset}"
            )

            yield from query_job

    # Currently we only parse JobCompleted events but in future we would want to parse other
    # events to also create field level lineage.
    def _parse_bigquery_log_entries(
        self, entries: Iterable[AuditLogEntry]
    ) -> Iterable[QueryEvent]:
        for entry in entries:
            event: Optional[QueryEvent] = None
            try:
                if QueryEvent.can_parse_entry(entry):
                    event = QueryEvent.from_entry(entry)
                else:
                    raise RuntimeError("Unable to parse log entry as QueryEvent.")
            except Exception as e:
                self.report.report_failure(
                    f"{entry.log_name}-{entry.insert_id}",
                    f"unable to parse log entry: {entry!r}",
                )
                logger.error("Unable to parse GCP log entry.", e)
            if event is not None:
                yield event

    def _parse_exported_bigquery_audit_metadata(
        self, audit_metadata_rows: Iterable[BigQueryAuditMetadata]
    ) -> Iterable[QueryEvent]:
        for audit_metadata in audit_metadata_rows:
            event: Optional[QueryEvent] = None
            try:
                if QueryEvent.can_parse_exported_bigquery_audit_metadata(
                    audit_metadata
                ):
                    event = QueryEvent.from_exported_bigquery_audit_metadata(
                        audit_metadata
                    )
                else:
                    raise RuntimeError("Unable to parse log entry as QueryEvent.")
            except Exception as e:
                self.report.report_failure(
                    f"""{audit_metadata["logName"]}-{audit_metadata["insertId"]}""",
                    f"unable to parse log entry: {audit_metadata!r}",
                )
                logger.error("Unable to parse GCP log entry.", e)
            if event is not None:
                yield event

    def _create_lineage_map(self, entries: Iterable[QueryEvent]) -> Dict[str, Set[str]]:
        lineage_map: Dict[str, Set[str]] = collections.defaultdict(set)
        for e in entries:
            if (
                e.destinationTable is None
                or e.destinationTable.is_anonymous()
                or not e.referencedTables
            ):
                continue
            for ref_table in e.referencedTables:
                destination_table_str = str(e.destinationTable.remove_extras())
                ref_table_str = str(ref_table.remove_extras())
                if ref_table_str != destination_table_str:
                    lineage_map[destination_table_str].add(ref_table_str)
        return lineage_map

    @classmethod
    def create(cls, config_dict, ctx):
        config = BigQueryConfig.parse_obj(config_dict)
        return cls(config, ctx)

    # Overriding the get_workunits method to first compute the workunits using the base SQLAlchemySource
    # and then computing lineage information only for those datasets that were ingested. This helps us to
    # maintain a clear separation between SQLAlchemySource and the BigQuerySource. Also, this way we honor
    # that flags like schema and table patterns for lineage computation as well.
    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        # only compute the lineage if the object is none. This is is safety check in case if in future refactoring we
        # end up computing lineage multiple times.
        if self.lineage_metadata is None:
            self._compute_big_query_lineage()
        with patch.dict(
            "pybigquery.sqlalchemy_bigquery._type_map",
            {"GEOGRAPHY": GEOGRAPHY},
            clear=False,
        ):
            for wu in super().get_workunits():
                yield wu
                if (
                    isinstance(wu, SqlWorkUnit)
                    and isinstance(wu.metadata, MetadataChangeEvent)
                    and isinstance(wu.metadata.proposedSnapshot, DatasetSnapshot)
                ):
                    lineage_mcp = self.get_lineage_mcp(wu.metadata.proposedSnapshot.urn)
                    if lineage_mcp is not None:
                        lineage_wu = MetadataWorkUnit(
                            id=f"{self.platform}-{lineage_mcp.entityUrn}-{lineage_mcp.aspectName}",
                            mcp=lineage_mcp,
                        )
                        yield lineage_wu
                        self.report.report_workunit(lineage_wu)

    def get_lineage_mcp(
        self, dataset_urn: str
    ) -> Optional[MetadataChangeProposalWrapper]:
        if self.lineage_metadata is None:
            return None
        dataset_key: Optional[DatasetKey] = mce_builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            return None
        project_id, dataset_name, tablename = dataset_key.name.split(".")
        bq_table = BigQueryTableRef(project_id, dataset_name, tablename)
        if str(bq_table) in self.lineage_metadata:
            upstream_list: List[UpstreamClass] = []
            # Sorting the list of upstream lineage events in order to avoid creating multiple aspects in backend
            # even if the lineage is same but the order is different.
            for ref_table in sorted(self.lineage_metadata[str(bq_table)]):
                upstream_table = BigQueryTableRef.from_string_name(ref_table)
                upstream_table_class = UpstreamClass(
                    mce_builder.make_dataset_urn(
                        self.platform,
                        "{project}.{database}.{table}".format(
                            project=upstream_table.project,
                            database=upstream_table.dataset,
                            table=upstream_table.table,
                        ),
                        self.config.env,
                    ),
                    DatasetLineageTypeClass.TRANSFORMED,
                )
                upstream_list.append(upstream_table_class)

            if upstream_list:
                upstream_lineage = UpstreamLineageClass(upstreams=upstream_list)
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage,
                )
                return mcp
        return None

    def prepare_profiler_args(self, schema: str, table: str) -> dict:
        self.config: BigQueryConfig
        return dict(
            schema=self.config.project_id,
            table=f"{schema}.{table}",
        )

    @staticmethod
    @functools.lru_cache()
    def _get_project_id(inspector: Inspector) -> str:
        with inspector.bind.connect() as connection:
            project_id = connection.connection._client.project
            return project_id

    def get_identifier(
        self,
        *,
        schema: str,
        entity: str,
        inspector: Inspector,
        **kwargs: Any,
    ) -> str:
        assert inspector
        project_id = self._get_project_id(inspector)
        trimmed_table_name = (
            BigQueryTableRef.from_spec_obj(
                {"projectId": project_id, "datasetId": schema, "tableId": entity}
            )
            .remove_extras()
            .table
        )
        return f"{project_id}.{schema}.{trimmed_table_name}"

    def standardize_schema_table_names(
        self, schema: str, entity: str
    ) -> Tuple[str, str]:
        # The get_table_names() method of the BigQuery driver returns table names
        # formatted as "<schema>.<table>" as the table name. Since later calls
        # pass both schema and table, schema essentially is passed in twice. As
        # such, one of the schema names is incorrectly interpreted as the
        # project ID. By removing the schema from the table name, we avoid this
        # issue.
        segments = entity.split(".")
        if len(segments) != 2:
            raise ValueError(f"expected table to contain schema name already {entity}")
        if segments[0] != schema:
            raise ValueError(f"schema {schema} does not match table {entity}")
        return segments[0], segments[1]
