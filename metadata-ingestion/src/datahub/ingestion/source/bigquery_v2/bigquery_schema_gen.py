import logging
import re
from base64 import b32decode
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Type, Union, cast

from google.cloud.bigquery.table import TableListItem

from datahub.api.entities.platformresource.platform_resource import PlatformResource
from datahub.configuration.pattern_utils import is_schema_allowed, is_tag_allowed
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import BigQueryDatasetKey, ContainerKey, ProjectIdKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.glossary.classification_mixin import (
    SAMPLE_SIZE_MULTIPLIER,
    ClassificationHandler,
    classification_workunit_processor,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_data_reader import BigQueryDataReader
from datahub.ingestion.source.bigquery_v2.bigquery_helper import (
    unquote_and_decode_unicode_escape_seq,
)
from datahub.ingestion.source.bigquery_v2.bigquery_platform_resource_helper import (
    BigQueryLabel,
    BigQueryLabelInfo,
    BigQueryPlatformResourceHelper,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigqueryColumn,
    BigqueryDataset,
    BigqueryProject,
    BigQuerySchemaApi,
    BigqueryTable,
    BigqueryTableConstraint,
    BigqueryTableSnapshot,
    BigqueryView,
)
from datahub.ingestion.source.bigquery_v2.common import (
    BQ_EXTERNAL_DATASET_URL_TEMPLATE,
    BQ_EXTERNAL_TABLE_URL_TEMPLATE,
    BigQueryIdentifierBuilder,
)
from datahub.ingestion.source.bigquery_v2.profiler import BigqueryProfiler
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_schema_container,
    get_domain_wu,
)
from datahub.ingestion.source_report.ingestion_stage import (
    METADATA_EXTRACTION,
    PROFILING,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProperties,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    DateType,
    ForeignKeyConstraint,
    MySqlDDL,
    NullType,
    NumberType,
    RecordType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    TimeType,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    GlobalTagsClass,
    TagAssociationClass,
)
from datahub.metadata.urns import TagUrn
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.hive_schema_to_avro import (
    HiveColumnToAvroConverter,
    get_schema_fields_for_hive_column,
)
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.ratelimiter import RateLimiter
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

ENCODED_TAG_PREFIX = "urn_li_encoded_tag_"

logger: logging.Logger = logging.getLogger(__name__)
# Handle table snapshots
# See https://cloud.google.com/bigquery/docs/table-snapshots-intro.
SNAPSHOT_TABLE_REGEX = re.compile(r"^(.+)@(\d{13})$")
CLUSTERING_COLUMN_TAG = "CLUSTERING_COLUMN"


class BigQuerySchemaGenerator:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    # Note: We use the hive schema parser to parse nested BigQuery types. We also have
    # some extra type mappings in that file.
    BIGQUERY_FIELD_TYPE_MAPPINGS: Dict[
        str,
        Type[
            Union[
                ArrayType,
                BytesType,
                BooleanType,
                NumberType,
                RecordType,
                StringType,
                TimeType,
                DateType,
                NullType,
            ]
        ],
    ] = {
        "BYTES": BytesType,
        "BOOL": BooleanType,
        "INT": NumberType,
        "INT64": NumberType,
        "SMALLINT": NumberType,
        "INTEGER": NumberType,
        "BIGINT": NumberType,
        "TINYINT": NumberType,
        "BYTEINT": NumberType,
        "BIGNUMERIC": NumberType,
        "NUMERIC": NumberType,
        "DECIMAL": NumberType,
        "BIGDECIMAL": NumberType,
        "FLOAT64": NumberType,
        "RANGE": NullType,
        "STRING": StringType,
        "TIME": TimeType,
        "TIMESTAMP": TimeType,
        "DATE": DateType,
        "DATETIME": TimeType,
        "GEOGRAPHY": NullType,
        "JSON": RecordType,
        "INTERVAL": NullType,
        "ARRAY": ArrayType,
        "STRUCT": RecordType,
    }

    def __init__(
        self,
        config: BigQueryV2Config,
        report: BigQueryV2Report,
        bigquery_data_dictionary: BigQuerySchemaApi,
        domain_registry: Optional[DomainRegistry],
        sql_parser_schema_resolver: SchemaResolver,
        profiler: BigqueryProfiler,
        identifiers: BigQueryIdentifierBuilder,
        graph: Optional[DataHubGraph] = None,
    ):
        self.config = config
        self.report = report
        self.schema_api = bigquery_data_dictionary
        self.domain_registry = domain_registry
        self.sql_parser_schema_resolver = sql_parser_schema_resolver
        self.profiler = profiler
        self.identifiers = identifiers
        self.graph = graph

        self.classification_handler = ClassificationHandler(self.config, self.report)
        self.data_reader: Optional[BigQueryDataReader] = None
        if self.classification_handler.is_classification_enabled():
            self.data_reader = BigQueryDataReader.create(
                self.config.get_bigquery_client()
            )

        # Global store of table identifiers for lineage filtering
        self.table_refs: Set[str] = set()

        # Maps project -> view_ref, so we can find all views in a project
        self.view_refs_by_project: Dict[str, Set[str]] = defaultdict(set)
        # Maps project -> snapshot_ref, so we can find all snapshots in a project
        self.snapshot_refs_by_project: Dict[str, Set[str]] = defaultdict(set)
        # Maps view ref -> actual sql
        self.view_definitions: FileBackedDict[str] = FileBackedDict()
        # Maps snapshot ref -> Snapshot
        self.snapshots_by_ref: FileBackedDict[BigqueryTableSnapshot] = FileBackedDict()
        # Add External BQ table
        self.external_tables: Dict[str, BigqueryTable] = defaultdict()
        self.bq_external_table_pattern = (
            r".*create\s+external\s+table\s+`?(?:project_id\.)?.*`?"
        )

        bq_project = (
            self.config.project_on_behalf
            if self.config.project_on_behalf
            else self.config.credential.project_id
            if self.config.credential
            else None
        )

        self.platform_resource_helper: BigQueryPlatformResourceHelper = (
            BigQueryPlatformResourceHelper(
                bq_project,
                self.graph,
            )
        )

    @property
    def store_table_refs(self):
        return (
            self.config.include_table_lineage
            or self.config.include_usage_statistics
            or self.config.use_queries_v2
        )

    def modified_base32decode(self, text_to_decode: str) -> str:
        # When we sync from DataHub to BigQuery, we encode the tags as modified base32 strings.
        # BiqQuery labels only support lowercase letters, international characters, numbers, or underscores.
        # So we need to modify the base32 encoding to replace the padding character `=` with `_` and convert to lowercase.
        if not text_to_decode.startswith("%s" % ENCODED_TAG_PREFIX):
            return text_to_decode
        text_to_decode = (
            text_to_decode.replace(ENCODED_TAG_PREFIX, "").upper().replace("_", "=")
        )
        text = b32decode(text_to_decode.encode("utf-8")).decode("utf-8")
        return text

    def get_project_workunits(
        self, project: BigqueryProject
    ) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage(f"{project.id}: {METADATA_EXTRACTION}"):
            logger.info(f"Processing project: {project.id}")
            yield from self._process_project(project)

    def get_dataplatform_instance_aspect(
        self, dataset_urn: str, project_id: str
    ) -> MetadataWorkUnit:
        aspect = DataPlatformInstanceClass(
            platform=self.identifiers.make_data_platform_urn(),
            instance=self.identifiers.make_dataplatform_instance_urn(project_id),
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=aspect
        ).as_workunit()

    def gen_dataset_key(self, db_name: str, schema: str) -> ContainerKey:
        return BigQueryDatasetKey(
            project_id=db_name,
            dataset_id=schema,
            platform=self.identifiers.platform,
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

    def gen_project_id_key(self, database: str) -> ContainerKey:
        return ProjectIdKey(
            project_id=database,
            platform=self.identifiers.platform,
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

    def gen_project_id_containers(self, database: str) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.gen_project_id_key(database)

        yield from gen_database_container(
            database=database,
            name=database,
            sub_types=[DatasetContainerSubTypes.BIGQUERY_PROJECT],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            database_container_key=database_container_key,
        )

    def gen_dataset_containers(
        self,
        dataset: str,
        project_id: str,
        tags: Optional[Dict[str, str]] = None,
        extra_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = self.gen_dataset_key(project_id, dataset)

        tags_joined: List[str] = []
        if tags and self.config.capture_dataset_label_as_tag:
            for k, v in tags.items():
                if is_tag_allowed(self.config.capture_dataset_label_as_tag, k):
                    tag_urn = TagUrn.from_string(self.make_tag_urn_from_label(k, v))
                    label = BigQueryLabel(key=k, value=v)
                    try:
                        platform_resource: PlatformResource = self.platform_resource_helper.generate_label_platform_resource(
                            label, tag_urn, managed_by_datahub=False
                        )
                        label_info: BigQueryLabelInfo = (
                            platform_resource.resource_info.value.as_pydantic_object(  # type: ignore
                                BigQueryLabelInfo
                            )
                        )
                        tag_urn = TagUrn.from_string(label_info.datahub_urn)

                        for mcpw in platform_resource.to_mcps():
                            yield mcpw.as_workunit()
                    except ValueError as e:
                        logger.warning(
                            f"Failed to generate platform resource for label {k}:{v}: {e}"
                        )
                    tags_joined.append(tag_urn.name)

        database_container_key = self.gen_project_id_key(database=project_id)

        yield from gen_schema_container(
            database=project_id,
            schema=dataset,
            sub_types=[DatasetContainerSubTypes.BIGQUERY_DATASET],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            schema_container_key=schema_container_key,
            database_container_key=database_container_key,
            external_url=(
                BQ_EXTERNAL_DATASET_URL_TEMPLATE.format(
                    project=project_id, dataset=dataset
                )
                if self.config.include_external_url
                else None
            ),
            tags=tags_joined,
            extra_properties=extra_properties,
        )

    def _process_project(
        self, bigquery_project: BigqueryProject
    ) -> Iterable[MetadataWorkUnit]:
        db_tables: Dict[str, List[BigqueryTable]] = {}

        project_id = bigquery_project.id
        try:
            bigquery_project.datasets = self.schema_api.get_datasets_for_project_id(
                project_id
            )
        except Exception as e:
            if self.config.project_ids and "not enabled BigQuery." in str(e):
                action_mesage = (
                    "The project has not enabled BigQuery API. "
                    "Did you mistype project id in recipe ?"
                )
            else:
                action_mesage = (
                    "Does your service account have `bigquery.datasets.get` permission ? "
                    "Assign predefined role `roles/bigquery.metadataViewer` to your service account."
                )

            self.report.failure(
                title="Unable to get datasets for project",
                message=action_mesage,
                context=project_id,
                exc=e,
            )
            return None

        if len(bigquery_project.datasets) == 0:
            action_message = (
                "Either there are no datasets in this project or missing `bigquery.datasets.get` permission. "
                "You can assign predefined roles/bigquery.metadataViewer role to your service account."
            )
            if self.config.exclude_empty_projects:
                self.report.report_dropped(project_id)
                logger.info(
                    f"Excluded project '{project_id}' since no datasets were found. {action_message}"
                )
            else:
                if self.config.include_schema_metadata:
                    yield from self.gen_project_id_containers(project_id)
                self.report.warning(
                    title="No datasets found in project",
                    message=action_message,
                    context=project_id,
                )
            return

        if self.config.include_schema_metadata:
            yield from self.gen_project_id_containers(project_id)

        self.report.num_project_datasets_to_scan[project_id] = len(
            bigquery_project.datasets
        )
        yield from self._process_project_datasets(bigquery_project, db_tables)

        if self.config.is_profiling_enabled():
            logger.info(f"Starting profiling project {project_id}")
            with self.report.new_stage(f"{project_id}: {PROFILING}"):
                yield from self.profiler.get_workunits(
                    project_id=project_id,
                    tables=db_tables,
                )

    def _process_project_datasets(
        self,
        bigquery_project: BigqueryProject,
        db_tables: Dict[str, List[BigqueryTable]],
    ) -> Iterable[MetadataWorkUnit]:
        db_views: Dict[str, List[BigqueryView]] = {}
        db_snapshots: Dict[str, List[BigqueryTableSnapshot]] = {}
        project_id = bigquery_project.id

        def _process_schema_worker(
            bigquery_dataset: BigqueryDataset,
        ) -> Iterable[MetadataWorkUnit]:
            if not is_schema_allowed(
                self.config.dataset_pattern,
                bigquery_dataset.name,
                project_id,
                self.config.match_fully_qualified_names,
            ):
                self.report.report_dropped(f"{bigquery_dataset.name}.*")
                return
            try:
                # db_tables, db_views, and db_snapshots are populated in the this method
                for wu in self._process_schema(
                    project_id, bigquery_dataset, db_tables, db_views, db_snapshots
                ):
                    yield wu
            except Exception as e:
                if self.config.is_profiling_enabled():
                    action_mesage = "Does your service account have bigquery.tables.list, bigquery.routines.get, bigquery.routines.list permission, bigquery.tables.getData permission?"
                else:
                    action_mesage = "Does your service account have bigquery.tables.list, bigquery.routines.get, bigquery.routines.list permission?"

                self.report.failure(
                    title="Unable to get tables for dataset",
                    message=action_mesage,
                    context=f"{project_id}.{bigquery_dataset.name}",
                    exc=e,
                )

        for wu in ThreadedIteratorExecutor.process(
            worker_func=_process_schema_worker,
            args_list=[(bq_dataset,) for bq_dataset in bigquery_project.datasets],
            max_workers=self.config.max_threads_dataset_parallelism,
        ):
            yield wu

    def _process_schema(
        self,
        project_id: str,
        bigquery_dataset: BigqueryDataset,
        db_tables: Dict[str, List[BigqueryTable]],
        db_views: Dict[str, List[BigqueryView]],
        db_snapshots: Dict[str, List[BigqueryTableSnapshot]],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = bigquery_dataset.name

        if self.config.include_schema_metadata:
            yield from self.gen_dataset_containers(
                dataset_name,
                project_id,
                bigquery_dataset.labels,
                (
                    {"location": bigquery_dataset.location}
                    if bigquery_dataset.location
                    else None
                ),
            )

        columns = None
        constraints: Optional[Dict[str, List[BigqueryTableConstraint]]] = None

        rate_limiter: Optional[RateLimiter] = None
        if self.config.rate_limit:
            rate_limiter = RateLimiter(
                max_calls=self.config.requests_per_min, period=60
            )

        if self.config.include_schema_metadata:
            columns = self.schema_api.get_columns_for_dataset(
                project_id=project_id,
                dataset_name=dataset_name,
                column_limit=self.config.column_limit,
                run_optimized_column_query=self.config.run_optimized_column_query,
                extract_policy_tags_from_catalog=self.config.extract_policy_tags_from_catalog,
                report=self.report,
                rate_limiter=rate_limiter,
            )
            if (
                self.config.include_table_constraints
                and bigquery_dataset.supports_table_constraints()
            ):
                constraints = self.schema_api.get_table_constraints_for_dataset(
                    project_id=project_id, dataset_name=dataset_name, report=self.report
                )
        elif self.store_table_refs:
            # Need table_refs to calculate lineage and usage
            for table_item in self.schema_api.list_tables(dataset_name, project_id):
                identifier = BigqueryTableIdentifier(
                    project_id=project_id,
                    dataset=dataset_name,
                    table=table_item.table_id,
                )
                if not self.config.table_pattern.allowed(identifier.raw_table_name()):
                    self.report.report_dropped(identifier.raw_table_name())
                    continue
                try:
                    self.table_refs.add(
                        str(BigQueryTableRef(identifier).get_sanitized_table_ref())
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not create table ref for {table_item.path}: {e}"
                    )
            return

        if self.config.include_tables:
            db_tables[dataset_name] = list(
                self.get_tables_for_dataset(project_id, bigquery_dataset)
            )

            for table in db_tables[dataset_name]:
                table_columns = columns.get(table.name, []) if columns else []
                table_constraints = (
                    constraints.get(table.name, []) if constraints else []
                )

                table.constraints = table_constraints
                table_wu_generator = self._process_table(
                    table=table,
                    columns=table_columns,
                    project_id=project_id,
                    dataset_name=dataset_name,
                )
                yield from classification_workunit_processor(
                    table_wu_generator,
                    self.classification_handler,
                    self.data_reader,
                    [project_id, dataset_name, table.name],
                    data_reader_kwargs=dict(
                        sample_size_percent=(
                            self.config.classification.sample_size
                            * SAMPLE_SIZE_MULTIPLIER
                            / table.rows_count
                            if table.rows_count
                            else None
                        )
                    ),
                )

        if self.config.include_views:
            db_views[dataset_name] = list(
                self.schema_api.get_views_for_dataset(
                    project_id,
                    dataset_name,
                    self.config.is_profiling_enabled(),
                    self.report,
                )
            )

            for view in db_views[dataset_name]:
                view_columns = columns.get(view.name, []) if columns else []
                yield from self._process_view(
                    view=view,
                    columns=view_columns,
                    project_id=project_id,
                    dataset_name=dataset_name,
                )

        if self.config.include_table_snapshots:
            db_snapshots[dataset_name] = list(
                self.schema_api.get_snapshots_for_dataset(
                    project_id,
                    dataset_name,
                    self.config.is_profiling_enabled(),
                    self.report,
                )
            )

            for snapshot in db_snapshots[dataset_name]:
                snapshot_columns = columns.get(snapshot.name, []) if columns else []
                yield from self._process_snapshot(
                    snapshot=snapshot,
                    columns=snapshot_columns,
                    project_id=project_id,
                    dataset_name=dataset_name,
                )

    def _process_table(
        self,
        table: BigqueryTable,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(project_id, dataset_name, table.name)

        self.report.report_entity_scanned(table_identifier.raw_table_name())

        if not self.config.table_pattern.allowed(table_identifier.raw_table_name()):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        if self.store_table_refs:
            self.table_refs.add(
                str(BigQueryTableRef(table_identifier).get_sanitized_table_ref())
            )
        table.column_count = len(columns)

        if not table.column_count:
            logger.warning(
                f"Table doesn't have any column or unable to get columns for table: {table_identifier}"
            )

        # If table has time partitioning, set the data type of the partitioning field
        if table.partition_info:
            table.partition_info.column = next(
                (
                    column
                    for column in columns
                    if column.name == table.partition_info.field
                ),
                None,
            )
        yield from self.gen_table_dataset_workunits(
            table, columns, project_id, dataset_name
        )

    def _process_view(
        self,
        view: BigqueryView,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(project_id, dataset_name, view.name)

        self.report.report_entity_scanned(table_identifier.raw_table_name(), "view")

        if not self.config.view_pattern.allowed(table_identifier.raw_table_name()):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        table_ref = str(BigQueryTableRef(table_identifier).get_sanitized_table_ref())
        self.table_refs.add(table_ref)
        if view.view_definition:
            self.view_refs_by_project[project_id].add(table_ref)
            self.view_definitions[table_ref] = view.view_definition

        view.column_count = len(columns)
        if not view.column_count:
            logger.warning(
                f"View doesn't have any column or unable to get columns for view: {table_identifier}"
            )

        yield from self.gen_view_dataset_workunits(
            table=view,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
        )

    def _process_snapshot(
        self,
        snapshot: BigqueryTableSnapshot,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = BigqueryTableIdentifier(
            project_id, dataset_name, snapshot.name
        )

        self.report.snapshots_scanned += 1

        if not self.config.table_snapshot_pattern.allowed(
            table_identifier.raw_table_name()
        ):
            self.report.report_dropped(table_identifier.raw_table_name())
            return

        snapshot.columns = columns
        snapshot.column_count = len(columns)
        if not snapshot.column_count:
            logger.warning(
                f"Snapshot doesn't have any column or unable to get columns for snapshot: {table_identifier}"
            )

        table_ref = str(BigQueryTableRef(table_identifier).get_sanitized_table_ref())
        self.table_refs.add(table_ref)
        if snapshot.base_table_identifier:
            self.snapshot_refs_by_project[project_id].add(table_ref)
            self.snapshots_by_ref[table_ref] = snapshot

        yield from self.gen_snapshot_dataset_workunits(
            table=snapshot,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
        )

    def make_tag_urn_from_label(self, key: str, value: str) -> str:
        if value:
            return make_tag_urn(f"""{key}:{value}""")
        else:
            return make_tag_urn(key)

    # New method to generate ForeignKeyConstraint aspects
    def gen_foreign_keys(
        self,
        table: BigqueryTable,
        dataset_name: str,
        project_id: str,
    ) -> Iterable[ForeignKeyConstraint]:
        table_id = f"{project_id}.{dataset_name}.{table.name}"
        foreign_keys: List[BigqueryTableConstraint] = list(
            filter(lambda x: x.type == "FOREIGN KEY", table.constraints)
        )
        for key, group in groupby_unsorted(
            foreign_keys,
            lambda x: f"{x.referenced_project_id}.{x.referenced_dataset}.{x.referenced_table_name}",
        ):
            dataset_urn = make_dataset_urn_with_platform_instance(
                platform="bigquery",
                name=table_id,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            foreign_dataset = make_dataset_urn_with_platform_instance(
                platform="bigquery",
                name=key,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            source_fields: List[str] = list()
            referenced_fields: List[str] = list()

            for item in group:
                source_field = make_schema_field_urn(
                    parent_urn=dataset_urn, field_path=item.field_path
                )
                assert item.referenced_column_name
                referenced_field = make_schema_field_urn(
                    parent_urn=foreign_dataset, field_path=item.referenced_column_name
                )

                source_fields.append(source_field)
                referenced_fields.append(referenced_field)

            foreign_key_aspect = ForeignKeyConstraint(
                name=key,
                foreignFields=referenced_fields,
                sourceFields=source_fields,
                foreignDataset=foreign_dataset,
            )

            yield foreign_key_aspect

    def gen_table_dataset_workunits(
        self,
        table: BigqueryTable,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties: Dict[str, str] = {}
        if table.expires:
            custom_properties["expiration_date"] = str(table.expires)

        if table.partition_info:
            custom_properties["partition_info"] = str(table.partition_info)

        if table.size_in_bytes:
            custom_properties["size_in_bytes"] = str(table.size_in_bytes)

        if table.active_billable_bytes:
            custom_properties["billable_bytes_active"] = str(
                table.active_billable_bytes
            )

        if table.long_term_billable_bytes:
            custom_properties["billable_bytes_long_term"] = str(
                table.long_term_billable_bytes
            )

        if table.max_partition_id:
            custom_properties["number_of_partitions"] = str(table.num_partitions)
            custom_properties["max_partition_id"] = str(table.max_partition_id)
            custom_properties["is_partitioned"] = str(True)

        sub_types: List[str] = [DatasetSubTypes.TABLE]
        if table.max_shard_id:
            custom_properties["max_shard_id"] = str(table.max_shard_id)
            custom_properties["is_sharded"] = str(True)
            sub_types = [DatasetSubTypes.SHARDED_TABLE] + sub_types
        if table.external:
            sub_types = [DatasetSubTypes.EXTERNAL_TABLE] + sub_types

        tags_to_add = None
        if table.labels and self.config.capture_table_label_as_tag:
            tags_to_add = []
            for k, v in table.labels.items():
                if is_tag_allowed(self.config.capture_table_label_as_tag, k):
                    tag_urn = TagUrn.from_string(self.make_tag_urn_from_label(k, v))
                    try:
                        label = BigQueryLabel(key=k, value=v)
                        platform_resource: PlatformResource = self.platform_resource_helper.generate_label_platform_resource(
                            label, tag_urn, managed_by_datahub=False
                        )
                        label_info: BigQueryLabelInfo = (
                            platform_resource.resource_info.value.as_pydantic_object(  # type: ignore
                                BigQueryLabelInfo
                            )
                        )
                        tag_urn = TagUrn.from_string(label_info.datahub_urn)

                        for mcpw in platform_resource.to_mcps():
                            yield mcpw.as_workunit()
                    except ValueError as e:
                        logger.warning(
                            f"Failed to generate platform resource for label {k}:{v}: {e}"
                        )
                    tags_to_add.append(tag_urn.urn())

        yield from self.gen_dataset_workunits(
            table=table,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
            sub_types=sub_types,
            tags_to_add=tags_to_add,
            custom_properties=custom_properties,
        )

    def gen_view_dataset_workunits(
        self,
        table: BigqueryView,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        tags_to_add = []
        if table.labels and self.config.capture_view_label_as_tag:
            for k, v in table.labels.items():
                if is_tag_allowed(self.config.capture_view_label_as_tag, k):
                    tag_urn = TagUrn.from_string(self.make_tag_urn_from_label(k, v))
                    try:
                        label = BigQueryLabel(key=k, value=v)
                        platform_resource: PlatformResource = self.platform_resource_helper.generate_label_platform_resource(
                            label, tag_urn, managed_by_datahub=False
                        )
                        label_info: BigQueryLabelInfo = (
                            platform_resource.resource_info.value.as_pydantic_object(  # type: ignore
                                BigQueryLabelInfo
                            )
                        )
                        tag_urn = TagUrn.from_string(label_info.datahub_urn)

                        for mcpw in platform_resource.to_mcps():
                            yield mcpw.as_workunit()
                    except ValueError as e:
                        logger.warning(
                            f"Failed to generate platform resource for label {k}:{v}: {e}"
                        )

                    tags_to_add.append(tag_urn.urn())
        yield from self.gen_dataset_workunits(
            table=table,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
            tags_to_add=tags_to_add,
            sub_types=[DatasetSubTypes.VIEW],
        )

        view = cast(BigqueryView, table)
        view_definition_string = view.view_definition
        view_properties_aspect = ViewProperties(
            materialized=view.materialized,
            viewLanguage="SQL",
            viewLogic=view_definition_string or "",
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=self.identifiers.gen_dataset_urn(
                project_id, dataset_name, table.name
            ),
            aspect=view_properties_aspect,
        ).as_workunit()

    def gen_snapshot_dataset_workunits(
        self,
        table: BigqueryTableSnapshot,
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties: Dict[str, str] = {}
        if table.ddl:
            custom_properties["snapshot_ddl"] = table.ddl
        if table.snapshot_time:
            custom_properties["snapshot_time"] = str(table.snapshot_time)
        if table.size_in_bytes:
            custom_properties["size_in_bytes"] = str(table.size_in_bytes)
        if table.rows_count:
            custom_properties["rows_count"] = str(table.rows_count)
        yield from self.gen_dataset_workunits(
            table=table,
            columns=columns,
            project_id=project_id,
            dataset_name=dataset_name,
            sub_types=[DatasetSubTypes.BIGQUERY_TABLE_SNAPSHOT],
            custom_properties=custom_properties,
        )

    def gen_dataset_workunits(
        self,
        table: Union[BigqueryTable, BigqueryView, BigqueryTableSnapshot],
        columns: List[BigqueryColumn],
        project_id: str,
        dataset_name: str,
        sub_types: List[str],
        tags_to_add: Optional[List[str]] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self.identifiers.gen_dataset_urn(
            project_id, dataset_name, table.name
        )

        # Added for bigquery to gcs lineage extraction
        if (
            isinstance(table, BigqueryTable)
            and table.table_type == "EXTERNAL"
            and table.ddl is not None
            and re.search(self.bq_external_table_pattern, table.ddl, re.IGNORECASE)
        ):
            self.external_tables[dataset_urn] = table

        status = Status(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=status
        ).as_workunit()

        datahub_dataset_name = BigqueryTableIdentifier(
            project_id, dataset_name, table.name
        )

        yield self.gen_schema_metadata(
            dataset_urn, table, columns, datahub_dataset_name
        )

        dataset_properties = DatasetProperties(
            name=datahub_dataset_name.get_table_display_name(),
            description=(
                unquote_and_decode_unicode_escape_seq(table.comment)
                if table.comment
                else ""
            ),
            qualifiedName=str(datahub_dataset_name),
            created=(
                TimeStamp(time=int(table.created.timestamp() * 1000))
                if table.created is not None
                else None
            ),
            lastModified=(
                TimeStamp(time=int(table.last_altered.timestamp() * 1000))
                if table.last_altered is not None
                else None
            ),
            externalUrl=(
                BQ_EXTERNAL_TABLE_URL_TEMPLATE.format(
                    project=project_id, dataset=dataset_name, table=table.name
                )
                if self.config.include_external_url
                else None
            ),
        )
        if custom_properties:
            dataset_properties.customProperties.update(custom_properties)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

        if tags_to_add:
            yield self.gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=self.gen_dataset_key(project_id, dataset_name),
        )
        yield self.get_dataplatform_instance_aspect(
            dataset_urn=dataset_urn, project_id=project_id
        )

        subTypes = SubTypes(typeNames=sub_types)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=subTypes
        ).as_workunit()

        if self.domain_registry:
            yield from get_domain_wu(
                dataset_name=str(datahub_dataset_name),
                entity_urn=dataset_urn,
                domain_registry=self.domain_registry,
                domain_config=self.config.domain,
            )

    def gen_tags_aspect_workunit(
        self, dataset_urn: str, tags_to_add: List[str]
    ) -> MetadataWorkUnit:
        tags = GlobalTagsClass(
            tags=[TagAssociationClass(tag_to_add) for tag_to_add in tags_to_add]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=tags
        ).as_workunit()

    def is_primary_key(
        self, field_path: str, constraints: List[BigqueryTableConstraint]
    ) -> bool:
        for constraint in constraints:
            if constraint.field_path == field_path and constraint.type == "PRIMARY KEY":
                return True
        return False

    def gen_schema_fields(
        self, columns: List[BigqueryColumn], constraints: List[BigqueryTableConstraint]
    ) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []

        # Below line affects HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR in global scope
        # TODO: Refractor this such that
        # converter = HiveColumnToAvroConverter(struct_type_separator=" ");
        # converter.get_schema_fields_for_hive_column(...)
        original_struct_type_separator = (
            HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR
        )
        HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR = " "
        _COMPLEX_TYPE = re.compile("^(struct|array)")
        last_id = -1
        for col in columns:
            # if col.data_type is empty that means this column is part of a complex type
            if col.data_type is None or _COMPLEX_TYPE.match(col.data_type.lower()):
                # If the we have seen the ordinal position that most probably means we already processed this complex type
                if last_id != col.ordinal_position:
                    schema_fields.extend(
                        get_schema_fields_for_hive_column(
                            col.name, col.data_type.lower(), description=col.comment
                        )
                    )

                # We have to add complex type comments to the correct level
                if col.comment:
                    for idx, field in enumerate(schema_fields):
                        # Remove all the [version=2.0].[type=struct]. tags to get the field path
                        if (
                            re.sub(
                                r"\[.*?\]\.",
                                repl="",
                                string=field.fieldPath.lower(),
                                count=0,
                                flags=re.MULTILINE,
                            )
                            == col.field_path.lower()
                        ):
                            field.description = col.comment
                            schema_fields[idx] = field
                            break
            else:
                tags = []
                if col.cluster_column_position is not None:
                    tags.append(
                        TagAssociationClass(
                            make_tag_urn(
                                f"{CLUSTERING_COLUMN_TAG}_{col.cluster_column_position}"
                            )
                        )
                    )

                if col.policy_tags:
                    for policy_tag in col.policy_tags:
                        tags.append(TagAssociationClass(make_tag_urn(policy_tag)))
                field = SchemaField(
                    fieldPath=col.name,
                    type=SchemaFieldDataType(
                        self.BIGQUERY_FIELD_TYPE_MAPPINGS.get(col.data_type, NullType)()
                    ),
                    isPartitioningKey=col.is_partition_column,
                    isPartOfKey=self.is_primary_key(col.field_path, constraints),
                    nativeDataType=col.data_type,
                    description=col.comment,
                    nullable=col.is_nullable,
                    globalTags=GlobalTagsClass(tags=tags),
                )
                schema_fields.append(field)
            last_id = col.ordinal_position
        HiveColumnToAvroConverter._STRUCT_TYPE_SEPARATOR = (
            original_struct_type_separator
        )
        return schema_fields

    def gen_schema_metadata(
        self,
        dataset_urn: str,
        table: Union[BigqueryTable, BigqueryView, BigqueryTableSnapshot],
        columns: List[BigqueryColumn],
        dataset_name: BigqueryTableIdentifier,
    ) -> MetadataWorkUnit:
        foreign_keys: List[ForeignKeyConstraint] = []
        # Foreign keys only make sense for tables
        if isinstance(table, BigqueryTable):
            foreign_keys = list(
                self.gen_foreign_keys(
                    table, dataset_name.dataset, dataset_name.project_id
                )
            )

        schema_metadata = SchemaMetadata(
            schemaName=str(dataset_name),
            platform=self.identifiers.make_data_platform_urn(),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            # fields=[],
            fields=self.gen_schema_fields(
                columns,
                (
                    table.constraints
                    if (isinstance(table, BigqueryTable) and table.constraints)
                    else []
                ),
            ),
            foreignKeys=foreign_keys if foreign_keys else None,
        )

        if self.config.lineage_use_sql_parser:
            self.sql_parser_schema_resolver.add_schema_metadata(
                dataset_urn, schema_metadata
            )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def get_tables_for_dataset(
        self,
        project_id: str,
        dataset: BigqueryDataset,
    ) -> Iterable[BigqueryTable]:
        # In bigquery there is no way to query all tables in a Project id
        with PerfTimer() as timer:
            with_partitions = (
                self.config.have_table_data_read_permission
                and dataset.supports_table_partitions()
            )

            # Partitions view throw exception if we try to query partition info for too many tables
            # so we have to limit the number of tables we query partition info.
            # The conn.list_tables returns table infos that information_schema doesn't contain and this
            # way we can merge that info with the queried one.
            # https://cloud.google.com/bigquery/docs/information-schema-partitions
            if with_partitions:
                max_batch_size = (
                    self.config.number_of_datasets_process_in_batch_if_profiling_enabled
                )
            else:
                max_batch_size = self.config.number_of_datasets_process_in_batch

            # We get the list of tables in the dataset to get core table properties and to be able to process the tables in batches
            # We collect only the latest shards from sharded tables (tables with _YYYYMMDD suffix) and ignore temporary tables
            table_items = self.get_core_table_details(
                dataset.name, project_id, self.config.temp_table_dataset_prefix
            )

            items_to_get: Dict[str, TableListItem] = {}
            for table_item in table_items:
                items_to_get[table_item] = table_items[table_item]
                if len(items_to_get) % max_batch_size == 0:
                    yield from self.schema_api.get_tables_for_dataset(
                        project_id,
                        dataset.name,
                        items_to_get,
                        with_partitions=with_partitions,
                        report=self.report,
                    )
                    items_to_get.clear()

            if items_to_get:
                yield from self.schema_api.get_tables_for_dataset(
                    project_id,
                    dataset.name,
                    items_to_get,
                    with_partitions=with_partitions,
                    report=self.report,
                )

        self.report.metadata_extraction_sec[f"{project_id}.{dataset.name}"] = (
            timer.elapsed_seconds(digits=2)
        )

    def get_core_table_details(
        self, dataset_name: str, project_id: str, temp_table_dataset_prefix: str
    ) -> Dict[str, TableListItem]:
        table_items: Dict[str, TableListItem] = {}
        # Dict to store sharded table and the last seen max shard id
        sharded_tables: Dict[str, TableListItem] = {}

        for table in self.schema_api.list_tables(dataset_name, project_id):
            table_identifier = BigqueryTableIdentifier(
                project_id=project_id,
                dataset=dataset_name,
                table=table.table_id,
            )

            if table.table_type == "VIEW":
                if (
                    not self.config.include_views
                    or not self.config.view_pattern.allowed(
                        table_identifier.raw_table_name()
                    )
                ):
                    self.report.report_dropped(table_identifier.raw_table_name())
                    continue
            else:
                if not self.config.table_pattern.allowed(
                    table_identifier.raw_table_name()
                ):
                    self.report.report_dropped(table_identifier.raw_table_name())
                    continue

            _, shard = BigqueryTableIdentifier.get_table_and_shard(
                table_identifier.table
            )
            table_name = table_identifier.get_table_name().split(".")[-1]

            # Sharded tables look like: table_20220120
            # For sharded tables we only process the latest shard and ignore the rest
            # to find the latest shard we iterate over the list of tables and store the maximum shard id
            # We only have one special case where the table name is a date `20220110`
            # in this case we merge all these tables under dataset name as table name.
            # For example some_dataset.20220110 will be turned to some_dataset.some_dataset
            # It seems like there are some bigquery user who uses this non-standard way of sharding the tables.
            if shard:
                if table_name not in sharded_tables:
                    sharded_tables[table_name] = table
                    continue

                stored_table_identifier = BigqueryTableIdentifier(
                    project_id=project_id,
                    dataset=dataset_name,
                    table=sharded_tables[table_name].table_id,
                )
                _, stored_shard = BigqueryTableIdentifier.get_table_and_shard(
                    stored_table_identifier.table
                )
                # When table is none, we use dataset_name as table_name
                assert stored_shard
                if stored_shard < shard:
                    sharded_tables[table_name] = table
                continue
            elif str(table_identifier).startswith(temp_table_dataset_prefix):
                logger.debug(f"Dropping temporary table {table_identifier.table}")
                self.report.report_dropped(table_identifier.raw_table_name())
                continue

            table_items[table.table_id] = table

        # Adding maximum shards to the list of tables
        table_items.update({value.table_id: value for value in sharded_tables.values()})

        return table_items
