import logging
import os
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

from google.cloud import bigquery, datacatalog_v1
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from pydantic import Field, PositiveInt, PrivateAttr, root_validator, validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    StatefulUsageConfigMixin,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.bigquery import BigQueryBaseConfig
from datahub.ingestion.source_config.usage.bigquery_usage import BigQueryCredential

logger = logging.getLogger(__name__)

DEFAULT_BQ_SCHEMA_PARALLELISM = int(
    os.getenv("DATAHUB_BIGQUERY_SCHEMA_PARALLELISM", 20)
)


class BigQueryUsageConfig(BaseUsageConfig):
    _query_log_delay_removed = pydantic_removed_field("query_log_delay")

    max_query_duration: timedelta = Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
    )

    apply_view_usage_to_tables: bool = Field(
        default=False,
        description="Whether to apply view's usage to its base tables. If set to False, uses sql parser and applies usage to views / tables mentioned in the query. If set to True, usage is applied to base tables only.",
    )


class BigQueryConnectionConfig(ConfigModel):
    credential: Optional[BigQueryCredential] = Field(
        default=None, description="BigQuery credential informations"
    )

    _credentials_path: Optional[str] = PrivateAttr(None)

    extra_client_options: Dict[str, Any] = Field(
        default={},
        description="Additional options to pass to google.cloud.logging_v2.client.Client.",
    )

    project_on_behalf: Optional[str] = Field(
        default=None,
        description="[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account.",
    )

    def __init__(self, **data: Any):
        super().__init__(**data)

        if self.credential:
            self._credentials_path = self.credential.create_credential_temp_file()
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

    def get_bigquery_client(self) -> bigquery.Client:
        client_options = self.extra_client_options
        return bigquery.Client(self.project_on_behalf, **client_options)

    def get_policy_tag_manager_client(self) -> datacatalog_v1.PolicyTagManagerClient:
        return datacatalog_v1.PolicyTagManagerClient()

    def make_gcp_logging_client(
        self, project_id: Optional[str] = None
    ) -> GCPLoggingClient:
        # See https://github.com/googleapis/google-cloud-python/issues/2674 for
        # why we disable gRPC here.
        client_options = self.extra_client_options.copy()
        client_options["_use_grpc"] = False
        if project_id is not None:
            return GCPLoggingClient(**client_options, project=project_id)
        else:
            return GCPLoggingClient(**client_options)

    def get_sql_alchemy_url(self) -> str:
        if self.project_on_behalf:
            return f"bigquery://{self.project_on_behalf}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return "bigquery://"


class BigQueryV2Config(
    BigQueryConnectionConfig,
    BigQueryBaseConfig,
    SQLCommonConfig,
    StatefulUsageConfigMixin,
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    ClassificationSourceConfigMixin,
):
    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for project_id to filter in ingestion.",
    )

    include_schema_metadata: bool = Field(
        default=True,
        description="Whether to ingest the BigQuery schema, i.e. projects, schemas, tables, and views.",
    )

    usage: BigQueryUsageConfig = Field(
        default=BigQueryUsageConfig(), description="Usage related configs"
    )

    include_usage_statistics: bool = Field(
        default=True,
        description="Generate usage statistic",
    )

    capture_table_label_as_tag: Union[bool, AllowDenyPattern] = Field(
        default=False,
        description="Capture BigQuery table labels as DataHub tag",
    )

    capture_view_label_as_tag: Union[bool, AllowDenyPattern] = Field(
        default=False,
        description="Capture BigQuery view labels as DataHub tag",
    )

    capture_dataset_label_as_tag: Union[bool, AllowDenyPattern] = Field(
        default=False,
        description="Capture BigQuery dataset labels as DataHub tag",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )

    match_fully_qualified_names: bool = Field(
        default=True,
        description="[deprecated] Whether `dataset_pattern` is matched against fully qualified dataset name `<project_id>.<dataset_name>`.",
    )

    include_external_url: bool = Field(
        default=True,
        description="Whether to populate BigQuery Console url to Datasets/Tables",
    )

    include_data_platform_instance: bool = Field(
        default=False,
        description="Whether to create a DataPlatformInstance aspect, equal to the BigQuery project id."
        " If enabled, will cause redundancy in the browse path for BigQuery entities in the UI,"
        " because the project id is represented as the top-level container.",
    )

    include_table_snapshots: Optional[bool] = Field(
        default=True, description="Whether table snapshots should be ingested."
    )

    table_snapshot_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for table snapshots to filter in ingestion. Specify regex to match the entire snapshot name in database.schema.snapshot format. e.g. to match all snapshots starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    debug_include_full_payloads: bool = Field(
        default=False,
        description="Include full payload into events. It is only for debugging and internal use.",
    )

    number_of_datasets_process_in_batch: int = Field(
        hidden_from_docs=True,
        default=10000,
        description="Number of table queried in batch when getting metadata. This is a low level config property which should be touched with care.",
    )

    number_of_datasets_process_in_batch_if_profiling_enabled: int = Field(
        default=1000,
        description="Number of partitioned table queried in batch when getting metadata. This is a low level config property which should be touched with care. This restriction is needed because we query partitions system view which throws error if we try to touch too many tables.",
    )

    use_tables_list_query_v2: bool = Field(
        default=False,
        description="List tables using an improved query that extracts partitions and last modified timestamps more accurately. Requires the ability to read table data. Automatically enabled when profiling is enabled.",
    )

    @property
    def have_table_data_read_permission(self) -> bool:
        return self.use_tables_list_query_v2 or self.is_profiling_enabled()

    column_limit: int = Field(
        default=300,
        description="Maximum number of columns to process in a table. This is a low level config property which should be touched with care. This restriction is needed because excessively wide tables can result in failure to ingest the schema.",
    )
    # The inheritance hierarchy is wonky here, but these options need modifications.
    project_id: Optional[str] = Field(
        default=None,
        description="[deprecated] Use project_id_pattern or project_ids instead.",
    )
    project_ids: List[str] = Field(
        default_factory=list,
        description=(
            "Ingests specified project_ids. Use this property if you want to specify what projects to ingest or "
            "don't want to give project resourcemanager.projects.list to your service account. "
            "Overrides `project_id_pattern`."
        ),
    )

    storage_project_id: None = Field(default=None, hidden_from_docs=True)

    lineage_use_sql_parser: bool = Field(
        default=True,
        description="Use sql parser to resolve view/table lineage.",
    )
    lineage_parse_view_ddl: bool = Field(
        default=True,
        description="Sql parse view ddl to get lineage.",
    )

    lineage_sql_parser_use_raw_names: bool = Field(
        default=False,
        description="This parameter ignores the lowercase pattern stipulated in the SQLParser. NOTE: Ignored if lineage_use_sql_parser is False.",
    )

    extract_column_lineage: bool = Field(
        default=False,
        description="If enabled, generate column level lineage. "
        "Requires lineage_use_sql_parser to be enabled.",
    )

    extract_lineage_from_catalog: bool = Field(
        default=False,
        description="This flag enables the data lineage extraction from Data Lineage API exposed by Google Data Catalog. NOTE: This extractor can't build views lineage. It's recommended to enable the view's DDL parsing. Read the docs to have more information about: https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage",
    )

    enable_legacy_sharded_table_support: bool = Field(
        default=True,
        description="Use the legacy sharded table urn suffix added.",
    )

    extract_policy_tags_from_catalog: bool = Field(
        default=False,
        description=(
            "This flag enables the extraction of policy tags from the Google Data Catalog API. "
            "When enabled, the extractor will fetch policy tags associated with BigQuery table columns. "
            "For more information about policy tags and column-level security, refer to the documentation: "
            "https://cloud.google.com/bigquery/docs/column-level-security-intro"
        ),
    )

    scheme: str = "bigquery"

    log_page_size: PositiveInt = Field(
        default=1000,
        description="The number of log item will be queried per page for lineage collection",
    )

    # extra_client_options, include_table_lineage and max_query_duration are relevant only when computing the lineage.
    include_table_lineage: Optional[bool] = Field(
        default=True,
        description="Option to enable/disable lineage generation. Is enabled by default.",
    )
    max_query_duration: timedelta = Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
    )

    bigquery_audit_metadata_datasets: Optional[List[str]] = Field(
        default=None,
        description="A list of datasets that contain a table named cloudaudit_googleapis_com_data_access which contain BigQuery audit logs, specifically, those containing BigQueryAuditMetadata. It is recommended that the project of the dataset is also specified, for example, projectA.datasetB.",
    )
    use_exported_bigquery_audit_metadata: bool = Field(
        default=False,
        description="When configured, use BigQueryAuditMetadata in bigquery_audit_metadata_datasets to compute lineage information.",
    )
    use_date_sharded_audit_log_tables: bool = Field(
        default=False,
        description="Whether to read date sharded tables or time partitioned tables when extracting usage from exported audit logs.",
    )

    _cache_path: Optional[str] = PrivateAttr(None)

    upstream_lineage_in_report: bool = Field(
        default=False,
        description="Useful for debugging lineage information. Set to True to see the raw lineage created internally.",
    )

    run_optimized_column_query: bool = Field(
        hidden_from_docs=True,
        default=False,
        description="Run optimized column query to get column information. This is an experimental feature and may not work for all cases.",
    )

    file_backed_cache_size: int = Field(
        hidden_from_docs=True,
        default=2000,
        description="Maximum number of entries for the in-memory caches of FileBacked data structures.",
    )

    exclude_empty_projects: bool = Field(
        default=False,
        description="Option to exclude empty projects from being ingested.",
    )

    schema_resolution_batch_size: int = Field(
        default=100,
        description="The number of tables to process in a batch when resolving schema from DataHub.",
        hidden_from_schema=True,
    )

    max_threads_dataset_parallelism: int = Field(
        default=DEFAULT_BQ_SCHEMA_PARALLELISM,
        description="Number of worker threads to use to parallelize BigQuery Dataset Metadata Extraction."
        " Set to 1 to disable.",
    )

    @root_validator(skip_on_failure=True)
    def profile_default_settings(cls, values: Dict) -> Dict:
        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        values["options"].setdefault("max_overflow", -1)

        return values

    @validator("bigquery_audit_metadata_datasets")
    def validate_bigquery_audit_metadata_datasets(
        cls, v: Optional[List[str]], values: Dict
    ) -> Optional[List[str]]:
        if values.get("use_exported_bigquery_audit_metadata"):
            assert (
                v and len(v) > 0
            ), "`bigquery_audit_metadata_datasets` should be set if using `use_exported_bigquery_audit_metadata: True`."

        return v

    @root_validator(pre=False, skip_on_failure=True)
    def backward_compatibility_configs_set(cls, values: Dict) -> Dict:
        project_id = values.get("project_id")
        project_id_pattern = values.get("project_id_pattern")

        if project_id_pattern == AllowDenyPattern.allow_all() and project_id:
            logging.warning(
                "project_id_pattern is not set but project_id is set, source will only ingest the project_id project. project_id will be deprecated, please use project_id_pattern instead."
            )
            values["project_id_pattern"] = AllowDenyPattern(allow=[f"^{project_id}$"])
        elif project_id_pattern != AllowDenyPattern.allow_all() and project_id:
            logging.warning(
                "use project_id_pattern whenever possible. project_id will be deprecated, please use project_id_pattern only if possible."
            )

        dataset_pattern: Optional[AllowDenyPattern] = values.get("dataset_pattern")
        schema_pattern = values.get("schema_pattern")
        if (
            dataset_pattern == AllowDenyPattern.allow_all()
            and schema_pattern != AllowDenyPattern.allow_all()
        ):
            logging.warning(
                "dataset_pattern is not set but schema_pattern is set, using schema_pattern as dataset_pattern. schema_pattern will be deprecated, please use dataset_pattern instead."
            )
            values["dataset_pattern"] = schema_pattern
            dataset_pattern = schema_pattern
        elif (
            dataset_pattern != AllowDenyPattern.allow_all()
            and schema_pattern != AllowDenyPattern.allow_all()
        ):
            logging.warning(
                "schema_pattern will be ignored in favour of dataset_pattern. schema_pattern will be deprecated, please use dataset_pattern only."
            )

        match_fully_qualified_names = values.get("match_fully_qualified_names")

        if (
            dataset_pattern is not None
            and dataset_pattern != AllowDenyPattern.allow_all()
            and match_fully_qualified_names is not None
            and not match_fully_qualified_names
        ):
            logger.warning(
                "Please update `dataset_pattern` to match against fully qualified schema name `<project_id>.<dataset_name>` and set config `match_fully_qualified_names : True`."
                "The config option `match_fully_qualified_names` is deprecated and will be removed in a future release."
            )
        elif match_fully_qualified_names and dataset_pattern is not None:
            adjusted = False
            for lst in [dataset_pattern.allow, dataset_pattern.deny]:
                for i, pattern in enumerate(lst):
                    if "." not in pattern:
                        if pattern.startswith("^"):
                            lst[i] = r"^.*\." + pattern[1:]
                        else:
                            lst[i] = r".*\." + pattern
                        adjusted = True
            if adjusted:
                logger.warning(
                    "`dataset_pattern` was adjusted to match against fully qualified schema names,"
                    " of the form `<project_id>.<dataset_name>`."
                )

        return values

    def get_table_pattern(self, pattern: List[str]) -> str:
        return "|".join(pattern) if pattern else ""

    platform_instance_not_supported_for_bigquery = pydantic_removed_field(
        "platform_instance"
    )
