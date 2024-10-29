import json
import logging
import os
import re
import tempfile
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

from google.cloud import bigquery, datacatalog_v1, resourcemanager_v3
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from pydantic import Field, PositiveInt, PrivateAttr, root_validator, validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_multiline_string import pydantic_multiline_string
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationSourceConfigMixin,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig, SQLFilterConfig
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    StatefulUsageConfigMixin,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

logger = logging.getLogger(__name__)

DEFAULT_BQ_SCHEMA_PARALLELISM = int(
    os.getenv("DATAHUB_BIGQUERY_SCHEMA_PARALLELISM", 20)
)

# Regexp for sharded tables.
# A sharded table is a table that has a suffix of the form _yyyymmdd or yyyymmdd, where yyyymmdd is a date.
# The regexp checks for valid dates in the suffix (e.g. 20200101, 20200229, 20201231) and if the date is not valid
# then it is not a sharded table.
_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX: str = (
    "((.+\\D)[_$]?)?(\\d\\d\\d\\d(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))$"
)


class BigQueryBaseConfig(ConfigModel):
    rate_limit: bool = Field(
        default=False, description="Should we rate limit requests made to API."
    )
    requests_per_min: int = Field(
        default=60,
        description="Used to control number of API calls made per min. Only used when `rate_limit` is set to `True`.",
    )

    temp_table_dataset_prefix: str = Field(
        default="_",
        description="If you are creating temp tables in a dataset with a particular prefix you can use this config to set the prefix for the dataset. This is to support workflows from before bigquery's introduction of temp tables. By default we use `_` because of datasets that begin with an underscore are hidden by default https://cloud.google.com/bigquery/docs/datasets#dataset-naming.",
    )

    sharded_table_pattern: str = Field(
        deprecated=True,
        default=_BIGQUERY_DEFAULT_SHARDED_TABLE_REGEX,
        description="The regex pattern to match sharded tables and group as one table. This is a very low level config parameter, only change if you know what you are doing, ",
    )

    @validator("sharded_table_pattern")
    def sharded_table_pattern_is_a_valid_regexp(cls, v):
        try:
            re.compile(v)
        except Exception as e:
            raise ValueError(
                "sharded_table_pattern configuration pattern is invalid."
            ) from e
        return v

    @root_validator(pre=True, skip_on_failure=True)
    def project_id_backward_compatibility_configs_set(cls, values: Dict) -> Dict:
        project_id = values.pop("project_id", None)
        project_ids = values.get("project_ids")

        if not project_ids and project_id:
            values["project_ids"] = [project_id]
        elif project_ids and project_id:
            logging.warning(
                "Please use `project_ids` config. Config `project_id` will be ignored."
            )
        return values


class BigQueryUsageConfig(BaseUsageConfig):
    _query_log_delay_removed = pydantic_removed_field("query_log_delay")

    max_query_duration: timedelta = Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens "
        "within our time range but the query completion event is delayed and happens after the configured"
        " end time.",
    )

    apply_view_usage_to_tables: bool = Field(
        default=False,
        description="Whether to apply view's usage to its base tables. If set to False, uses sql parser and applies "
        "usage to views / tables mentioned in the query. If set to True, usage is applied to base tables "
        "only.",
    )


class BigQueryCredential(ConfigModel):
    project_id: str = Field(description="Project id to set the credentials")
    private_key_id: str = Field(description="Private key id")
    private_key: str = Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'"
    )
    client_email: str = Field(description="Client email")
    client_id: str = Field(description="Client Id")
    auth_uri: str = Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = Field(default="service_account", description="Authentication type")
    client_x509_cert_url: Optional[str] = Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )

    _fix_private_key_newlines = pydantic_multiline_string("private_key")

    @root_validator(skip_on_failure=True)
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("client_x509_cert_url") is None:
            values[
                "client_x509_cert_url"
            ] = f'https://www.googleapis.com/robot/v1/metadata/x509/{values["client_email"]}'
        return values

    def create_credential_temp_file(self) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            cred_json = json.dumps(self.dict(), indent=4, separators=(",", ": "))
            fp.write(cred_json.encode())
            return fp.name


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

    def get_projects_client(self) -> resourcemanager_v3.ProjectsClient:
        return resourcemanager_v3.ProjectsClient()

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


class GcsLineageProviderConfig(ConfigModel):
    """
    Any source that produces gcs lineage from/to Datasets should inherit this class.
    """

    path_specs: List[PathSpec] = Field(
        default=[],
        description="List of PathSpec. See below the details about PathSpec",
    )

    strip_urls: bool = Field(
        default=True,
        description="Strip filename from gcs url. It only applies if path_specs are not specified.",
    )

    ignore_non_path_spec_path: bool = Field(
        default=False,
        description="Ignore paths that are not match in path_specs. It only applies if path_specs are specified.",
    )


class GcsDatasetLineageProviderConfigBase(ConfigModel):
    """
    Any source that produces gcs lineage from/to Datasets should inherit this class.
    This is needeed to group all lineage related configs under `gcs_lineage_config` config property.
    """

    gcs_lineage_config: GcsLineageProviderConfig = Field(
        default=GcsLineageProviderConfig(),
        description="Common config for gcs lineage generation",
    )


class BigQueryFilterConfig(SQLFilterConfig):
    project_ids: List[str] = Field(
        default_factory=list,
        description=(
            "Ingests specified project_ids. Use this property if you want to specify what projects to ingest or "
            "don't want to give project resourcemanager.projects.list to your service account. "
            "Overrides `project_id_pattern`."
        ),
    )
    project_labels: List[str] = Field(
        default_factory=list,
        description=(
            "Ingests projects with the specified labels. Set value in the format of `key:value`. Use this property to "
            "define which projects to ingest based"
            "on project-level labels. If project_ids or project_id is set, this configuration has no effect. The "
            "ingestion process filters projects by label first, and then applies the project_id_pattern."
        ),
    )

    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for project_id to filter in ingestion.",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset to filter in ingestion. Specify regex to only match the schema name. "
        "e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )

    match_fully_qualified_names: bool = Field(
        default=True,
        description="[deprecated] Whether `dataset_pattern` is matched against fully qualified dataset name "
        "`<project_id>.<dataset_name>`.",
    )

    table_snapshot_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for table snapshots to filter in ingestion. Specify regex to match the entire "
        "snapshot name in database.schema.snapshot format. e.g. to match all snapshots starting with "
        "customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )

    # NOTE: `schema_pattern` is added here only to hide it from docs.
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        hidden_from_docs=True,
    )

    @root_validator(pre=False, skip_on_failure=True)
    def backward_compatibility_configs_set(cls, values: Dict) -> Dict:
        dataset_pattern: Optional[AllowDenyPattern] = values.get("dataset_pattern")
        schema_pattern = values.get("schema_pattern")
        if (
            dataset_pattern == AllowDenyPattern.allow_all()
            and schema_pattern != AllowDenyPattern.allow_all()
        ):
            logging.warning(
                "dataset_pattern is not set but schema_pattern is set, using schema_pattern as dataset_pattern. "
                "schema_pattern will be deprecated, please use dataset_pattern instead."
            )
            values["dataset_pattern"] = schema_pattern
            dataset_pattern = schema_pattern
        elif (
            dataset_pattern != AllowDenyPattern.allow_all()
            and schema_pattern != AllowDenyPattern.allow_all()
        ):
            logging.warning(
                "schema_pattern will be ignored in favour of dataset_pattern. schema_pattern will be deprecated,"
                " please use dataset_pattern only."
            )

        match_fully_qualified_names = values.get("match_fully_qualified_names")

        if (
            dataset_pattern is not None
            and dataset_pattern != AllowDenyPattern.allow_all()
            and match_fully_qualified_names is not None
            and not match_fully_qualified_names
        ):
            logger.warning(
                "Please update `dataset_pattern` to match against fully qualified schema name "
                "`<project_id>.<dataset_name>` and set config `match_fully_qualified_names : True`."
                "The config option `match_fully_qualified_names` is deprecated and will be "
                "removed in a future release."
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


class BigQueryIdentifierConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, LowerCaseDatasetUrnConfigMixin
):
    include_data_platform_instance: bool = Field(
        default=False,
        description="Whether to create a DataPlatformInstance aspect, equal to the BigQuery project id."
        " If enabled, will cause redundancy in the browse path for BigQuery entities in the UI,"
        " because the project id is represented as the top-level container.",
    )

    enable_legacy_sharded_table_support: bool = Field(
        default=True,
        description="Use the legacy sharded table urn suffix added.",
    )


class BigQueryV2Config(
    GcsDatasetLineageProviderConfigBase,
    BigQueryConnectionConfig,
    BigQueryBaseConfig,
    BigQueryFilterConfig,
    # BigQueryFilterConfig must come before (higher precedence) the SQLCommon config, so that the documentation overrides are applied.
    BigQueryIdentifierConfig,
    SQLCommonConfig,
    StatefulUsageConfigMixin,
    StatefulLineageConfigMixin,
    StatefulProfilingConfigMixin,
    ClassificationSourceConfigMixin,
):

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

    include_table_constraints: bool = Field(
        default=True,
        description="Whether to ingest table constraints. If you know you don't use table constraints, you can disable it to save one extra query per dataset. In general it should be enabled",
    )

    include_external_url: bool = Field(
        default=True,
        description="Whether to populate BigQuery Console url to Datasets/Tables",
    )

    include_table_snapshots: Optional[bool] = Field(
        default=True, description="Whether table snapshots should be ingested."
    )

    debug_include_full_payloads: bool = Field(
        default=False,
        description="Include full payload into events. It is only for debugging and internal use.",
    )

    number_of_datasets_process_in_batch: int = Field(
        hidden_from_docs=True,
        default=10000,
        description="Number of table queried in batch when getting metadata. This is a low level config property "
        "which should be touched with care.",
    )

    number_of_datasets_process_in_batch_if_profiling_enabled: int = Field(
        default=1000,
        description="Number of partitioned table queried in batch when getting metadata. This is a low level config "
        "property which should be touched with care. This restriction is needed because we query "
        "partitions system view which throws error if we try to touch too many tables.",
    )

    use_tables_list_query_v2: bool = Field(
        default=False,
        description="List tables using an improved query that extracts partitions and last modified timestamps more "
        "accurately. Requires the ability to read table data. Automatically enabled when profiling is "
        "enabled.",
    )

    use_queries_v2: bool = Field(
        default=False,
        description="If enabled, uses the new queries extractor to extract queries from bigquery.",
    )

    @property
    def have_table_data_read_permission(self) -> bool:
        return self.use_tables_list_query_v2 or self.is_profiling_enabled()

    column_limit: int = Field(
        default=300,
        description="Maximum number of columns to process in a table. This is a low level config property which "
        "should be touched with care. This restriction is needed because excessively wide tables can "
        "result in failure to ingest the schema.",
    )

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

    include_column_lineage_with_gcs: bool = Field(
        default=True,
        description="When enabled, column-level lineage will be extracted from the gcs.",
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

    region_qualifiers: List[str] = Field(
        default=["region-us", "region-eu"],
        description="BigQuery regions to be scanned for bigquery jobs when using `use_queries_v2`. "
        "See [this](https://cloud.google.com/bigquery/docs/information-schema-jobs#scope_and_syntax) for details.",
    )

    # include_view_lineage and include_view_column_lineage are inherited from SQLCommonConfig
    # but not used in bigquery so we hide them from docs.
    include_view_lineage: bool = Field(default=True, hidden_from_docs=True)

    include_view_column_lineage: bool = Field(default=True, hidden_from_docs=True)

    @root_validator(pre=True)
    def set_include_schema_metadata(cls, values: Dict) -> Dict:
        # Historically this is used to disable schema ingestion
        if (
            "include_tables" in values
            and "include_views" in values
            and not values["include_tables"]
            and not values["include_views"]
        ):
            values["include_schema_metadata"] = False
            values["include_table_snapshots"] = False
            logger.info(
                "include_tables and include_views are both set to False."
                " Disabling schema metadata ingestion for tables, views, and snapshots."
            )

        return values

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

    def get_table_pattern(self, pattern: List[str]) -> str:
        return "|".join(pattern) if pattern else ""

    platform_instance_not_supported_for_bigquery = pydantic_removed_field(
        "platform_instance"
    )
