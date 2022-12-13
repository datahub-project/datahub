import logging
from datetime import timedelta
from typing import Dict, List, Optional

from pydantic import Field, PositiveInt, root_validator

from datahub.configuration.common import AllowDenyPattern, LineageConfig
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.sql.bigquery import BigQueryConfig

logger = logging.getLogger(__name__)


class BigQueryUsageConfig(BaseUsageConfig):
    query_log_delay: Optional[PositiveInt] = Field(
        default=None,
        description="To account for the possibility that the query event arrives after the read event in the audit logs, we wait for at least query_log_delay additional events to be processed before attempting to resolve BigQuery job information from the logs. If query_log_delay is None, it gets treated as an unlimited delay, which prioritizes correctness at the expense of memory usage.",
    )

    max_query_duration: timedelta = Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
    )


class BigQueryV2Config(BigQueryConfig, LineageConfig):
    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for project_id to filter in ingestion.",
    )

    usage: BigQueryUsageConfig = Field(
        default=BigQueryUsageConfig(), description="Usage related configs"
    )

    include_usage_statistics: bool = Field(
        default=True,
        description="Generate usage statistic",
    )

    capture_table_label_as_tag: bool = Field(
        default=False,
        description="Capture BigQuery table labels as tag",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )

    match_fully_qualified_names: bool = Field(
        default=False,
        description="Whether `dataset_pattern` is matched against fully qualified dataset name `<project_id>.<dataset_name>`.",
    )

    include_external_url: bool = Field(
        default=True,
        description="Whether to populate BigQuery Console url to Datasets/Tables",
    )

    debug_include_full_payloads: bool = Field(
        default=False,
        description="Include full payload into events. It is only for debugging and internal use.",
    )

    number_of_datasets_process_in_batch: int = Field(
        default=80,
        description="Number of table queried in batch when getting metadata. This is a low level config property which should be touched with care. This restriction is needed because we query partitions system view which throws error if we try to touch too many tables.",
    )
    column_limit: int = Field(
        default=300,
        description="Maximum number of columns to process in a table. This is a low level config property which should be touched with care. This restriction is needed because excessively wide tables can result in failure to ingest the schema.",
    )
    # The inheritance hierarchy is wonky here, but these options need modifications.
    project_id: Optional[str] = Field(
        default=None,
        description="[deprecated] Use project_id_pattern instead. You can use this property if you only want to ingest one project and don't want to give project resourcemanager.projects.list to your service account",
    )

    project_on_behalf: Optional[str] = Field(
        default=None,
        description="[Advanced] The BigQuery project in which queries are executed. Will be passed when creating a job. If not passed, falls back to the project associated with the service account..",
    )

    storage_project_id: None = Field(default=None, hidden_from_schema=True)

    lineage_use_sql_parser: bool = Field(
        default=False,
        description="Experimental. Use sql parser to resolve view/table lineage. If there is a view being referenced then bigquery sends both the view as well as underlying tablein the references. There is no distinction between direct/base objects accessed. So doing sql parsing to ensure we only use direct objects accessed for lineage.",
    )
    lineage_parse_view_ddl: bool = Field(
        default=True,
        description="Sql parse view ddl to get lineage.",
    )

    convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Convert urns to lowercase.",
    )

    @root_validator(pre=False)
    def profile_default_settings(cls, values: Dict) -> Dict:
        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        values["options"].setdefault("max_overflow", values["profiling"].max_workers)

        return values

    @root_validator(pre=False)
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

        dataset_pattern = values.get("dataset_pattern")
        schema_pattern = values.get("schema_pattern")
        if (
            dataset_pattern == AllowDenyPattern.allow_all()
            and schema_pattern != AllowDenyPattern.allow_all()
        ):
            logging.warning(
                "dataset_pattern is not set but schema_pattern is set, using schema_pattern as dataset_pattern. schema_pattern will be deprecated, please use dataset_pattern instead."
            )
            values["dataset_pattern"] = schema_pattern
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
                "Current default `match_fully_qualified_names: False` is only to maintain backward compatibility. "
                "The config option `match_fully_qualified_names` will be deprecated in future and the default behavior will assume `match_fully_qualified_names: True`."
            )
        return values

    def get_table_pattern(self, pattern: List[str]) -> str:
        return "|".join(pattern) if self.table_pattern else ""

    # TODO: remove run_on_compute when the legacy bigquery source will be deprecated
    def get_sql_alchemy_url(self, run_on_compute: bool = False) -> str:
        if self.project_on_behalf:
            return f"bigquery://{self.project_on_behalf}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return "bigquery://"
