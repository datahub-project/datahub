from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Union

from airflow.configuration import conf
from pydantic import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub_airflow_plugin._airflow_version_specific import IS_AIRFLOW_3_OR_HIGHER

if TYPE_CHECKING:
    from datahub_airflow_plugin.hooks.datahub import (
        DatahubCompositeHook,
        DatahubGenericHook,
    )


class DatajobUrl(Enum):
    GRID = "grid"
    TASKINSTANCE = "taskinstance"
    TASKS = "tasks"  # Airflow 3.x task URL format: /dags/{dag_id}/tasks/{task_id}


class DatahubLineageConfig(ConfigModel):
    enabled: bool

    # DataHub hook connection ID (can be comma-separated for multiple connections).
    datahub_conn_id: str

    # Cluster to associate with the pipelines and tasks. Defaults to "prod".
    cluster: str

    # Platform instance to associate with the pipelines and tasks.
    platform_instance: Optional[str]

    # If true, the owners field of the DAG will be captured as a DataHub corpuser.
    capture_ownership_info: bool

    # If true, the owners field of the DAG will instead be captured as a DataHub corpgroup.
    capture_ownership_as_group: bool

    # If true, the tags field of the DAG will be captured as DataHub tags.
    capture_tags_info: bool

    # If true (default), we'll materialize and un-soft-delete any urns
    # referenced by inlets or outlets.
    materialize_iolets: bool

    # If true (default), capture native Airflow Assets/Datasets as DataHub lineage.
    # Airflow 2.4+ Dataset and Airflow 3.x Asset objects in inlets/outlets
    # will be converted to DataHub dataset URNs.
    capture_airflow_assets: bool

    capture_executions: bool

    datajob_url_link: DatajobUrl

    enable_extractors: bool

    # OpenLineage extractor patching/override controls (only apply when enable_extractors=True)
    # These allow fine-grained control over DataHub's enhancements to OpenLineage extractors

    # If true (default), patch SqlExtractor to use DataHub's SQL parser
    # This enables column-level lineage extraction from SQL queries
    # Works with both Legacy OpenLineage and OpenLineage Provider
    patch_sql_parser: bool

    # If true (default), patch SnowflakeExtractor's default_schema property
    # Fixes schema detection issues in Snowflake operators
    # Works with both Legacy OpenLineage and OpenLineage Provider
    patch_snowflake_schema: bool

    # If true (default), use DataHub's custom AthenaOperatorExtractor
    # Provides better Athena lineage with DataHub's SQL parser
    # Only applies to Legacy OpenLineage (OpenLineage Provider has its own)
    extract_athena_operator: bool

    # If true (default), use DataHub's custom BigQueryInsertJobOperatorExtractor
    # Handles BigQuery job configuration and destination tables
    # Only applies to Legacy OpenLineage (OpenLineage Provider has its own)
    extract_bigquery_insert_job_operator: bool

    # If true (default) use DataHub's custom TeradataOperator
    extract_teradata_operator: bool

    # If true, ti.render_templates() will be called in the listener.
    # Makes extraction of jinja-templated fields more accurate.
    render_templates: bool

    # Only if true, lineage will be emitted for the DataJobs.
    enable_datajob_lineage: bool

    dag_filter_pattern: AllowDenyPattern = Field(
        description="regex patterns for DAGs to ingest",
    )

    log_level: Optional[str]
    debug_emitter: bool

    disable_openlineage_plugin: bool

    @property
    def _datahub_connection_ids(self) -> List[str]:
        """
        Parse comma-separated connection IDs into a list.

        This is implemented as a property to avoid the class variable pollution
        bug that would occur with validators. Each instance computes its own
        connection ID list from its datahub_conn_id field.
        """
        return [conn_id.strip() for conn_id in self.datahub_conn_id.split(",")]

    def make_emitter_hook(self) -> Union["DatahubGenericHook", "DatahubCompositeHook"]:
        # This is necessary to avoid issues with circular imports.
        from datahub_airflow_plugin.hooks.datahub import (
            DatahubCompositeHook,
            DatahubGenericHook,
        )

        connection_ids = self._datahub_connection_ids
        if len(connection_ids) == 1:
            return DatahubGenericHook(connection_ids[0])
        else:
            return DatahubCompositeHook(connection_ids)


def get_lineage_config() -> DatahubLineageConfig:
    """Load the DataHub plugin config from airflow.cfg."""

    enabled = conf.get("datahub", "enabled", fallback=True)
    datahub_conn_id = conf.get("datahub", "conn_id", fallback="datahub_rest_default")
    cluster = conf.get("datahub", "cluster", fallback=builder.DEFAULT_FLOW_CLUSTER)
    platform_instance = conf.get("datahub", "platform_instance", fallback=None)
    capture_tags_info = conf.get("datahub", "capture_tags_info", fallback=True)
    capture_ownership_info = conf.get(
        "datahub", "capture_ownership_info", fallback=True
    )
    capture_ownership_as_group = conf.get(
        "datahub", "capture_ownership_as_group", fallback=False
    )
    capture_executions = conf.get("datahub", "capture_executions", fallback=True)
    materialize_iolets = conf.get("datahub", "materialize_iolets", fallback=True)
    capture_airflow_assets = conf.get(
        "datahub", "capture_airflow_assets", fallback=True
    )
    enable_extractors = conf.get("datahub", "enable_extractors", fallback=True)

    # OpenLineage extractor patching/override configuration
    # These only apply when enable_extractors=True
    patch_sql_parser = conf.get("datahub", "patch_sql_parser", fallback=True)
    patch_snowflake_schema = conf.get(
        "datahub", "patch_snowflake_schema", fallback=True
    )
    extract_athena_operator = conf.get(
        "datahub", "extract_athena_operator", fallback=True
    )
    extract_bigquery_insert_job_operator = conf.get(
        "datahub", "extract_bigquery_insert_job_operator", fallback=True
    )
    extract_teradata_operator = conf.get(
        "datahub", "extract_teradata_operator", fallback=True
    )

    log_level = conf.get("datahub", "log_level", fallback=None)
    debug_emitter = conf.get("datahub", "debug_emitter", fallback=False)

    # Disable OpenLineage plugin by default (disable_openlineage_plugin=True) for all versions.
    # This is the safest default since most DataHub users only want DataHub's lineage.
    #
    # When disable_openlineage_plugin=True (default):
    # - Only DataHub plugin runs (OpenLineagePlugin.listeners are cleared if present)
    # - In Airflow 3: SQLParser calls only DataHub's enhanced parser
    # - In Airflow 2: DataHub uses its own extractors
    # - DataHub gets enhanced parsing with column-level lineage
    #
    # When disable_openlineage_plugin=False (opt-in for dual plugin mode):
    # - Both DataHub and OpenLineage plugins run side-by-side
    # - In Airflow 3: SQLParser calls BOTH parsers
    #   - OpenLineage plugin uses its own parsing results (inputs/outputs)
    #   - DataHub extracts its enhanced parsing (with column-level lineage) from run_facets
    #   - Both plugins get their expected parsing without interference
    # - In Airflow 2: Not recommended - may cause conflicts
    default_disable_openlineage = True

    disable_openlineage_plugin = conf.get(
        "datahub", "disable_openlineage_plugin", fallback=default_disable_openlineage
    )
    render_templates = conf.get("datahub", "render_templates", fallback=True)

    # Use new task URL format for Airflow 3.x, old taskinstance format for Airflow 2.x
    # Airflow 3 changed URL structure: /dags/{dag_id}/tasks/{task_id} instead of /taskinstance/list/...
    default_datajob_url = (
        DatajobUrl.TASKS.value
        if IS_AIRFLOW_3_OR_HIGHER
        else DatajobUrl.TASKINSTANCE.value
    )
    datajob_url_link = conf.get(
        "datahub", "datajob_url_link", fallback=default_datajob_url
    )
    dag_filter_pattern = AllowDenyPattern.model_validate_json(
        conf.get("datahub", "dag_filter_str", fallback='{"allow": [".*"]}')
    )
    enable_lineage = conf.get("datahub", "enable_datajob_lineage", fallback=True)

    return DatahubLineageConfig(
        enabled=enabled,
        datahub_conn_id=datahub_conn_id,
        cluster=cluster,
        platform_instance=platform_instance,
        capture_ownership_info=capture_ownership_info,
        capture_ownership_as_group=capture_ownership_as_group,
        capture_tags_info=capture_tags_info,
        capture_executions=capture_executions,
        materialize_iolets=materialize_iolets,
        capture_airflow_assets=capture_airflow_assets,
        enable_extractors=enable_extractors,
        patch_sql_parser=patch_sql_parser,
        patch_snowflake_schema=patch_snowflake_schema,
        extract_athena_operator=extract_athena_operator,
        extract_bigquery_insert_job_operator=extract_bigquery_insert_job_operator,
        extract_teradata_operator=extract_teradata_operator,
        log_level=log_level,
        debug_emitter=debug_emitter,
        disable_openlineage_plugin=disable_openlineage_plugin,
        datajob_url_link=datajob_url_link,
        render_templates=render_templates,
        dag_filter_pattern=dag_filter_pattern,
        enable_datajob_lineage=enable_lineage,
    )
