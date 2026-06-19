import logging
from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Union

from airflow.configuration import conf
from pydantic import Field

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from datahub_airflow_plugin.hooks.datahub import (
        DatahubCompositeHook,
        DatahubGenericHook,
    )


class DatajobUrl(Enum):
    GRID = "grid"
    TASKS = "tasks"  # /dags/{dag_id}/tasks/{task_id}


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

    # If true (default), capture native Airflow Assets/Datasets in inlets/outlets
    # as DataHub dataset URNs.
    capture_airflow_assets: bool

    capture_executions: bool

    datajob_url_link: DatajobUrl

    enable_extractors: bool

    # Per-patch controls (only apply when enable_extractors=True).
    # Patch the OpenLineage `SQLParser.generate_openlineage_metadata_from_sql()` to use
    # DataHub's SQL parser (enables column-level lineage).
    patch_sql_parser: bool

    # Replace the corresponding operator's `get_openlineage_facets_on_complete()` with a
    # DataHub-enhanced implementation.
    extract_athena_operator: bool
    extract_bigquery_insert_job_operator: bool
    extract_teradata_operator: bool

    # If true, ti.render_templates() will be called in the listener.
    # Makes extraction of jinja-templated fields more accurate.
    render_templates: bool

    # If true, use multi-statement SQL parsing that resolves temporary tables
    # and merges lineage across multiple SQL statements in a single execution.
    # When false (default), only the first SQL statement is parsed.
    enable_multi_statement_sql_parsing: bool

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

    # OpenLineage patches (only apply when enable_extractors=True)
    patch_sql_parser = conf.get("datahub", "patch_sql_parser", fallback=True)
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

    # Disable OpenLineage plugin by default — only DataHub's listener runs, and
    # SQLParser is patched to invoke DataHub's enhanced parser. Setting this
    # to False lets both plugins run side-by-side; DataHub then reads its enhanced
    # parsing from run_facets and OpenLineage keeps its own inputs/outputs.
    default_disable_openlineage = True

    disable_openlineage_plugin = conf.get(
        "datahub", "disable_openlineage_plugin", fallback=default_disable_openlineage
    )
    render_templates = conf.get("datahub", "render_templates", fallback=True)
    enable_multi_statement_sql_parsing = conf.get(
        "datahub", "enable_multi_statement_sql_parsing", fallback=False
    )

    datajob_url_link = conf.get(
        "datahub", "datajob_url_link", fallback=DatajobUrl.TASKS.value
    )
    # `taskinstance` was the Airflow-2 URL format (DatajobUrl.TASKINSTANCE,
    # now removed). Catch it with a clear migration message rather than
    # surfacing an opaque pydantic enum-validation error.
    if datajob_url_link.lower() == "taskinstance":
        raise ValueError(
            "[datahub] datajob_url_link=taskinstance is the Airflow 2 URL "
            "format and is no longer supported by this plugin. "
            "Set datajob_url_link to 'tasks' (default) or 'grid' in airflow.cfg."
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
        enable_multi_statement_sql_parsing=enable_multi_statement_sql_parsing,
    )


def get_configured_env() -> str:
    """
    Get the configured DataHub cluster/environment name.

    Uses cached config from the listener when available, otherwise reads
    directly from config.

    Returns:
        The configured cluster name
    """
    from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

    listener = get_airflow_plugin_listener()
    if listener and listener.config:
        return listener.config.cluster

    # Fallback: listener disabled or failed to initialize
    logger.debug(
        "Listener or config not available, falling back to get_lineage_config()"
    )
    return get_lineage_config().cluster


def get_enable_multi_statement() -> bool:
    """
    Get the enable_multi_statement_sql_parsing flag from config.

    Uses cached config from the listener when available, otherwise reads
    directly from config. Returns False if config loading fails.

    Returns:
        True if multi-statement SQL parsing is enabled, False otherwise
    """
    try:
        from datahub_airflow_plugin.datahub_listener import get_airflow_plugin_listener

        listener = get_airflow_plugin_listener()
        if listener and listener.config:
            return listener.config.enable_multi_statement_sql_parsing

        # Fallback: listener disabled or failed to initialize
        logger.debug(
            "Listener or config not available, falling back to get_lineage_config()"
        )
        return get_lineage_config().enable_multi_statement_sql_parsing
    except Exception as e:
        logger.warning(
            f"Could not load config to check enable_multi_statement_sql_parsing: {e}"
        )
        return False
