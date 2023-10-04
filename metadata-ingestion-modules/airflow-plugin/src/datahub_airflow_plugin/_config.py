from typing import TYPE_CHECKING, Optional

import datahub.emitter.mce_builder as builder
from airflow.configuration import conf
from datahub.configuration.common import ConfigModel

if TYPE_CHECKING:
    from datahub_airflow_plugin.hooks.datahub import DatahubGenericHook


class DatahubLineageConfig(ConfigModel):
    # This class is shared between the lineage backend and the Airflow plugin.
    # The defaults listed here are only relevant for the lineage backend.
    # The Airflow plugin's default values come from the fallback values in
    # the get_lineage_config() function below.

    enabled: bool = True

    # DataHub hook connection ID.
    datahub_conn_id: str

    # Cluster to associate with the pipelines and tasks. Defaults to "prod".
    cluster: str = builder.DEFAULT_FLOW_CLUSTER

    # If true, the owners field of the DAG will be capture as a DataHub corpuser.
    capture_ownership_info: bool = True

    # If true, the tags field of the DAG will be captured as DataHub tags.
    capture_tags_info: bool = True

    capture_executions: bool = False

    enable_extractors: bool = True

    log_level: Optional[str] = None
    debug_emitter: bool = False

    disable_openlineage_plugin: bool = True

    # Note that this field is only respected by the lineage backend.
    # The Airflow plugin behaves as if it were set to True.
    graceful_exceptions: bool = True

    def make_emitter_hook(self) -> "DatahubGenericHook":
        # This is necessary to avoid issues with circular imports.
        from datahub_airflow_plugin.hooks.datahub import DatahubGenericHook

        return DatahubGenericHook(self.datahub_conn_id)


def get_lineage_config() -> DatahubLineageConfig:
    """Load the DataHub plugin config from airflow.cfg."""

    enabled = conf.get("datahub", "enabled", fallback=True)
    datahub_conn_id = conf.get("datahub", "conn_id", fallback="datahub_rest_default")
    cluster = conf.get("datahub", "cluster", fallback=builder.DEFAULT_FLOW_CLUSTER)
    capture_tags_info = conf.get("datahub", "capture_tags_info", fallback=True)
    capture_ownership_info = conf.get(
        "datahub", "capture_ownership_info", fallback=True
    )
    capture_executions = conf.get("datahub", "capture_executions", fallback=True)
    enable_extractors = conf.get("datahub", "enable_extractors", fallback=True)
    log_level = conf.get("datahub", "log_level", fallback=None)
    debug_emitter = conf.get("datahub", "debug_emitter", fallback=False)
    disable_openlineage_plugin = conf.get(
        "datahub", "disable_openlineage_plugin", fallback=True
    )

    return DatahubLineageConfig(
        enabled=enabled,
        datahub_conn_id=datahub_conn_id,
        cluster=cluster,
        capture_ownership_info=capture_ownership_info,
        capture_tags_info=capture_tags_info,
        capture_executions=capture_executions,
        enable_extractors=enable_extractors,
        log_level=log_level,
        debug_emitter=debug_emitter,
        disable_openlineage_plugin=disable_openlineage_plugin,
    )
