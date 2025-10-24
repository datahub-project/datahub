from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from airflow.configuration import conf
from pydantic import Field

# Support both Pydantic v1 and v2
try:
    from pydantic import model_validator

    PYDANTIC_VERSION = 2
except ImportError:
    from pydantic import root_validator  # type: ignore

    PYDANTIC_VERSION = 1

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel

if TYPE_CHECKING:
    from datahub_airflow_plugin.hooks.datahub import (
        DatahubCompositeHook,
        DatahubGenericHook,
    )


class DatajobUrl(Enum):
    GRID = "grid"
    TASKINSTANCE = "taskinstance"


class DatahubLineageConfig(ConfigModel):
    enabled: bool

    # DataHub hook connection ID.
    datahub_conn_id: str

    _datahub_connection_ids: List[str]

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

    capture_executions: bool

    datajob_url_link: DatajobUrl

    enable_extractors: bool

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

    def make_emitter_hook(self) -> Union["DatahubGenericHook", "DatahubCompositeHook"]:
        # This is necessary to avoid issues with circular imports.
        from datahub_airflow_plugin.hooks.datahub import (
            DatahubCompositeHook,
            DatahubGenericHook,
        )

        if len(self._datahub_connection_ids) == 1:
            return DatahubGenericHook(self._datahub_connection_ids[0])
        else:
            return DatahubCompositeHook(self._datahub_connection_ids)

    # Support both Pydantic v1 and v2
    if PYDANTIC_VERSION == 2:

        @model_validator(mode="before")
        @classmethod
        def split_conn_ids(cls, data: Any) -> Any:
            if isinstance(data, dict):
                if not data.get("datahub_conn_id"):
                    raise ValueError("datahub_conn_id is required")
                conn_ids = data.get("datahub_conn_id", "").split(",")
                cls._datahub_connection_ids = [conn_id.strip() for conn_id in conn_ids]
            return data
    else:

        @root_validator(skip_on_failure=True)  # type: ignore
        def split_conn_ids(cls, values: Dict) -> Dict:
            if not values.get("datahub_conn_id"):
                raise ValueError("datahub_conn_id is required")
            conn_ids = values.get("datahub_conn_id", "").split(",")
            cls._datahub_connection_ids = [conn_id.strip() for conn_id in conn_ids]
            return values


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
    enable_extractors = conf.get("datahub", "enable_extractors", fallback=True)
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
    datajob_url_link = conf.get(
        "datahub", "datajob_url_link", fallback=DatajobUrl.TASKINSTANCE.value
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
        enable_extractors=enable_extractors,
        log_level=log_level,
        debug_emitter=debug_emitter,
        disable_openlineage_plugin=disable_openlineage_plugin,
        datajob_url_link=datajob_url_link,
        render_templates=render_templates,
        dag_filter_pattern=dag_filter_pattern,
        enable_datajob_lineage=enable_lineage,
    )
