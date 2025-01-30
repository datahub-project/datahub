import datetime
import logging
import random
import string
from typing import Any, Dict, List, Optional

from pydantic import Field, validator

from datahub.configuration.common import ConfigModel, DynamicTypedConfig
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.ingestion.sink.file import FileSinkConfig

logger = logging.getLogger(__name__)

# Sentinel value used to check if the run ID is the default value.
DEFAULT_RUN_ID = "__DEFAULT_RUN_ID"


class SourceConfig(DynamicTypedConfig):
    extractor: str = "generic"
    extractor_config: dict = Field(default_factory=dict)


class ReporterConfig(DynamicTypedConfig):
    required: bool = Field(
        False,
        description="Whether the reporter is a required reporter or not. If not required, then configuration and reporting errors will be treated as warnings, not errors",
    )


class FailureLoggingConfig(ConfigModel):
    enabled: bool = Field(
        False,
        description="When enabled, records that fail to be sent to DataHub are logged to disk",
    )
    log_config: Optional[FileSinkConfig] = None


class FlagsConfig(ConfigModel):
    """Experimental flags for the ingestion pipeline.

    As ingestion flags an experimental feature, we do not guarantee backwards compatibility.
    Use at your own risk!
    """

    generate_browse_path_v2: bool = Field(
        default=True,
        description="Generate BrowsePathsV2 aspects from container hierarchy and existing BrowsePaths aspects.",
    )

    generate_browse_path_v2_dry_run: bool = Field(
        default=False,
        description=(
            "Run through browse paths v2 generation but do not actually write the aspects to DataHub. "
            "Requires `generate_browse_path_v2` to also be enabled."
        ),
    )

    generate_memory_profiles: Optional[str] = Field(
        default=None,
        description=(
            "Generate memray memory dumps for ingestion process by providing a path to write the dump file in."
        ),
    )

    set_system_metadata: bool = Field(
        True, description="Set system metadata on entities."
    )
    set_system_metadata_pipeline_name: bool = Field(
        True,
        description="Set system metadata pipeline name. Requires `set_system_metadata` to be enabled.",
    )


def _generate_run_id(source_type: Optional[str] = None) -> str:
    current_time = datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

    if source_type is None:
        source_type = "ingestion"
    return f"{source_type}-{current_time}-{random_suffix}"


class PipelineConfig(ConfigModel):
    source: SourceConfig
    sink: Optional[DynamicTypedConfig] = None
    transformers: Optional[List[DynamicTypedConfig]] = None
    flags: FlagsConfig = Field(default=FlagsConfig(), hidden_from_docs=True)
    reporting: List[ReporterConfig] = []
    run_id: str = DEFAULT_RUN_ID
    datahub_api: Optional[DatahubClientConfig] = None
    pipeline_name: Optional[str] = None
    failure_log: FailureLoggingConfig = FailureLoggingConfig()

    _raw_dict: Optional[dict] = (
        None  # the raw dict that was parsed to construct this config
    )

    @validator("run_id", pre=True, always=True)
    def run_id_should_be_semantic(
        cls, v: Optional[str], values: Dict[str, Any], **kwargs: Any
    ) -> str:
        if v == DEFAULT_RUN_ID:
            source_type = None
            if "source" in values and hasattr(values["source"], "type"):
                source_type = values["source"].type

            return _generate_run_id(source_type)
        else:
            assert v is not None
            return v

    @classmethod
    def from_dict(
        cls, resolved_dict: dict, raw_dict: Optional[dict] = None
    ) -> "PipelineConfig":
        config = cls.parse_obj(resolved_dict)
        config._raw_dict = raw_dict
        return config

    def get_raw_dict(self) -> Dict:
        result = self._raw_dict
        if result is None:
            result = self.dict()
        return result
