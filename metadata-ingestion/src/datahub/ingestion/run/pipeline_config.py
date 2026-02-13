from __future__ import annotations

import datetime
import logging
import random
import string
from typing import Dict, List, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import ConfigModel, DynamicTypedConfig, HiddenFromDocs
from datahub.configuration.env_vars import (
    get_report_failure_sample_size,
    get_report_log_failure_summaries_to_console,
    get_report_log_warning_summaries_to_console,
    get_report_warning_sample_size,
)
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.recording.config import RecordingConfig
from datahub.ingestion.sink.file import FileSinkConfig

logger = logging.getLogger(__name__)

# Report config defaults from environment variables
_DEFAULT_REPORT_FAILURE_SAMPLE_SIZE = get_report_failure_sample_size()
_DEFAULT_REPORT_WARNING_SAMPLE_SIZE = get_report_warning_sample_size()
_DEFAULT_REPORT_LOG_FAILURE_SUMMARIES_TO_CONSOLE = (
    get_report_log_failure_summaries_to_console()
)
_DEFAULT_REPORT_LOG_WARNING_SUMMARIES_TO_CONSOLE = (
    get_report_log_warning_summaries_to_console()
)

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


class ReportConfig(ConfigModel):
    """Configuration for ingestion report behavior.

    All settings can be overridden via environment variables:
    - DATAHUB_REPORT_FAILURE_SAMPLE_SIZE
    - DATAHUB_REPORT_WARNING_SAMPLE_SIZE
    - DATAHUB_REPORT_LOG_FAILURE_SUMMARIES_TO_CONSOLE
    - DATAHUB_REPORT_LOG_WARNING_SUMMARIES_TO_CONSOLE
    """

    failure_sample_size: int = Field(
        default=_DEFAULT_REPORT_FAILURE_SAMPLE_SIZE,
        description="Maximum number of failure entries to include in the report. "
        "Uses reservoir sampling to fairly represent failures when there are more than this limit. "
        "Can also be set via DATAHUB_REPORT_FAILURE_SAMPLE_SIZE env var.",
        ge=1,
    )
    warning_sample_size: int = Field(
        default=_DEFAULT_REPORT_WARNING_SAMPLE_SIZE,
        description="Maximum number of warning entries to include in the report. "
        "Uses reservoir sampling to fairly represent warnings when there are more than this limit. "
        "Can also be set via DATAHUB_REPORT_WARNING_SAMPLE_SIZE env var.",
        ge=1,
    )
    log_failure_summaries_to_console: Optional[bool] = Field(
        default=_DEFAULT_REPORT_LOG_FAILURE_SUMMARIES_TO_CONSOLE,
        description="Control failure summary logging to console: None (default) uses caller's choice, "
        "True forces all failure summaries to log, False suppresses all logging. "
        "Can also be set via DATAHUB_REPORT_LOG_FAILURE_SUMMARIES_TO_CONSOLE env var (true/false/unset).",
    )
    log_warning_summaries_to_console: Optional[bool] = Field(
        default=_DEFAULT_REPORT_LOG_WARNING_SUMMARIES_TO_CONSOLE,
        description="Control warning summary logging to console: None (default) uses caller's choice, "
        "True forces all warning summaries to log, False suppresses all logging. "
        "Can also be set via DATAHUB_REPORT_LOG_WARNING_SUMMARIES_TO_CONSOLE env var (true/false/unset).",
    )


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
    flags: HiddenFromDocs[FlagsConfig] = FlagsConfig()
    reporting: List[ReporterConfig] = []
    run_id: str = DEFAULT_RUN_ID
    datahub_api: Optional[DatahubClientConfig] = None
    pipeline_name: Optional[str] = None
    failure_log: FailureLoggingConfig = FailureLoggingConfig()
    report: ReportConfig = ReportConfig()
    recording: Optional[RecordingConfig] = Field(
        default=None,
        description="Recording configuration for debugging ingestion runs.",
    )

    _raw_dict: Optional[dict] = (
        None  # the raw dict that was parsed to construct this config
    )

    @model_validator(mode="after")
    def run_id_should_be_semantic(self) -> "PipelineConfig":
        if self.run_id == DEFAULT_RUN_ID:
            source_type = None
            if hasattr(self.source, "type"):
                source_type = self.source.type

            self.run_id = _generate_run_id(source_type)
        else:
            assert self.run_id is not None
        return self

    @classmethod
    def from_dict(
        cls, resolved_dict: dict, raw_dict: Optional[dict] = None
    ) -> "PipelineConfig":
        config = cls.model_validate(resolved_dict)
        config._raw_dict = raw_dict
        return config

    def get_raw_dict(self) -> Dict:
        result = self._raw_dict
        if result is None:
            result = self.model_dump()
        return result
