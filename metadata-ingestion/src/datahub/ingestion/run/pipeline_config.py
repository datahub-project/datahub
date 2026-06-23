from __future__ import annotations

import datetime
import logging
import random
import string
from typing import Dict, List, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import ConfigModel, DynamicTypedConfig, HiddenFromDocs
from datahub.configuration.env_vars import (
    get_progress_report_max_failures,
    get_progress_report_max_infos,
    get_progress_report_max_warnings,
    get_report_failure_sample_size,
    get_report_info_sample_size,
    get_report_warning_sample_size,
)
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.recording.config import RecordingConfig
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


class UpstreamPlatformCasing(ConfigModel):
    """An upstream warehouse platform whose asset casing lineage references should
    be reconciled against."""

    platform: str = Field(
        description="Upstream data platform whose assets are referenced by this "
        "source's lineage (e.g. `snowflake`). References to this platform's assets "
        "are reconciled against the casing stored in DataHub.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance of the upstream platform, if any.",
    )
    env: str = Field(
        default="PROD",
        description="Environment (FabricType) of the upstream platform's assets.",
    )


class NormalizeLineageUrnCasingConfig(ConfigModel):
    """Configuration for the lineage URN casing normalization work unit processor.

    Intended to be enabled on BI-tool / cross-platform ingestions that reference
    warehouse assets — NOT on the warehouse ingestion itself, whose reported casing
    and identity must be respected.
    """

    enabled: bool = Field(
        default=False,
        description="Whether to reconcile the casing of upstream warehouse URN "
        "references in lineage against the casing stored in DataHub.",
    )
    upstream_platforms: List[UpstreamPlatformCasing] = Field(
        default_factory=list,
        description="The upstream warehouse platform(s) to bulk-load and reconcile "
        "lineage references against. References to platforms not listed here are "
        "left unchanged.",
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

    normalize_lineage_urn_casing: NormalizeLineageUrnCasingConfig = Field(
        default_factory=NormalizeLineageUrnCasingConfig,
        description=(
            "Experimental: before emitting lineage, reconcile the casing of upstream "
            "warehouse URN references (table- and column-level) against the casing "
            "stored in DataHub, so casing mismatches between sources (e.g. an uppercase "
            "Snowflake table referenced as lowercase by a BI tool, or vice versa) don't "
            "produce two disconnected lineage nodes. Requires a DataHub backend "
            "connection (no-op for offline/file-only ingestion) and the upstream "
            "platform(s) to be configured. Enable on BI-tool ingestions, not on the "
            "warehouse ingestion itself."
        ),
    )

    progress_report_max_failures: int = Field(
        ge=0,
        default_factory=get_progress_report_max_failures,
        description=(
            "Maximum failure entries shown in interim progress reports (every 60 s). "
            "Does not affect the final report. "
            "Also settable via DATAHUB_PROGRESS_REPORT_MAX_FAILURES env var."
        ),
    )
    progress_report_max_warnings: int = Field(
        ge=0,
        default_factory=get_progress_report_max_warnings,
        description=(
            "Maximum warning entries shown in interim progress reports. "
            "Does not affect the final report. "
            "Also settable via DATAHUB_PROGRESS_REPORT_MAX_WARNINGS env var."
        ),
    )
    progress_report_max_infos: int = Field(
        ge=0,
        default_factory=get_progress_report_max_infos,
        description=(
            "Maximum info entries shown in interim progress reports. "
            "Does not affect the final report. "
            "Also settable via DATAHUB_PROGRESS_REPORT_MAX_INFOS env var."
        ),
    )

    report_failure_sample_size: int = Field(
        ge=0,
        default_factory=get_report_failure_sample_size,
        description=(
            "How many failure entries to retain. Controls the final report size "
            "and the pool that interim reports draw from. "
            "Also settable via DATAHUB_REPORT_FAILURE_SAMPLE_SIZE env var."
        ),
    )
    report_warning_sample_size: int = Field(
        ge=0,
        default_factory=get_report_warning_sample_size,
        description=(
            "How many warning entries to retain. Controls the final report size "
            "and the pool that interim reports draw from. "
            "Also settable via DATAHUB_REPORT_WARNING_SAMPLE_SIZE env var."
        ),
    )
    report_info_sample_size: int = Field(
        ge=0,
        default_factory=get_report_info_sample_size,
        description=(
            "How many info entries to retain. Controls the final report size "
            "and the pool that interim reports draw from. "
            "Also settable via DATAHUB_REPORT_INFO_SAMPLE_SIZE env var."
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
