import json
import logging
from typing import Any, Dict

from pydantic import field_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.api.sink import Sink
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry

logger = logging.getLogger(__name__)


class FileReporterConfig(ConfigModel):
    filename: str
    format: str = "json"

    @field_validator("format", mode="after")
    @classmethod
    def only_json_supported(cls, v: str) -> str:
        if v and v.lower() != "json":
            raise ValueError(
                f"Format {v} is not yet supported. Only json is supported at this time"
            )
        return v


class FileReporter(PipelineRunListener):
    @classmethod
    def create(
        cls,
        config_dict: Dict[str, Any],
        ctx: PipelineContext,
        sink: Sink,
    ) -> PipelineRunListener:
        reporter_config = FileReporterConfig.model_validate(config_dict)
        return cls(reporter_config)

    def __init__(self, reporter_config: FileReporterConfig) -> None:
        self.config = reporter_config

    def on_start(self, ctx: PipelineContext) -> None:
        pass

    def on_completion(
        self,
        status: str,
        report: Dict[str, Any],
        ctx: PipelineContext,
    ) -> None:
        try:
            masking_filter = SecretMaskingFilter(SecretRegistry.get_instance())
            masked_report = masking_filter.mask_structure(report)
            with open(self.config.filename, "w") as report_out:
                json.dump(masked_report, report_out)
            logger.info(f"Wrote {status} report successfully to {self.config.filename}")
        except Exception as e:
            logger.error(f"Failed to write structured report due to {e}")
            raise e
