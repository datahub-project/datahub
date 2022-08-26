import datetime
import logging
import uuid
from typing import Any, Dict, List, Optional

from pydantic import Field, root_validator, validator

from datahub.cli.cli_utils import get_url_and_token
from datahub.configuration import config_loader
from datahub.configuration.common import ConfigModel, DynamicTypedConfig
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.ingestion.sink.file import FileSinkConfig

logger = logging.getLogger(__name__)


class SourceConfig(DynamicTypedConfig):
    extractor: str = "generic"


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


class PipelineConfig(ConfigModel):
    # Once support for discriminated unions gets merged into Pydantic, we can
    # simplify this configuration and validation.
    # See https://github.com/samuelcolvin/pydantic/pull/2336.

    source: SourceConfig
    sink: DynamicTypedConfig
    transformers: Optional[List[DynamicTypedConfig]]
    reporting: List[ReporterConfig] = []
    run_id: str = "__DEFAULT_RUN_ID"
    datahub_api: Optional[DatahubClientConfig] = None
    pipeline_name: Optional[str] = None
    failure_log: FailureLoggingConfig = FailureLoggingConfig()

    _raw_dict: Optional[
        dict
    ] = None  # the raw dict that was parsed to construct this config

    @validator("run_id", pre=True, always=True)
    def run_id_should_be_semantic(
        cls, v: Optional[str], values: Dict[str, Any], **kwargs: Any
    ) -> str:
        if v == "__DEFAULT_RUN_ID":
            if "source" in values and hasattr(values["source"], "type"):
                source_type = values["source"].type
                current_time = datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
                return f"{source_type}-{current_time}"

            return str(uuid.uuid1())  # default run_id if we cannot infer a source type
        else:
            assert v is not None
            return v

    @root_validator(pre=True)
    def default_sink_is_datahub_rest(cls, values: Dict[str, Any]) -> Any:
        if "sink" not in values:
            gms_host, gms_token = get_url_and_token()
            default_sink_config = {
                "type": "datahub-rest",
                "config": {
                    "server": gms_host,
                    "token": gms_token,
                },
            }
            # resolve env variables if present
            default_sink_config = config_loader.resolve_env_variables(
                default_sink_config
            )
            values["sink"] = default_sink_config

        return values

    @validator("datahub_api", always=True)
    def datahub_api_should_use_rest_sink_as_default(
        cls, v: Optional[DatahubClientConfig], values: Dict[str, Any], **kwargs: Any
    ) -> Optional[DatahubClientConfig]:
        if v is None and "sink" in values and hasattr(values["sink"], "type"):
            sink_type = values["sink"].type
            if sink_type == "datahub-rest":
                sink_config = values["sink"].config
                v = DatahubClientConfig.parse_obj(sink_config)
        return v

    @classmethod
    def from_dict(
        cls, resolved_dict: dict, raw_dict: Optional[dict] = None
    ) -> "PipelineConfig":
        config = cls.parse_obj(resolved_dict)
        config._raw_dict = raw_dict
        return config
