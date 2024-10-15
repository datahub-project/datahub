import logging
from typing import Dict, Optional

import requests
from pydantic.class_validators import root_validator, validator
from pydantic.fields import Field

from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.superset import SupersetConfig, SupersetSource
from datahub.utilities import config_clean

logger = logging.getLogger(__name__)


class PresetConfig(SupersetConfig):
    manager_uri: str = Field(
        default="https://api.app.preset.io", description="Preset.io API URL"
    )
    connect_uri: str = Field(default="", description="Preset workspace URL.")
    display_uri: Optional[str] = Field(
        default=None,
        description="optional URL to use in links (if `connect_uri` is only for ingestion)",
    )
    api_key: Optional[str] = Field(default=None, description="Preset.io API key.")
    api_secret: Optional[str] = Field(default=None, description="Preset.io API secret.")

    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Preset Stateful Ingestion Config."
    )

    options: Dict = Field(default={}, description="")
    env: str = Field(
        default=DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs",
    )
    database_alias: Dict[str, str] = Field(
        default={},
        description="Can be used to change mapping for database names in superset to what you have in datahub",
    )

    @validator("connect_uri", "display_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)

    @root_validator(skip_on_failure=True)
    def default_display_uri_to_connect_uri(cls, values):
        base = values.get("display_uri")
        if base is None:
            values["display_uri"] = values.get("connect_uri")
        return values


@platform_name("Preset")
@config_class(PresetConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.DELETION_DETECTION, "Optionally enabled via stateful_ingestion"
)
class PresetSource(SupersetSource):
    """
    Variation of the Superset plugin that works with Preset.io (Apache Superset SaaS).
    """

    config: PresetConfig
    report: StaleEntityRemovalSourceReport
    platform = "preset"

    def __init__(self, ctx: PipelineContext, config: PresetConfig):
        logger.info(f"ctx is {ctx}")

        super().__init__(ctx, config)
        self.config = config
        self.report = StaleEntityRemovalSourceReport()

    def login(self):
        try:
            login_response = requests.post(
                f"{self.config.manager_uri}/v1/auth/",
                json={"name": self.config.api_key, "secret": self.config.api_secret},
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to authenticate with Preset: {e}")
            raise e

        self.access_token = login_response.json()["payload"]["access_token"]
        logger.debug("Got access token from Preset")

        requests_session = requests.Session()
        requests_session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )
        # Test the connection
        test_response = requests_session.get(f"{self.config.connect_uri}/version")
        if not test_response.ok:
            logger.error("Unable to connect to workspace")
        return requests_session
