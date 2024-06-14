import re
from typing import Any, Dict, Optional, Tuple

from looker_sdk.sdk.api40.models import DBConnection
from pydantic import Field, validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError
from datahub.configuration.source_common import EnvConfigMixin


def _get_bigquery_definition(
    looker_connection: DBConnection,
) -> Tuple[str, Optional[str], Optional[str]]:
    platform = "bigquery"
    # bigquery project ids are returned in the host field
    db = looker_connection.host
    schema = looker_connection.database
    return (platform, db, schema)


def _get_generic_definition(
    looker_connection: DBConnection, platform: Optional[str] = None
) -> Tuple[str, Optional[str], Optional[str]]:
    if platform is None:
        # We extract the platform from the dialect name
        dialect_name = looker_connection.dialect_name
        assert dialect_name is not None
        # generally the first part of the dialect name before _ is the name of the platform
        # versions are encoded as numbers and can be removed
        # e.g. spark1 or hive2 or druid_18
        platform = re.sub(r"[0-9]+", "", dialect_name.split("_")[0])

    assert (
        platform is not None
    ), f"Failed to extract a valid platform from connection {looker_connection}"
    db = looker_connection.database
    schema = looker_connection.schema  # ok for this to be None
    return (platform, db, schema)


class LookerConnectionDefinition(ConfigModel):
    platform: str
    default_db: str
    default_schema: Optional[str]  # Optional since some sources are two-level only
    platform_instance: Optional[str] = None
    platform_env: Optional[str] = Field(
        default=None,
        description="The environment that the platform is located in. Leaving this empty will inherit defaults from "
        "the top level Looker configuration",
    )

    @validator("platform_env")
    def platform_env_must_be_one_of(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return EnvConfigMixin.env_must_be_one_of(v)
        return v

    @validator("platform", "default_db", "default_schema")
    def lower_everything(cls, v):
        """We lower case all strings passed in to avoid casing issues later"""
        if v is not None:
            return v.lower()

    @classmethod
    def from_looker_connection(
        cls, looker_connection: DBConnection
    ) -> "LookerConnectionDefinition":
        """Dialect definitions are here: https://docs.looker.com/setup-and-management/database-config"""
        extractors: Dict[str, Any] = {
            "^bigquery": _get_bigquery_definition,
            ".*": _get_generic_definition,
        }

        if looker_connection.dialect_name is None:
            raise ConfigurationError(
                f"Unable to fetch a fully filled out connection for {looker_connection.name}. Please check your API permissions."
            )
        for extractor_pattern, extracting_function in extractors.items():
            if re.match(extractor_pattern, looker_connection.dialect_name):
                (platform, db, schema) = extracting_function(looker_connection)
                return cls(platform=platform, default_db=db, default_schema=schema)
        raise ConfigurationError(
            f"Could not find an appropriate platform for looker_connection: {looker_connection.name} with dialect: {looker_connection.dialect_name}"
        )
