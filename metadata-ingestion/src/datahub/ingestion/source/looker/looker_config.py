import dataclasses
import os
import re
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Union, cast

import pydantic
from looker_sdk.sdk.api40.models import DBConnection
from pydantic import Field, validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPIConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class NamingPattern(ConfigModel):
    ALLOWED_VARS: ClassVar[List[str]] = []
    REQUIRE_AT_LEAST_ONE_VAR: ClassVar[bool] = True

    pattern: str

    @classmethod
    def __get_validators__(cls):
        yield cls.pydantic_accept_raw_pattern
        yield cls.validate
        yield cls.pydantic_validate_pattern

    @classmethod
    def pydantic_accept_raw_pattern(cls, v):
        if isinstance(v, (NamingPattern, dict)):
            return v
        assert isinstance(v, str), "pattern must be a string"
        return {"pattern": v}

    @classmethod
    def pydantic_validate_pattern(cls, v):
        assert isinstance(v, NamingPattern)
        assert v.validate_pattern(cls.REQUIRE_AT_LEAST_ONE_VAR)
        return v

    @classmethod
    def allowed_docstring(cls) -> str:
        return f"Allowed variables are {cls.ALLOWED_VARS}"

    def validate_pattern(self, at_least_one: bool) -> bool:
        variables = re.findall("({[^}{]+})", self.pattern)

        variables = [v[1:-1] for v in variables]  # remove the {}

        for v in variables:
            if v not in self.ALLOWED_VARS:
                raise ValueError(
                    f"Failed to find {v} in allowed_variables {self.ALLOWED_VARS}"
                )
        if at_least_one and len(variables) == 0:
            raise ValueError(
                f"Failed to find any variable assigned to pattern {self.pattern}. Must have at least one. {self.allowed_docstring()}"
            )
        return True

    def replace_variables(self, values: Union[Dict[str, Optional[str]], object]) -> str:
        if not isinstance(values, dict):
            # Check that this is a dataclass instance (not a dataclass type).
            assert dataclasses.is_dataclass(values) and not isinstance(values, type)
            values = dataclasses.asdict(values)
        values = {k: v for k, v in values.items() if v is not None}
        return self.pattern.format(**values)


@dataclasses.dataclass
class NamingPatternMapping:
    platform: str
    env: str
    project: str
    model: str
    name: str


@dataclasses.dataclass
class ViewNamingPatternMapping(NamingPatternMapping):
    file_path: str
    folder_path: str


class LookerNamingPattern(NamingPattern):
    ALLOWED_VARS = [field.name for field in dataclasses.fields(NamingPatternMapping)]


class LookerViewNamingPattern(NamingPattern):
    ALLOWED_VARS = [
        field.name for field in dataclasses.fields(ViewNamingPatternMapping)
    ]


# TODO: deprecate browse_pattern configs
class LookerCommonConfig(EnvConfigMixin, PlatformInstanceConfigMixin):
    explore_naming_pattern: LookerNamingPattern = pydantic.Field(
        description=f"Pattern for providing dataset names to explores. {LookerNamingPattern.allowed_docstring()}",
        default=LookerNamingPattern(pattern="{model}.explore.{name}"),
    )
    explore_browse_pattern: LookerNamingPattern = pydantic.Field(
        description=f"Pattern for providing browse paths to explores. {LookerNamingPattern.allowed_docstring()}",
        default=LookerNamingPattern(pattern="/Explore/{model}"),
    )
    view_naming_pattern: LookerViewNamingPattern = Field(
        LookerViewNamingPattern(pattern="{project}.view.{name}"),
        description=f"Pattern for providing dataset names to views. {LookerViewNamingPattern.allowed_docstring()}",
    )
    view_browse_pattern: LookerViewNamingPattern = Field(
        LookerViewNamingPattern(pattern="/Develop/{project}/{folder_path}"),
        description=f"Pattern for providing browse paths to views. {LookerViewNamingPattern.allowed_docstring()}",
    )

    _deprecate_explore_browse_pattern = pydantic_field_deprecated(
        "explore_browse_pattern"
    )
    _deprecate_view_browse_pattern = pydantic_field_deprecated("view_browse_pattern")

    tag_measures_and_dimensions: bool = Field(
        True,
        description="When enabled, attaches tags to measures, dimensions and dimension groups to make them more "
        "discoverable. When disabled, adds this information to the description of the column.",
    )
    platform_name: str = Field(
        # TODO: This shouldn't be part of the config.
        "looker",
        description="Default platform name.",
        hidden_from_docs=True,
    )
    extract_column_level_lineage: bool = Field(
        True,
        description="When enabled, extracts column-level lineage from Views and Explores",
    )


def _get_bigquery_definition(
    looker_connection: DBConnection,
) -> Tuple[str, Optional[str], Optional[str]]:
    platform = "bigquery"
    # bigquery project ids are returned in the host field
    db = looker_connection.host
    schema = looker_connection.database
    return platform, db, schema


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

    assert platform is not None, (
        f"Failed to extract a valid platform from connection {looker_connection}"
    )
    db = looker_connection.database
    schema = looker_connection.schema  # ok for this to be None
    return platform, db, schema


class LookerConnectionDefinition(ConfigModel):
    platform: str
    default_db: str
    default_schema: Optional[str] = (
        None  # Optional since some sources are two-level only
    )
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


class LookerDashboardSourceConfig(
    LookerAPIConfig,
    LookerCommonConfig,
    StatefulIngestionConfigBase,
    EnvConfigMixin,
):
    _removed_github_info = pydantic_removed_field("github_info")

    dashboard_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting dashboard ids that are to be included",
    )
    chart_pattern: AllowDenyPattern = Field(
        AllowDenyPattern.allow_all(),
        description="Patterns for selecting chart ids that are to be included",
    )
    include_deleted: bool = Field(
        False,
        description="Whether to include deleted dashboards and looks.",
    )
    extract_owners: bool = Field(
        True,
        description="When enabled, extracts ownership from Looker directly. When disabled, ownership is left empty "
        "for dashboards and charts.",
    )
    strip_user_ids_from_email: bool = Field(
        False,
        description="When enabled, converts Looker user emails of the form name@domain.com to urn:li:corpuser:name "
        "when assigning ownership",
    )
    skip_personal_folders: bool = Field(
        False,
        description="Whether to skip ingestion of dashboards in personal folders. Setting this to True will only "
        "ingest dashboards in the Shared folder space.",
    )
    max_threads: int = Field(
        default_factory=lambda: os.cpu_count() or 40,
        description="Max parallelism for Looker API calls. Defaults to cpuCount or 40",
    )
    external_base_url: Optional[str] = Field(
        None,
        description="Optional URL to use when constructing external URLs to Looker if the `base_url` is not the "
        "correct one to use. For example, `https://looker-public.company.com`. If not provided, "
        "the external base URL will default to `base_url`.",
    )
    extract_usage_history: bool = Field(
        True,
        description="Whether to ingest usage statistics for dashboards. Setting this to True will query looker system "
        "activity explores to fetch historical dashboard usage.",
    )
    # TODO - stateful ingestion to autodetect usage history interval
    extract_usage_history_for_interval: str = Field(
        "30 days",
        description="Used only if extract_usage_history is set to True. Interval to extract looker dashboard usage "
        "history for. See https://docs.looker.com/reference/filter-expressions#date_and_time.",
    )
    extract_embed_urls: bool = Field(
        True,
        description="Produce URLs used to render Looker Explores as Previews inside of DataHub UI. Embeds must be "
        "enabled inside of Looker to use this feature.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description=""
    )
    extract_independent_looks: bool = Field(
        False,
        description="Extract looks which are not part of any Dashboard. To enable this flag the stateful_ingestion "
        "should also be enabled.",
    )
    emit_used_explores_only: bool = Field(
        True,
        description="When enabled, only explores that are used by a Dashboard/Look will be ingested.",
    )
    include_platform_instance_in_urns: bool = Field(
        False,
        description="When enabled, platform instance will be added in dashboard and chart urn.",
    )

    folder_path_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Allow or deny dashboards from specific folders using their fully qualified paths. "
        "For example: \n"
        "deny: \n"
        " - Shared/deprecated \n"
        "This pattern will deny the ingestion of all dashboards and looks within the Shared/deprecated folder. \n"
        "allow: \n"
        " - Shared/sales \n"
        "This pattern will allow only the ingestion of dashboards within the Shared/sales folder. \n"
        "To get the correct path from Looker, take the folder hierarchy shown in the UI and join it with slashes. "
        "For example, Shared -> Customer Reports -> Sales becomes Shared/Customer Reports/Sales. "
        "Dashboards will only be ingested if they're allowed by both this config and dashboard_pattern.",
    )

    @validator("external_base_url", pre=True, always=True)
    def external_url_defaults_to_api_config_base_url(
        cls, v: Optional[str], *, values: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> Optional[str]:
        return v or values.get("base_url")

    @validator("extract_independent_looks", always=True)
    def stateful_ingestion_should_be_enabled(
        cls, v: Optional[bool], *, values: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> Optional[bool]:
        stateful_ingestion: StatefulStaleMetadataRemovalConfig = cast(
            StatefulStaleMetadataRemovalConfig, values.get("stateful_ingestion")
        )
        if v is True and (
            stateful_ingestion is None or stateful_ingestion.enabled is False
        ):
            raise ValueError("stateful_ingestion.enabled should be set to true")

        return v
