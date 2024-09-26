from enum import auto
from typing import Any, Dict, Optional

import pydantic
from pydantic import root_validator, validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigEnum, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter import mce_builder
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.dbt.target_platform_config import DBTTargetPlatformMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

DBT_PLATFORM = "dbt"


class EmitDirective(ConfigEnum):
    """A holder for directives for emission for specific types of entities"""

    YES = auto()  # Okay to emit for this type
    NO = auto()  # Do not emit for this type
    ONLY = auto()  # Only emit metadata for this type and no others


class DBTEntitiesEnabled(ConfigModel):
    """Controls which dbt entities are going to be emitted by this source"""

    models: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for dbt models when set to Yes or Only",
    )
    sources: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for dbt sources when set to Yes or Only",
    )
    seeds: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for dbt seeds when set to Yes or Only",
    )
    snapshots: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for dbt snapshots when set to Yes or Only",
    )
    test_definitions: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for test definitions when enabled when set to Yes or Only",
    )

    test_results: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for test results when set to Yes or Only",
    )
    model_performance: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit model performance metadata when set to Yes or Only. "
        "Only supported with dbt core.",
    )

    @root_validator(skip_on_failure=True)
    def process_only_directive(cls, values):
        # Checks that at most one is set to ONLY, and then sets the others to NO.

        only_values = [k for k in values if values.get(k) == EmitDirective.ONLY]
        if len(only_values) > 1:
            raise ValueError(
                f"Cannot have more than 1 type of entity emission set to ONLY. Found {only_values}"
            )

        if len(only_values) == 1:
            for k in values:
                values[k] = EmitDirective.NO
            values[only_values[0]] = EmitDirective.YES

        return values

    def _node_type_allow_map(self):
        # Node type comes from dbt's node types.
        return {
            "model": self.models,
            "source": self.sources,
            "seed": self.seeds,
            "snapshot": self.snapshots,
            "test": self.test_definitions,
        }

    def can_emit_node_type(self, node_type: str) -> bool:
        allowed = self._node_type_allow_map().get(node_type)
        if allowed is None:
            return False

        return allowed == EmitDirective.YES

    @property
    def can_emit_test_definitions(self) -> bool:
        return self.test_definitions == EmitDirective.YES

    @property
    def can_emit_test_results(self) -> bool:
        return self.test_results == EmitDirective.YES

    def is_only_test_results(self) -> bool:
        return self.test_results == EmitDirective.YES and all(
            v == EmitDirective.NO for v in self._node_type_allow_map().values()
        )

    @property
    def can_emit_model_performance(self) -> bool:
        return self.model_performance == EmitDirective.YES


class DBTCommonConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    IncrementalLineageConfigMixin,
    DBTTargetPlatformMixin,
):
    env: str = Field(
        default=mce_builder.DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )

    use_identifiers: bool = Field(
        default=False,
        description="Use model identifier instead of model name if defined (if not, default to model name).",
    )
    _deprecate_use_identifiers = pydantic_field_deprecated(
        "use_identifiers", warn_if_value_is_not=False
    )

    entities_enabled: DBTEntitiesEnabled = Field(
        DBTEntitiesEnabled(),
        description="Controls for enabling / disabling metadata emission for different dbt entities (models, test definitions, test results, etc.)",
    )
    prefer_sql_parser_lineage: bool = Field(
        default=False,
        description="Normally we use dbt's metadata to generate table lineage. When enabled, we prefer results from the SQL parser when generating lineage instead. "
        "This can be useful when dbt models reference tables directly, instead of using the ref() macro. "
        "This requires that `skip_sources_in_lineage` is enabled.",
    )
    skip_sources_in_lineage: bool = Field(
        default=False,
        description="[Experimental] When enabled, dbt sources will not be included in the lineage graph. "
        "Requires that `entities_enabled.sources` is set to `NO`. "
        "This is mainly useful when you have multiple, interdependent dbt projects. ",
    )
    tag_prefix: str = Field(
        default=f"{DBT_PLATFORM}:", description="Prefix added to tags during ingestion."
    )
    node_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for dbt model names to filter in ingestion.",
    )
    meta_mapping: Dict = Field(
        default={},
        description="mapping rules that will be executed against dbt meta properties. Refer to the section below on dbt meta automated mappings.",
    )
    column_meta_mapping: Dict = Field(
        default={},
        description="mapping rules that will be executed against dbt column meta properties. Refer to the section below on dbt meta automated mappings.",
    )
    enable_meta_mapping: bool = Field(
        default=True,
        description="When enabled, applies the mappings that are defined through the meta_mapping directives.",
    )
    query_tag_mapping: Dict = Field(
        default={},
        description="mapping rules that will be executed against dbt query_tag meta properties. Refer to the section below on dbt meta automated mappings.",
    )
    enable_query_tag_mapping: bool = Field(
        default=True,
        description="When enabled, applies the mappings that are defined through the `query_tag_mapping` directives.",
    )
    write_semantics: str = Field(
        # TODO: Replace with the WriteSemantics enum.
        default="PATCH",
        description='Whether the new tags, terms and owners to be added will override the existing ones added only by this source or not. Value for this config can be "PATCH" or "OVERRIDE"',
    )
    strip_user_ids_from_email: bool = Field(
        default=False,
        description="Whether or not to strip email id while adding owners using dbt meta actions.",
    )
    enable_owner_extraction: bool = Field(
        default=True,
        description="When enabled, ownership info will be extracted from the dbt meta",
    )
    owner_extraction_pattern: Optional[str] = Field(
        default=None,
        description='Regex string to extract owner from the dbt node using the `(?P<name>...) syntax` of the [match object](https://docs.python.org/3/library/re.html#match-objects), where the group name must be `owner`. Examples: (1)`r"(?P<owner>(.*)): (\\w+) (\\w+)"` will extract `jdoe` as the owner from `"jdoe: John Doe"` (2) `r"@(?P<owner>(.*))"` will extract `alice` as the owner from `"@alice"`.',
    )

    include_env_in_assertion_guid: bool = Field(
        default=False,
        description="Prior to version 0.9.4.2, the assertion GUIDs did not include the environment. If you're using multiple dbt ingestion "
        "that are only distinguished by env, then you should set this flag to True.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="DBT Stateful Ingestion Config."
    )
    convert_column_urns_to_lowercase: bool = Field(
        default=False,
        description="When enabled, converts column URNs to lowercase to ensure cross-platform compatibility. "
        "If `target_platform` is Snowflake, the default is True.",
    )
    test_warnings_are_errors: bool = Field(
        default=False,
        description="When enabled, dbt test warnings will be treated as failures.",
    )
    infer_dbt_schemas: bool = Field(
        default=True,
        description="When enabled, schemas will be inferred from the dbt node definition.",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="When enabled, column-level lineage will be extracted from the dbt node definition. Requires `infer_dbt_schemas` to be enabled. "
        "If you run into issues where the column name casing does not match up with properly, providing a datahub_api or using the rest sink will improve accuracy.",
    )
    # override default value to True.
    incremental_lineage: bool = Field(
        default=True,
        description="When enabled, emits incremental/patch lineage for non-dbt entities. When disabled, re-states lineage on each run.",
    )

    _remove_use_compiled_code = pydantic_removed_field("use_compiled_code")

    include_compiled_code: bool = Field(
        default=True,
        description="When enabled, includes the compiled code in the emitted metadata.",
    )

    @validator("target_platform")
    def validate_target_platform_value(cls, target_platform: str) -> str:
        if target_platform.lower() == DBT_PLATFORM:
            raise ValueError(
                "target_platform cannot be dbt. It should be the platform which dbt is operating on top of. For e.g "
                "postgres."
            )
        return target_platform

    @root_validator(pre=True)
    def set_convert_column_urns_to_lowercase_default_for_snowflake(
        cls, values: dict
    ) -> dict:
        if values.get("target_platform", "").lower() == "snowflake":
            values.setdefault("convert_column_urns_to_lowercase", True)
        return values

    @validator("write_semantics")
    def validate_write_semantics(cls, write_semantics: str) -> str:
        if write_semantics.lower() not in {"patch", "override"}:
            raise ValueError(
                "write_semantics cannot be any other value than PATCH or OVERRIDE. Default value is PATCH. "
                "For PATCH semantics consider using the datahub-rest sink or "
                "provide a datahub_api: configuration on your ingestion recipe"
            )
        return write_semantics

    @validator("meta_mapping")
    def meta_mapping_validator(
        cls, meta_mapping: Dict[str, Any], values: Dict, **kwargs: Any
    ) -> Dict[str, Any]:
        for k, v in meta_mapping.items():
            if "match" not in v:
                raise ValueError(
                    f"meta_mapping section {k} doesn't have a match clause."
                )
            if "operation" not in v:
                raise ValueError(
                    f"meta_mapping section {k} doesn't have an operation clause."
                )
            if v["operation"] == "add_owner":
                owner_category = v["config"].get("owner_category")
                if owner_category:
                    mce_builder.validate_ownership_type(owner_category)
        return meta_mapping

    @validator("include_column_lineage")
    def validate_include_column_lineage(
        cls, include_column_lineage: bool, values: Dict
    ) -> bool:
        if include_column_lineage and not values.get("infer_dbt_schemas"):
            raise ValueError(
                "`infer_dbt_schemas` must be enabled to use `include_column_lineage`"
            )

        return include_column_lineage

    @validator("skip_sources_in_lineage", always=True)
    def validate_skip_sources_in_lineage(
        cls, skip_sources_in_lineage: bool, values: Dict
    ) -> bool:
        entities_enabled: Optional[DBTEntitiesEnabled] = values.get("entities_enabled")
        prefer_sql_parser_lineage: Optional[bool] = values.get(
            "prefer_sql_parser_lineage"
        )

        if prefer_sql_parser_lineage and not skip_sources_in_lineage:
            raise ValueError(
                "`prefer_sql_parser_lineage` requires that `skip_sources_in_lineage` is enabled."
            )

        if (
            skip_sources_in_lineage
            and entities_enabled
            and entities_enabled.sources == EmitDirective.YES
            # When `prefer_sql_parser_lineage` is enabled, it's ok to have `skip_sources_in_lineage` enabled
            # without also disabling sources.
            and not prefer_sql_parser_lineage
        ):
            raise ValueError(
                "When `skip_sources_in_lineage` is enabled, `entities_enabled.sources` must be set to NO."
            )

        return skip_sources_in_lineage
