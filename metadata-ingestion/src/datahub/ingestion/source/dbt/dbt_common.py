import logging
import re
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import auto
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import more_itertools
import pydantic
from pydantic import root_validator, validator
from pydantic.fields import Field

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigEnum,
    ConfigModel,
    ConfigurationError,
)
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.dbt.dbt_tests import (
    DBTTest,
    DBTTestResult,
    make_assertion_from_test,
    make_assertion_result_from_test,
)
from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    GlossaryTermAssociation,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    NullTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.schema_resolver import SchemaInfo, SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingDebugInfo,
    SqlParsingResult,
    infer_output_schema,
    sqlglot_lineage,
)
from datahub.sql_parsing.sqlglot_utils import (
    detach_ctes,
    parse_statements_and_pick,
    try_format_query,
)
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.mapping import Constants, OperationProcessor
from datahub.utilities.time import datetime_to_ts_millis
from datahub.utilities.topological_sort import topological_sort

logger = logging.getLogger(__name__)
DBT_PLATFORM = "dbt"

_DEFAULT_ACTOR = mce_builder.make_user_urn("unknown")


@dataclass
class DBTSourceReport(StaleEntityRemovalSourceReport):
    sql_parser_skipped_missing_code: LossyList[str] = field(default_factory=LossyList)
    sql_parser_skipped_non_sql_model: LossyList[str] = field(default_factory=LossyList)
    sql_parser_parse_failures: int = 0
    sql_parser_detach_ctes_failures: int = 0
    sql_parser_table_errors: int = 0
    sql_parser_column_errors: int = 0
    sql_parser_successes: int = 0

    # Details on where column info comes from.
    nodes_with_catalog_columns: int = 0
    nodes_with_inferred_columns: int = 0
    nodes_with_graph_columns: int = 0
    nodes_with_no_columns: int = 0

    sql_parser_parse_failures_list: LossyList[str] = field(default_factory=LossyList)
    sql_parser_detach_ctes_failures_list: LossyList[str] = field(
        default_factory=LossyList
    )

    nodes_filtered: LossyList[str] = field(default_factory=LossyList)


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
):
    env: str = Field(
        default=mce_builder.DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )
    target_platform: str = Field(
        description="The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)",
    )
    target_platform_instance: Optional[str] = Field(
        default=None,
        description="The platform instance for the platform that dbt is operating on. Use this if you have multiple instances of the same platform (e.g. redshift) and need to distinguish between them.",
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
    include_database_name: bool = Field(
        default=True,
        description="Whether to add database name to the table urn. "
        "Set to False to skip it for engines like AWS Athena where it's not required.",
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


@dataclass
class DBTColumn:
    name: str
    comment: str
    description: str
    index: int
    data_type: str

    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    datahub_data_type: Optional[SchemaFieldDataType] = None


@dataclass
class DBTColumnLineageInfo:
    upstream_dbt_name: str

    upstream_col: str
    downstream_col: str


@dataclass
class DBTModelPerformance:
    # This is specifically for model/snapshot builds.

    run_id: str
    status: str
    start_time: datetime
    end_time: datetime

    def is_success(self) -> bool:
        return self.status == "success"


@dataclass
class DBTNode:
    """
    The DBTNode is generated by joining data from the manifest and catalog files.
    It can contain source/model/seed/test nodes, and models can have a variety of
    materialization types.
    """

    database: Optional[str]
    schema: Optional[str]
    name: str  # name, identifier
    alias: Optional[str]  # alias if present
    comment: str
    description: str
    language: Optional[str]
    raw_code: Optional[str]

    dbt_adapter: str
    dbt_name: str
    dbt_file_path: Optional[str]
    dbt_package_name: Optional[str]  # this is pretty much always present

    node_type: str  # source, model, snapshot, seed, test, etc
    max_loaded_at: Optional[datetime]
    materialization: Optional[str]  # table, view, ephemeral, incremental, snapshot
    # see https://docs.getdbt.com/reference/artifacts/manifest-json
    catalog_type: Optional[str]
    missing_from_catalog: (
        bool  # indicates if the node was missing from the catalog.json
    )

    owner: Optional[str]

    columns: List[DBTColumn] = field(default_factory=list)
    upstream_nodes: List[str] = field(default_factory=list)  # list of upstream dbt_name
    upstream_cll: List[DBTColumnLineageInfo] = field(default_factory=list)
    raw_sql_parsing_result: Optional[SqlParsingResult] = (
        None  # only set for nodes that don't depend on ephemeral models
    )
    cll_debug_info: Optional[SqlParsingDebugInfo] = None

    meta: Dict[str, Any] = field(default_factory=dict)
    query_tag: Dict[str, Any] = field(default_factory=dict)

    tags: List[str] = field(default_factory=list)
    compiled_code: Optional[str] = None

    test_info: Optional["DBTTest"] = None  # only populated if node_type == 'test'
    test_results: List["DBTTestResult"] = field(default_factory=list)

    model_performances: List["DBTModelPerformance"] = field(default_factory=list)

    @staticmethod
    def _join_parts(parts: List[Optional[str]]) -> str:
        joined = ".".join([part for part in parts if part])
        assert joined
        return joined

    def get_db_fqn(self) -> str:
        # Database might be None, but schema and name should always be present.
        fqn = self._join_parts([self.database, self.schema, self.name])
        return fqn.replace('"', "")

    def get_urn(
        self,
        target_platform: str,
        env: str,
        # If target_platform = dbt, this is the dbt platform instance.
        # Otherwise, it's the target platform instance.
        data_platform_instance: Optional[str],
    ) -> str:
        db_fqn = self.get_db_fqn()
        if target_platform != DBT_PLATFORM:
            db_fqn = db_fqn.lower()
        return mce_builder.make_dataset_urn_with_platform_instance(
            platform=target_platform,
            name=db_fqn,
            platform_instance=data_platform_instance,
            env=env,
        )

    def is_ephemeral_model(self) -> bool:
        return self.materialization == "ephemeral"

    def get_fake_ephemeral_table_name(self) -> str:
        assert self.is_ephemeral_model()

        # Similar to get_db_fqn.
        db_fqn = self._join_parts(
            [self.database, self.schema, f"__datahub__dbt__ephemeral__{self.name}"]
        )
        db_fqn = db_fqn.lower()
        return db_fqn.replace('"', "")

    def get_urn_for_upstream_lineage(
        self,
        dbt_platform_instance: Optional[str],
        target_platform: str,
        target_platform_instance: Optional[str],
        env: str,
        skip_sources_in_lineage: bool,
    ) -> str:
        """
        Get the urn to use when referencing this node in a dbt node's upstream lineage.

        If the node is an ephemeral dbt node, we should point at the dbt node.
        If the node is a source node, and skip_sources_in_lineage is not enabled, we should also point at the dbt node.
        Otherwise, the node is materialized in the target platform, and so lineage should
        point there.
        """
        # TODO: This logic shouldn't live in the DBTNode class. It should be moved to the source.

        platform_value = DBT_PLATFORM
        platform_instance_value = dbt_platform_instance

        if self.is_ephemeral_model():
            pass  # leave it pointing at dbt
        elif self.node_type == "source" and not skip_sources_in_lineage:
            pass  # leave it as dbt
        else:
            # upstream urns point to the target platform
            platform_value = target_platform
            platform_instance_value = target_platform_instance

        return self.get_urn(
            target_platform=platform_value,
            env=env,
            data_platform_instance=platform_instance_value,
        )

    @property
    def exists_in_target_platform(self):
        return not (self.is_ephemeral_model() or self.node_type == "test")

    def set_columns(self, schema_fields: List[SchemaField]) -> None:
        """Update the column list."""

        self.columns = [
            DBTColumn(
                name=schema_field.fieldPath,
                comment="",
                description="",
                index=i,
                data_type=schema_field.nativeDataType,
                datahub_data_type=schema_field.type,
            )
            for i, schema_field in enumerate(schema_fields)
        ]


def get_custom_properties(node: DBTNode) -> Dict[str, str]:
    # initialize custom properties to node's meta props
    # (dbt-native node properties)
    custom_properties = node.meta

    # additional node attributes to extract to custom properties
    node_attributes = {
        "node_type": node.node_type,
        "materialization": node.materialization,
        "dbt_file_path": node.dbt_file_path,
        "catalog_type": node.catalog_type,
        "language": node.language,
        "dbt_unique_id": node.dbt_name,
        "dbt_package_name": node.dbt_package_name,
    }

    for attribute, node_attribute_value in node_attributes.items():
        if node_attribute_value is not None:
            custom_properties[attribute] = node_attribute_value

    custom_properties = {key: str(value) for key, value in custom_properties.items()}

    return custom_properties


def _get_dbt_cte_names(name: str, target_platform: str) -> List[str]:
    # Match the dbt CTE naming scheme:
    # The default is defined here https://github.com/dbt-labs/dbt-core/blob/4122f6c308c88be4a24c1ea490802239a4c1abb8/core/dbt/adapters/base/relation.py#L222
    # However, since this PR https://github.com/dbt-labs/dbt-core/pull/2712, it's also possible
    # for adapters to override this default. Only a handful actually do though:
    # https://github.com/search?type=code&q=add_ephemeral_prefix+path:/%5Edbt%5C/adapters%5C//

    # Regardless, we need to keep the original name to work with older dbt versions.
    default_cte_name = f"__dbt__cte__{name}"

    adapter_cte_names = {
        "hive": f"tmp__dbt__cte__{name}",
        "oracle": f"dbt__cte__{name}__",
        "netezza": f"dbt__cte__{name}",
        "exasol": f"dbt__CTE__{name}",
        "db2": f"DBT_CTE__{name}",  # ibm db2
    }

    cte_names = [default_cte_name]
    if target_platform in adapter_cte_names:
        cte_names.append(adapter_cte_names[target_platform])

    return cte_names


def get_upstreams(
    upstreams: List[str],
    all_nodes: Dict[str, DBTNode],
    target_platform: str,
    target_platform_instance: Optional[str],
    environment: str,
    platform_instance: Optional[str],
    skip_sources_in_lineage: bool,
) -> List[str]:
    upstream_urns = []

    for upstream in sorted(upstreams):
        if upstream not in all_nodes:
            logger.debug(
                f"Upstream node - {upstream} not found in all manifest entities."
            )
            continue

        upstream_manifest_node = all_nodes[upstream]

        # This logic creates lineages among dbt nodes.
        upstream_urns.append(
            upstream_manifest_node.get_urn_for_upstream_lineage(
                dbt_platform_instance=platform_instance,
                target_platform=target_platform,
                target_platform_instance=target_platform_instance,
                env=environment,
                skip_sources_in_lineage=skip_sources_in_lineage,
            )
        )
    return upstream_urns


def get_upstreams_for_test(
    test_node: DBTNode,
    all_nodes_map: Dict[str, DBTNode],
    platform_instance: Optional[str],
    environment: str,
) -> Dict[str, str]:
    upstreams = {}

    for upstream in test_node.upstream_nodes:
        if upstream not in all_nodes_map:
            logger.debug(
                f"Upstream node of test {upstream} not found in all manifest entities."
            )
            continue

        upstream_manifest_node = all_nodes_map[upstream]

        upstreams[upstream] = upstream_manifest_node.get_urn(
            target_platform=DBT_PLATFORM,
            data_platform_instance=platform_instance,
            env=environment,
        )

    return upstreams


def make_mapping_upstream_lineage(
    upstream_urn: str,
    downstream_urn: str,
    node: DBTNode,
    convert_column_urns_to_lowercase: bool,
    skip_sources_in_lineage: bool,
) -> UpstreamLineageClass:
    cll = []
    if not (node.node_type == "source" and skip_sources_in_lineage):
        # If `skip_sources_in_lineage` is enabled, we want to generate table lineage (for siblings)
        # but not CLL. That's because CLL will make it look like the warehouse node has downstream
        # column lineage, but it's really just empty.
        for column in node.columns or []:
            field_name = column.name
            if convert_column_urns_to_lowercase:
                field_name = field_name.lower()

            cll.append(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        mce_builder.make_schema_field_urn(upstream_urn, field_name)
                    ],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[
                        mce_builder.make_schema_field_urn(downstream_urn, field_name)
                    ],
                )
            )

    return UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream_urn,
                type=DatasetLineageTypeClass.COPY,
                auditStamp=AuditStamp(
                    time=mce_builder.get_sys_time(), actor=_DEFAULT_ACTOR
                ),
            )
        ],
        fineGrainedLineages=cll or None,
    )


def get_column_type(
    report: DBTSourceReport,
    dataset_name: str,
    column_type: Optional[str],
    dbt_adapter: str,
) -> SchemaFieldDataType:
    """
    Maps known DBT types to datahub types
    """

    TypeClass = resolve_sql_type(column_type, dbt_adapter)

    # if still not found, report a warning
    if TypeClass is None:
        if column_type:
            report.info(
                title="Unable to map column types to DataHub types",
                message="Got an unexpected column type. The column's parsed field type will not be populated.",
                context=f"{dataset_name} - {column_type}",
                log=False,
            )
        TypeClass = NullTypeClass()

    return SchemaFieldDataType(type=TypeClass)


@platform_name("dbt")
@config_class(DBTCommonConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configure using `include_column_lineage`",
)
class DBTSourceBase(StatefulIngestionSourceBase):
    def __init__(self, config: DBTCommonConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.platform: str = "dbt"

        self.config = config
        self.report: DBTSourceReport = DBTSourceReport()

        self.compiled_owner_extraction_pattern: Optional[Any] = None
        if self.config.owner_extraction_pattern:
            self.compiled_owner_extraction_pattern = re.compile(
                self.config.owner_extraction_pattern
            )
        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, ctx
        )

    def create_test_entity_mcps(
        self,
        test_nodes: List[DBTNode],
        extra_custom_props: Dict[str, str],
        all_nodes_map: Dict[str, DBTNode],
    ) -> Iterable[MetadataChangeProposalWrapper]:
        for node in sorted(test_nodes, key=lambda n: n.dbt_name):
            upstreams = get_upstreams_for_test(
                test_node=node,
                all_nodes_map=all_nodes_map,
                platform_instance=self.config.platform_instance,
                environment=self.config.env,
            )

            # In case a dbt test depends on multiple tables, we create separate assertions for each.
            for upstream_node_name, upstream_urn in upstreams.items():
                guid_upstream_part = {}
                if len(upstreams) > 1:
                    # If we depend on multiple upstreams, we need to generate a unique guid for each assertion.
                    # If there was only one upstream, we want to maintain the original assertion for backwards compatibility.
                    guid_upstream_part = {
                        "on_dbt_upstream": upstream_node_name,
                    }

                assertion_urn = mce_builder.make_assertion_urn(
                    mce_builder.datahub_guid(
                        {
                            k: v
                            for k, v in {
                                "platform": DBT_PLATFORM,
                                "name": node.dbt_name,
                                "instance": self.config.platform_instance,
                                # Ideally we'd include the env unconditionally. However, we started out
                                # not including env in the guid, so we need to maintain backwards compatibility
                                # with existing PROD assertions.
                                **(
                                    {"env": self.config.env}
                                    if self.config.env != mce_builder.DEFAULT_ENV
                                    and self.config.include_env_in_assertion_guid
                                    else {}
                                ),
                                **guid_upstream_part,
                            }.items()
                            if v is not None
                        }
                    )
                )

                custom_props = {
                    "dbt_unique_id": node.dbt_name,
                    "dbt_test_upstream_unique_id": upstream_node_name,
                    **extra_custom_props,
                }

                if self.config.entities_enabled.can_emit_test_definitions:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=assertion_urn,
                        aspect=self._make_data_platform_instance_aspect(),
                    )

                    yield make_assertion_from_test(
                        custom_props,
                        node,
                        assertion_urn,
                        upstream_urn,
                    )

                for test_result in node.test_results:
                    if self.config.entities_enabled.can_emit_test_results:
                        yield make_assertion_result_from_test(
                            node,
                            test_result,
                            assertion_urn,
                            upstream_urn,
                            test_warnings_are_errors=self.config.test_warnings_are_errors,
                        )
                    else:
                        logger.debug(
                            f"Skipping test result {node.name} ({test_result.invocation_id}) emission since it is turned off."
                        )

    @abstractmethod
    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        # return dbt nodes + global custom properties
        raise NotImplementedError()

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def _make_data_platform_instance_aspect(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=mce_builder.make_data_platform_urn(DBT_PLATFORM),
            instance=(
                mce_builder.make_dataplatform_instance_urn(
                    mce_builder.make_data_platform_urn(DBT_PLATFORM),
                    self.config.platform_instance,
                )
                if self.config.platform_instance
                else None
            ),
        )

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, MetadataChangeProposalWrapper]]:
        if self.config.write_semantics == "PATCH":
            self.ctx.require_graph("Using dbt with write_semantics=PATCH")

        all_nodes, additional_custom_props = self.load_nodes()

        all_nodes_map = {node.dbt_name: node for node in all_nodes}
        additional_custom_props_filtered = {
            key: value
            for key, value in additional_custom_props.items()
            if value is not None
        }

        # We need to run this before filtering nodes, because the info generated
        # for a filtered node may be used by an unfiltered node.
        # NOTE: This method mutates the DBTNode objects directly.
        self._infer_schemas_and_update_cll(all_nodes_map)

        nodes = self._filter_nodes(all_nodes)
        non_test_nodes = [
            dataset_node for dataset_node in nodes if dataset_node.node_type != "test"
        ]
        test_nodes = [test_node for test_node in nodes if test_node.node_type == "test"]

        logger.info(f"Creating dbt metadata for {len(nodes)} nodes")
        yield from self.create_dbt_platform_mces(
            non_test_nodes,
            additional_custom_props_filtered,
            all_nodes_map,
        )

        logger.info(f"Updating {self.config.target_platform} metadata")
        yield from self.create_target_platform_mces(non_test_nodes)

        yield from self.create_test_entity_mcps(
            test_nodes,
            additional_custom_props_filtered,
            all_nodes_map,
        )

    def _is_allowed_node(self, key: str) -> bool:
        return self.config.node_name_pattern.allowed(key)

    def _filter_nodes(self, all_nodes: List[DBTNode]) -> List[DBTNode]:
        nodes = []
        for node in all_nodes:
            key = node.dbt_name

            if not self._is_allowed_node(key):
                self.report.nodes_filtered.append(key)
                continue

            nodes.append(node)

        return nodes

    @staticmethod
    def _to_schema_info(schema_fields: List[SchemaField]) -> SchemaInfo:
        return {column.fieldPath: column.nativeDataType for column in schema_fields}

    def _determine_cll_required_nodes(
        self, all_nodes_map: Dict[str, DBTNode]
    ) -> Tuple[Set[str], Set[str]]:
        # Based on the filter patterns, we only need to do schema inference and CLL
        # for a subset of nodes.
        # If a node depends on an ephemeral model, the ephemeral model should also be in the CLL list.
        # Invariant: If it's in the CLL list, it will also be in the schema list.
        # Invariant: The upstream of any node in the CLL list will be in the schema list.
        schema_nodes: Set[str] = set()
        cll_nodes: Set[str] = set()

        def add_node_to_cll_list(dbt_name: str) -> None:
            if dbt_name in cll_nodes:
                return
            for upstream in all_nodes_map[dbt_name].upstream_nodes:
                schema_nodes.add(upstream)

                upstream_node = all_nodes_map.get(upstream)
                if upstream_node and upstream_node.is_ephemeral_model():
                    add_node_to_cll_list(upstream)

            cll_nodes.add(dbt_name)
            schema_nodes.add(dbt_name)

        for dbt_name in all_nodes_map:
            if self._is_allowed_node(dbt_name):
                add_node_to_cll_list(dbt_name)

        return schema_nodes, cll_nodes

    def _infer_schemas_and_update_cll(  # noqa: C901
        self, all_nodes_map: Dict[str, DBTNode]
    ) -> None:
        """Annotate the DBTNode objects with schema information and column-level lineage.

        Note that this mutates the DBTNode objects directly.

        This method does the following:
        1. Iterate over the dbt nodes in topological order.
        2. For each node, either load the schema from the graph or from the dbt catalog info.
           We also add this schema to the schema resolver.
        3. Run sql parser to infer the schema + generate column lineage.
        4. Write the schema and column lineage back to the DBTNode object.
        5. If we haven't already added the node's schema to the schema resolver, do that.
        """

        if self.config.entities_enabled.is_only_test_results():
            # If we're not emitting any other entities, so there's no need to infer schemas.
            return
        if not self.config.infer_dbt_schemas:
            if self.config.include_column_lineage:
                raise ConfigurationError(
                    "`infer_dbt_schemas` must be enabled to use `include_column_lineage`"
                )
            return

        graph: Optional[DataHubGraph] = self.ctx.graph

        schema_resolver = SchemaResolver(
            platform=self.config.target_platform,
            platform_instance=self.config.target_platform_instance,
            env=self.config.env,
        )

        target_platform_urn_to_dbt_name: Dict[str, str] = {}

        # Iterate over the dbt nodes in topological order.
        # This ensures that we process upstream nodes before downstream nodes.
        all_node_order = topological_sort(
            list(all_nodes_map.keys()),
            edges=list(
                (upstream, node.dbt_name)
                for node in all_nodes_map.values()
                for upstream in node.upstream_nodes
                if upstream in all_nodes_map
            ),
        )
        schema_required_nodes, cll_required_nodes = self._determine_cll_required_nodes(
            all_nodes_map
        )

        for dbt_name in all_node_order:
            if dbt_name not in schema_required_nodes:
                logger.debug(
                    f"Skipping {dbt_name} because it is filtered out by patterns"
                )
                continue

            node = all_nodes_map[dbt_name]
            logger.debug(f"Processing CLL/schemas for {node.dbt_name}")

            target_node_urn = None
            should_fetch_target_node_schema = False
            if node.exists_in_target_platform:
                target_node_urn = node.get_urn(
                    self.config.target_platform,
                    self.config.env,
                    self.config.target_platform_instance,
                )
                should_fetch_target_node_schema = True
            elif node.is_ephemeral_model():
                # For ephemeral nodes, we "pretend" that they exist in the target platform
                # for schema resolution purposes.
                target_node_urn = mce_builder.make_dataset_urn_with_platform_instance(
                    platform=self.config.target_platform,
                    name=node.get_fake_ephemeral_table_name(),
                    platform_instance=self.config.target_platform_instance,
                    env=self.config.env,
                )
            if target_node_urn:
                target_platform_urn_to_dbt_name[target_node_urn] = node.dbt_name

            # Our schema resolver preference is:
            # 1. graph
            # 2. dbt catalog
            # 3. inferred
            # Exception: if convert_column_urns_to_lowercase is enabled, swap 1 and 2.
            # Cases 1 and 2 are handled here, and case 3 is handled after schema inference has occurred.
            schema_fields: Optional[List[SchemaField]] = None

            # Fetch the schema from the graph.
            if target_node_urn and should_fetch_target_node_schema and graph:
                schema_metadata = graph.get_aspect(target_node_urn, SchemaMetadata)
                if schema_metadata:
                    schema_fields = schema_metadata.fields

            # Otherwise, load the schema from the dbt catalog.
            # Note that this might get the casing wrong relative to DataHub, but
            # has a more up-to-date column list.
            if node.columns and (
                not schema_fields or self.config.convert_column_urns_to_lowercase
            ):
                schema_fields = [
                    SchemaField(
                        fieldPath=(
                            column.name.lower()
                            if self.config.convert_column_urns_to_lowercase
                            else column.name
                        ),
                        type=column.datahub_data_type
                        or SchemaFieldDataType(type=NullTypeClass()),
                        nativeDataType=column.data_type,
                    )
                    for column in node.columns
                ]

            # Add the node to the schema resolver, so that we can get column
            # casing to match the upstream platform.
            added_to_schema_resolver = False
            if target_node_urn and schema_fields:
                schema_resolver.add_raw_schema_info(
                    target_node_urn, self._to_schema_info(schema_fields)
                )
                added_to_schema_resolver = True

            # Run sql parser to infer the schema + generate column lineage.
            sql_result = None
            depends_on_ephemeral_models = False
            if node.node_type in {"source", "test", "seed"}:
                # For sources, we generate CLL as a 1:1 mapping.
                # We don't support CLL for tests (assertions) or seeds.
                pass
            elif node.dbt_name not in cll_required_nodes:
                logger.debug(
                    f"Not generating CLL for {node.dbt_name} because we don't need it."
                )
            elif node.language != "sql":
                logger.debug(
                    f"Not generating CLL for {node.dbt_name} because it is not a SQL model."
                )
                self.report.sql_parser_skipped_non_sql_model.append(node.dbt_name)
            elif node.compiled_code:
                # Add CTE stops based on the upstreams list.
                cte_mapping = {
                    cte_name: upstream_node.get_fake_ephemeral_table_name()
                    for upstream_node in [
                        all_nodes_map[upstream_node_name]
                        for upstream_node_name in node.upstream_nodes
                        if upstream_node_name in all_nodes_map
                    ]
                    if upstream_node.is_ephemeral_model()
                    for cte_name in _get_dbt_cte_names(
                        upstream_node.name, schema_resolver.platform
                    )
                }
                if cte_mapping:
                    depends_on_ephemeral_models = True

                sql_result = self._parse_cll(node, cte_mapping, schema_resolver)
            else:
                self.report.sql_parser_skipped_missing_code.append(node.dbt_name)

            # Save the column lineage.
            if self.config.include_column_lineage and sql_result:
                # We save the raw info here. We use this for supporting `prefer_sql_parser_lineage`.
                if not depends_on_ephemeral_models:
                    node.raw_sql_parsing_result = sql_result

                # We use this for error reporting. However, we only want to report errors
                # after node filters are applied.
                node.cll_debug_info = sql_result.debug_info

                if sql_result.column_lineage:
                    node.upstream_cll = [
                        DBTColumnLineageInfo(
                            upstream_dbt_name=target_platform_urn_to_dbt_name[
                                upstream_column.table
                            ],
                            upstream_col=upstream_column.column,
                            downstream_col=column_lineage_info.downstream.column,
                        )
                        for column_lineage_info in sql_result.column_lineage
                        for upstream_column in column_lineage_info.upstreams
                        # Only include the CLL if the table in in the upstream list.
                        # TODO: Add some telemetry around this - how frequently does it filter stuff out?
                        if target_platform_urn_to_dbt_name.get(upstream_column.table)
                        in node.upstream_nodes
                    ]

            # If we didn't fetch the schema from the graph, use the inferred schema.
            inferred_schema_fields = None
            if sql_result:
                inferred_schema_fields = infer_output_schema(sql_result)

            # Conditionally add the inferred schema to the schema resolver.
            if (
                not added_to_schema_resolver
                and target_node_urn
                and inferred_schema_fields
            ):
                schema_resolver.add_raw_schema_info(
                    target_node_urn, self._to_schema_info(inferred_schema_fields)
                )

            # When updating the node's columns, our order of preference is:
            # 1. Schema from the dbt catalog
            # 2. Inferred schema
            # 3. Schema fetched from the graph
            if node.columns:
                self.report.nodes_with_catalog_columns += 1
                pass  # we already have columns from the dbt catalog
            elif inferred_schema_fields:
                logger.debug(
                    f"Using {len(inferred_schema_fields)} inferred columns for {node.dbt_name}"
                )
                self.report.nodes_with_inferred_columns += 1
                node.set_columns(inferred_schema_fields)
            elif schema_fields:
                logger.debug(
                    f"Using {len(schema_fields)} graph columns for {node.dbt_name}"
                )
                self.report.nodes_with_graph_columns += 1
                node.set_columns(schema_fields)
            else:
                logger.debug(f"No columns found for {node.dbt_name}")
                self.report.nodes_with_no_columns += 1

    def _parse_cll(
        self,
        node: DBTNode,
        cte_mapping: Dict[str, str],
        schema_resolver: SchemaResolver,
    ) -> SqlParsingResult:
        assert node.compiled_code is not None

        try:
            picked_statement = parse_statements_and_pick(
                node.compiled_code,
                platform=schema_resolver.platform,
            )
        except Exception as e:
            logger.debug(
                f"Failed to parse compiled code. {node.dbt_name} will not have column lineage."
            )
            self.report.sql_parser_parse_failures += 1
            self.report.sql_parser_parse_failures_list.append(node.dbt_name)
            return SqlParsingResult.make_from_error(e)

        try:
            preprocessed_sql = detach_ctes(
                picked_statement,
                platform=schema_resolver.platform,
                cte_mapping=cte_mapping,
            )
        except Exception as e:
            self.report.sql_parser_detach_ctes_failures += 1
            self.report.sql_parser_detach_ctes_failures_list.append(node.dbt_name)
            logger.debug(
                f"Failed to detach CTEs from compiled code. {node.dbt_name} will not have column lineage."
            )
            return SqlParsingResult.make_from_error(e)

        sql_result = sqlglot_lineage(preprocessed_sql, schema_resolver=schema_resolver)
        if sql_result.debug_info.table_error:
            self.report.sql_parser_table_errors += 1
            logger.info(
                f"Failed to generate any CLL lineage for {node.dbt_name}: {sql_result.debug_info.error}"
            )
        elif sql_result.debug_info.column_error:
            self.report.sql_parser_column_errors += 1
            logger.info(
                f"Failed to generate CLL for {node.dbt_name}: {sql_result.debug_info.column_error}"
            )
        else:
            self.report.sql_parser_successes += 1
        return sql_result

    def create_dbt_platform_mces(
        self,
        dbt_nodes: List[DBTNode],
        additional_custom_props_filtered: Dict[str, str],
        all_nodes_map: Dict[str, DBTNode],
    ) -> Iterable[MetadataWorkUnit]:
        """Create MCEs and MCPs for the dbt platform."""

        action_processor = OperationProcessor(
            self.config.meta_mapping,
            self.config.tag_prefix,
            "SOURCE_CONTROL",
            self.config.strip_user_ids_from_email,
        )

        action_processor_tag = OperationProcessor(
            self.config.query_tag_mapping,
            self.config.tag_prefix,
            "SOURCE_CONTROL",
            self.config.strip_user_ids_from_email,
        )
        for node in sorted(dbt_nodes, key=lambda n: n.dbt_name):
            node_datahub_urn = node.get_urn(
                DBT_PLATFORM,
                self.config.env,
                self.config.platform_instance,
            )

            meta_aspects: Dict[str, Any] = {}
            if self.config.enable_meta_mapping and node.meta:
                meta_aspects = action_processor.process(node.meta)

            if self.config.enable_query_tag_mapping and node.query_tag:
                self.extract_query_tag_aspects(
                    action_processor_tag, meta_aspects, node
                )  # mutates meta_aspects

            aspects = self._generate_base_dbt_aspects(
                node, additional_custom_props_filtered, DBT_PLATFORM, meta_aspects
            )

            # Upstream lineage.
            upstream_lineage_class = self._create_lineage_aspect_for_dbt_node(
                node, all_nodes_map
            )
            if upstream_lineage_class:
                aspects.append(upstream_lineage_class)

            # View properties.
            view_prop_aspect = self._create_view_properties_aspect(node)
            if view_prop_aspect:
                aspects.append(view_prop_aspect)

            # Generate main MCE.
            if self.config.entities_enabled.can_emit_node_type(node.node_type):
                # Subtype.
                sub_type_wu = self._create_subType_wu(node, node_datahub_urn)
                if sub_type_wu:
                    yield sub_type_wu

                # DataPlatformInstance aspect.
                yield MetadataChangeProposalWrapper(
                    entityUrn=node_datahub_urn,
                    aspect=self._make_data_platform_instance_aspect(),
                ).as_workunit()

                standalone_aspects, snapshot_aspects = more_itertools.partition(
                    (
                        lambda aspect: mce_builder.can_add_aspect_to_snapshot(
                            DatasetSnapshot, type(aspect)
                        )
                    ),
                    aspects,
                )
                for aspect in standalone_aspects:
                    # The domains aspect, and some others, may not support being added to the snapshot.
                    yield MetadataChangeProposalWrapper(
                        entityUrn=node_datahub_urn,
                        aspect=aspect,
                    ).as_workunit()

                dataset_snapshot = DatasetSnapshot(
                    urn=node_datahub_urn, aspects=list(snapshot_aspects)
                )
                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                if self.config.write_semantics == "PATCH":
                    mce = self.get_patched_mce(mce)
                yield MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
            else:
                logger.debug(
                    f"Skipping emission of node {node_datahub_urn} because node_type {node.node_type} is disabled"
                )

            # Model performance.
            if self.config.entities_enabled.can_emit_model_performance:
                yield from auto_workunit(
                    self._create_dataprocess_instance_mcps(node, upstream_lineage_class)
                )

    def _create_dataprocess_instance_mcps(
        self,
        node: DBTNode,
        upstream_lineage_class: Optional[UpstreamLineageClass],
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if not node.model_performances:
            return

        node_datahub_urn = node.get_urn(
            DBT_PLATFORM,
            self.config.env,
            self.config.platform_instance,
        )

        for model_performance in node.model_performances:
            data_process_instance = DataProcessInstance(
                # Parts of the urn.
                # platform_instance is already captured as part of the node urn.
                id=f"{model_performance.run_id}_{node_datahub_urn}",
                orchestrator=DBT_PLATFORM,
                cluster=None,
                # Part of relationships.
                template_urn=DatasetUrn.from_string(node_datahub_urn),
                inlets=[
                    DatasetUrn.from_string(upstream.dataset)
                    for upstream in (
                        upstream_lineage_class.upstreams
                        if upstream_lineage_class
                        else []
                    )
                ],
                outlets=[DatasetUrn.from_string(node_datahub_urn)],
                # Part of properties.
                properties={
                    "dbt_name": node.dbt_name,
                    "dbt_urn": node_datahub_urn,
                },
                url=self.get_external_url(node),
            )

            yield from data_process_instance.generate_mcp(
                materialize_iolets=False,
                created_ts_millis=datetime_to_ts_millis(model_performance.start_time),
            )

            yield from data_process_instance.start_event_mcp(
                start_timestamp_millis=datetime_to_ts_millis(
                    model_performance.start_time
                ),
            )
            yield from data_process_instance.end_event_mcp(
                end_timestamp_millis=datetime_to_ts_millis(model_performance.end_time),
                start_timestamp_millis=datetime_to_ts_millis(
                    model_performance.start_time
                ),
                result=(
                    InstanceRunResult.SUCCESS
                    if model_performance.is_success()
                    else InstanceRunResult.FAILURE
                ),
                result_type=model_performance.status,
            )

    def create_target_platform_mces(
        self,
        dbt_nodes: List[DBTNode],
    ) -> Iterable[MetadataWorkUnit]:
        """Create MCEs and MCPs for the target (e.g. Snowflake, BigQuery) platform."""

        mce_platform = self.config.target_platform
        mce_platform_instance = self.config.target_platform_instance

        for node in sorted(dbt_nodes, key=lambda n: n.dbt_name):
            node_datahub_urn = node.get_urn(
                mce_platform,
                self.config.env,
                mce_platform_instance,
            )
            if not self.config.entities_enabled.can_emit_node_type(node.node_type):
                logger.debug(
                    f"Skipping emission of node {node_datahub_urn} because node_type {node.node_type} is disabled"
                )
                continue

            # We are creating empty node for platform and only add lineage/keyaspect.
            if not node.exists_in_target_platform:
                continue

            # This code block is run when we are generating entities of platform type.
            # We will not link the platform not to the dbt node for type "source" because
            # in this case the platform table existed first.
            if node.node_type != "source":
                upstream_dbt_urn = node.get_urn(
                    DBT_PLATFORM,
                    self.config.env,
                    self.config.platform_instance,
                )
                upstreams_lineage_class = make_mapping_upstream_lineage(
                    upstream_urn=upstream_dbt_urn,
                    downstream_urn=node_datahub_urn,
                    node=node,
                    convert_column_urns_to_lowercase=self.config.convert_column_urns_to_lowercase,
                    skip_sources_in_lineage=self.config.skip_sources_in_lineage,
                )
                if self.config.incremental_lineage:
                    # We only generate incremental lineage for non-dbt nodes.
                    wu = convert_upstream_lineage_to_patch(
                        urn=node_datahub_urn,
                        aspect=upstreams_lineage_class,
                        system_metadata=None,
                    )
                    wu.is_primary_source = False
                    yield wu
                else:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=node_datahub_urn,
                        aspect=upstreams_lineage_class,
                    ).as_workunit(is_primary_source=False)

    def extract_query_tag_aspects(
        self,
        action_processor_tag: OperationProcessor,
        meta_aspects: Dict[str, Any],
        node: DBTNode,
    ) -> None:
        query_tag_aspects = action_processor_tag.process(node.query_tag)
        if "add_tag" in query_tag_aspects:
            if "add_tag" in meta_aspects:
                meta_aspects["add_tag"].tags.extend(query_tag_aspects["add_tag"].tags)
            else:
                meta_aspects["add_tag"] = query_tag_aspects["add_tag"]

    def get_aspect_from_dataset(
        self, dataset_snapshot: DatasetSnapshot, aspect_type: type
    ) -> Any:
        for aspect in dataset_snapshot.aspects:
            if isinstance(aspect, aspect_type):
                return aspect
        return None

    def get_patched_mce(self, mce):
        owner_aspect = self.get_aspect_from_dataset(
            mce.proposedSnapshot, OwnershipClass
        )
        if owner_aspect:
            transformed_owner_list = self.get_transformed_owners_by_source_type(
                owner_aspect.owners,
                mce.proposedSnapshot.urn,
                str(OwnershipSourceTypeClass.SOURCE_CONTROL),
            )
            owner_aspect.owners = transformed_owner_list

        tag_aspect = self.get_aspect_from_dataset(mce.proposedSnapshot, GlobalTagsClass)
        if tag_aspect:
            transformed_tag_list = self.get_transformed_tags_by_prefix(
                tag_aspect.tags,
                mce.proposedSnapshot.urn,
                tag_prefix=self.config.tag_prefix,
            )
            tag_aspect.tags = transformed_tag_list

        term_aspect: GlossaryTermsClass = self.get_aspect_from_dataset(
            mce.proposedSnapshot, GlossaryTermsClass
        )
        if term_aspect:
            transformed_terms = self.get_transformed_terms(
                term_aspect.terms, mce.proposedSnapshot.urn
            )
            term_aspect.terms = transformed_terms
        return mce

    def _create_dataset_properties_aspect(
        self, node: DBTNode, additional_custom_props_filtered: Dict[str, str]
    ) -> DatasetPropertiesClass:
        description = node.description

        custom_props = {
            **get_custom_properties(node),
            **additional_custom_props_filtered,
        }
        dbt_properties = DatasetPropertiesClass(
            description=description,
            customProperties=custom_props,
            tags=node.tags,
            name=node.name,
        )
        dbt_properties.externalUrl = self.get_external_url(node)

        return dbt_properties

    @abstractmethod
    def get_external_url(self, node: DBTNode) -> Optional[str]:
        pass

    def _create_view_properties_aspect(
        self, node: DBTNode
    ) -> Optional[ViewPropertiesClass]:
        if node.language != "sql" or not node.raw_code:
            return None

        compiled_code = None
        if self.config.include_compiled_code and node.compiled_code:
            compiled_code = try_format_query(
                node.compiled_code, platform=self.config.target_platform
            )

        materialized = node.materialization in {"table", "incremental", "snapshot"}
        view_properties = ViewPropertiesClass(
            materialized=materialized,
            viewLanguage="SQL",
            viewLogic=node.raw_code,
            formattedViewLogic=compiled_code,
        )
        return view_properties

    def _generate_base_dbt_aspects(
        self,
        node: DBTNode,
        additional_custom_props_filtered: Dict[str, str],
        mce_platform: str,
        meta_aspects: Dict[str, Any],
    ) -> List[Any]:
        """
        Some common aspects that get generated for dbt nodes.
        """

        # create an empty list of aspects and keep adding to it. Initializing with Any to avoid a
        # large union of aspect types.
        aspects: List[Any] = []

        # add dataset properties aspect
        dbt_properties = self._create_dataset_properties_aspect(
            node, additional_custom_props_filtered
        )
        aspects.append(dbt_properties)

        # add status aspect
        status = StatusClass(removed=False)
        aspects.append(status)
        # add owners aspect
        if self.config.enable_owner_extraction:
            # we need to aggregate owners added by meta properties and the owners that are coming from server.
            meta_owner_aspects = meta_aspects.get(Constants.ADD_OWNER_OPERATION)
            aggregated_owners = self._aggregate_owners(node, meta_owner_aspects)
            if aggregated_owners:
                aspects.append(OwnershipClass(owners=aggregated_owners))

        # add tags aspects
        meta_tags_aspect = meta_aspects.get(Constants.ADD_TAG_OPERATION)
        aggregated_tags = self._aggregate_tags(node, meta_tags_aspect)
        if aggregated_tags:
            aspects.append(
                mce_builder.make_global_tag_aspect_with_tag_list(aggregated_tags)
            )

        # add meta term aspects
        if (
            meta_aspects.get(Constants.ADD_TERM_OPERATION)
            and self.config.enable_meta_mapping
        ):
            aspects.append(meta_aspects.get(Constants.ADD_TERM_OPERATION))

        # add meta domains aspect
        if meta_aspects.get(Constants.ADD_DOMAIN_OPERATION):
            aspects.append(meta_aspects.get(Constants.ADD_DOMAIN_OPERATION))

        # add meta links aspect
        meta_links_aspect = meta_aspects.get(Constants.ADD_DOC_LINK_OPERATION)
        if meta_links_aspect and self.config.enable_meta_mapping:
            aspects.append(meta_links_aspect)

        # add schema metadata aspect
        schema_metadata = self.get_schema_metadata(self.report, node, mce_platform)
        aspects.append(schema_metadata)

        return aspects

    def get_schema_metadata(
        self, report: DBTSourceReport, node: DBTNode, platform: str
    ) -> SchemaMetadata:
        action_processor = OperationProcessor(
            self.config.column_meta_mapping,
            self.config.tag_prefix,
            "SOURCE_CONTROL",
            self.config.strip_user_ids_from_email,
        )

        canonical_schema: List[SchemaField] = []
        for column in node.columns:
            description = None

            if (
                column.comment
                and column.description
                and column.comment != column.description
            ):
                description = f"{platform} comment: {column.comment}\n\ndbt model description: {column.description}"
            elif column.comment:
                description = column.comment
            elif column.description:
                description = column.description

            meta_aspects: Dict[str, Any] = {}
            if self.config.enable_meta_mapping and column.meta:
                meta_aspects = action_processor.process(column.meta)

            if meta_aspects.get(Constants.ADD_OWNER_OPERATION):
                logger.warning("The add_owner operation is not supported for columns.")

            meta_tags: Optional[GlobalTagsClass] = meta_aspects.get(
                Constants.ADD_TAG_OPERATION
            )
            globalTags = None
            if meta_tags or column.tags:
                # Merge tags from meta mapping and column tags.
                globalTags = GlobalTagsClass(
                    tags=(meta_tags.tags if meta_tags else [])
                    + [
                        TagAssociationClass(mce_builder.make_tag_urn(tag))
                        for tag in column.tags
                    ]
                )

            glossaryTerms = None
            if meta_aspects.get(Constants.ADD_TERM_OPERATION):
                glossaryTerms = meta_aspects.get(Constants.ADD_TERM_OPERATION)

            field_name = column.name
            if self.config.convert_column_urns_to_lowercase:
                field_name = field_name.lower()

            field = SchemaField(
                fieldPath=field_name,
                nativeDataType=column.data_type,
                type=column.datahub_data_type
                or get_column_type(
                    report, node.dbt_name, column.data_type, node.dbt_adapter
                ),
                description=description,
                nullable=False,  # TODO: actually autodetect this
                recursive=False,
                globalTags=globalTags,
                glossaryTerms=glossaryTerms,
            )

            canonical_schema.append(field)

        last_modified = None
        if node.max_loaded_at is not None:
            actor = mce_builder.make_user_urn("dbt_executor")
            last_modified = AuditStamp(
                time=datetime_to_ts_millis(node.max_loaded_at),
                actor=actor,
            )

        return SchemaMetadata(
            schemaName=node.dbt_name,
            platform=mce_builder.make_data_platform_urn(platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            lastModified=last_modified,
            fields=canonical_schema,
        )

    def _aggregate_owners(
        self, node: DBTNode, meta_owner_aspects: Any
    ) -> List[OwnerClass]:
        owner_list: List[OwnerClass] = []
        if meta_owner_aspects and self.config.enable_meta_mapping:
            # we disregard owners generated from node.owner because that is also coming from the meta section
            owner_list = meta_owner_aspects.owners
        elif node.owner:
            owner: str = node.owner
            if self.compiled_owner_extraction_pattern:
                match: Optional[Any] = re.match(
                    self.compiled_owner_extraction_pattern, owner
                )
                if match:
                    owner = match.group("owner")
                    logger.debug(
                        f"Owner after applying owner extraction pattern:'{self.config.owner_extraction_pattern}' is '{owner}'."
                    )
            owners = owner if isinstance(owner, list) else [owner]

            for owner in owners:
                if self.config.strip_user_ids_from_email:
                    owner = owner.split("@")[0]
                    logger.debug(f"Owner (after stripping email):{owner}")

                owner_list.append(
                    OwnerClass(
                        owner=mce_builder.make_user_urn(owner),
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                )

        owner_list = sorted(owner_list, key=lambda x: x.owner)
        return owner_list

    def _aggregate_tags(self, node: DBTNode, meta_tag_aspect: Any) -> List[str]:
        tags_list: List[str] = []
        if node.tags:
            tags_list = tags_list + node.tags
        if meta_tag_aspect and self.config.enable_meta_mapping:
            tags_list = tags_list + [
                tag_association.tag[len("urn:li:tag:") :]
                for tag_association in meta_tag_aspect.tags
            ]
        return sorted(tags_list)

    def _create_subType_wu(
        self, node: DBTNode, node_datahub_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if not node.node_type:
            return None

        subtypes: List[str] = [node.node_type.capitalize()]

        return MetadataChangeProposalWrapper(
            entityUrn=node_datahub_urn,
            aspect=SubTypesClass(typeNames=subtypes),
        ).as_workunit()

    def _create_lineage_aspect_for_dbt_node(
        self,
        node: DBTNode,
        all_nodes_map: Dict[str, DBTNode],
    ) -> Optional[UpstreamLineageClass]:
        """
        This method creates lineage amongst dbt nodes. A dbt node can be linked to other dbt nodes or a platform node.
        """

        node_urn = node.get_urn(
            target_platform=DBT_PLATFORM,
            env=self.config.env,
            data_platform_instance=self.config.platform_instance,
        )

        # if a node is of type source in dbt, its upstream lineage should have the corresponding table/view
        # from the platform. This code block is executed when we are generating entities of type "dbt".
        if node.node_type == "source":
            return make_mapping_upstream_lineage(
                upstream_urn=node.get_urn(
                    self.config.target_platform,
                    self.config.env,
                    self.config.target_platform_instance,
                ),
                downstream_urn=node_urn,
                node=node,
                convert_column_urns_to_lowercase=self.config.convert_column_urns_to_lowercase,
                skip_sources_in_lineage=self.config.skip_sources_in_lineage,
            )
        else:
            upstream_urns = get_upstreams(
                node.upstream_nodes,
                all_nodes_map,
                self.config.target_platform,
                self.config.target_platform_instance,
                self.config.env,
                self.config.platform_instance,
                skip_sources_in_lineage=self.config.skip_sources_in_lineage,
            )

            def _translate_dbt_name_to_upstream_urn(dbt_name: str) -> str:
                return all_nodes_map[dbt_name].get_urn_for_upstream_lineage(
                    dbt_platform_instance=self.config.platform_instance,
                    target_platform=self.config.target_platform,
                    target_platform_instance=self.config.target_platform_instance,
                    env=self.config.env,
                    skip_sources_in_lineage=self.config.skip_sources_in_lineage,
                )

            if node.cll_debug_info and node.cll_debug_info.error:
                self.report.report_warning(
                    "Error parsing SQL to generate column lineage",
                    context=node.dbt_name,
                    exc=node.cll_debug_info.error,
                )

            cll = None
            if self.config.prefer_sql_parser_lineage and node.raw_sql_parsing_result:
                sql_parsing_result = node.raw_sql_parsing_result
                if sql_parsing_result and not sql_parsing_result.debug_info.table_error:
                    # If we have some table lineage from SQL parsing, use that.
                    upstream_urns = sql_parsing_result.in_tables

                    cll = []
                    for column_lineage in sql_parsing_result.column_lineage or []:
                        cll.append(
                            FineGrainedLineage(
                                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                                upstreams=[
                                    mce_builder.make_schema_field_urn(
                                        upstream.table, upstream.column
                                    )
                                    for upstream in column_lineage.upstreams
                                ],
                                downstreams=[
                                    mce_builder.make_schema_field_urn(
                                        node_urn, column_lineage.downstream.column
                                    )
                                ],
                                confidenceScore=sql_parsing_result.debug_info.confidence,
                            )
                        )

            else:
                if self.config.prefer_sql_parser_lineage:
                    if node.upstream_cll:
                        self.report.report_warning(
                            "SQL parser lineage is not available for this node, falling back to dbt-based column lineage.",
                            context=node.dbt_name,
                        )
                    else:
                        # SQL parsing failed entirely, which is already reported above.
                        pass

                cll = [
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        upstreams=[
                            mce_builder.make_schema_field_urn(
                                _translate_dbt_name_to_upstream_urn(
                                    upstream_column.upstream_dbt_name
                                ),
                                upstream_column.upstream_col,
                            )
                            for upstream_column in upstreams
                        ],
                        downstreams=[
                            mce_builder.make_schema_field_urn(node_urn, downstream)
                        ],
                        confidenceScore=(
                            node.cll_debug_info.confidence
                            if node.cll_debug_info
                            else None
                        ),
                    )
                    for downstream, upstreams in groupby_unsorted(
                        node.upstream_cll, lambda x: x.downstream_col
                    )
                ]

            if not upstream_urns:
                return None

            auditStamp = AuditStamp(
                time=mce_builder.get_sys_time(),
                actor=_DEFAULT_ACTOR,
            )
            sibling_urn = node.get_urn(
                self.config.target_platform,
                self.config.env,
                self.config.target_platform_instance,
            )
            return UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=upstream,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                        auditStamp=auditStamp,
                    )
                    for upstream in upstream_urns
                    if not (node.node_type == "model" and upstream == sibling_urn)
                ],
                fineGrainedLineages=(
                    (cll or None) if self.config.include_column_lineage else None
                ),
            )

    # This method attempts to read-modify and return the owners of a dataset.
    # From the existing owners it will remove the owners that are of the source_type_filter and
    # then add all the new owners to that list.
    def get_transformed_owners_by_source_type(
        self, owners: List[OwnerClass], entity_urn: str, source_type_filter: str
    ) -> List[OwnerClass]:
        transformed_owners: List[OwnerClass] = []
        if owners:
            transformed_owners += owners
        if self.ctx.graph:
            existing_ownership = self.ctx.graph.get_ownership(entity_urn)
            if not existing_ownership or not existing_ownership.owners:
                return transformed_owners

            for existing_owner in existing_ownership.owners:
                if (
                    existing_owner.source
                    and existing_owner.source.type != source_type_filter
                ):
                    transformed_owners.append(existing_owner)
        return sorted(transformed_owners, key=self.owner_sort_key)

    def owner_sort_key(self, owner_class: OwnerClass) -> str:
        return str(owner_class)

    # This method attempts to read-modify and return the tags of a dataset.
    # From the existing tags it will remove the tags that have a prefix tags_prefix_filter and
    # then add all the new tags to that list.
    def get_transformed_tags_by_prefix(
        self,
        new_tags: List[TagAssociationClass],
        entity_urn: str,
        tag_prefix: str,
    ) -> List[TagAssociationClass]:
        tag_set = {new_tag.tag for new_tag in new_tags}

        if self.ctx.graph:
            existing_tags_class = self.ctx.graph.get_tags(entity_urn)
            if existing_tags_class and existing_tags_class.tags:
                for existing_tag in existing_tags_class.tags:
                    if tag_prefix and existing_tag.tag.startswith(
                        mce_builder.make_tag_urn(tag_prefix)
                    ):
                        continue
                    tag_set.add(existing_tag.tag)
        return [TagAssociationClass(tag) for tag in sorted(tag_set)]

    # This method attempts to read-modify and return the glossary terms of a dataset.
    # This will combine all new and existing terms and return the final deduped list.
    def get_transformed_terms(
        self, new_terms: List[GlossaryTermAssociation], entity_urn: str
    ) -> List[GlossaryTermAssociation]:
        term_id_set = {term.urn for term in new_terms}
        if self.ctx.graph:
            existing_terms_class = self.ctx.graph.get_glossary_terms(entity_urn)
            if existing_terms_class and existing_terms_class.terms:
                for existing_term in existing_terms_class.terms:
                    term_id_set.add(existing_term.urn)
        return [GlossaryTermAssociation(term_urn) for term_urn in sorted(term_id_set)]

    def get_report(self):
        return self.report
