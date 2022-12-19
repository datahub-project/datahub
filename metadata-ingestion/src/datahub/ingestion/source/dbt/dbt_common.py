import json
import logging
import re
from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import auto
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import pydantic
from pydantic import root_validator, validator
from pydantic.fields import Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigEnum,
    ConfigModel,
    ConfigurationError,
    LineageConfig,
)
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
from datahub.ingestion.api.ingestion_job_state_provider import JobId
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import (
    BIGQUERY_TYPES_MAP,
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    SPARK_SQL_TYPES_MAP,
    TRINO_SQL_TYPES_MAP,
    resolve_postgres_modified_type,
    resolve_trino_modified_type,
)
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.dbt_state import DbtCheckpointState
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
    StateType,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    GlossaryTermAssociation,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    DateTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    RecordType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
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
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.mapping import Constants, OperationProcessor
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)
DBT_PLATFORM = "dbt"


@dataclass
class DBTSourceReport(StaleEntityRemovalSourceReport):
    pass


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
    test_definitions: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for test definitions when enabled when set to Yes or Only",
    )

    test_results: EmitDirective = Field(
        EmitDirective.YES,
        description="Emit metadata for test results when set to Yes or Only",
    )

    @root_validator
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

    def can_emit_node_type(self, node_type: str) -> bool:
        # Node type comes from dbt's node types.

        field_to_node_type_map = {
            "model": "models",
            "source": "sources",
            "seed": "seeds",
            "test": "test_definitions",
        }
        field = field_to_node_type_map.get(node_type)
        if not field:
            return False

        return self.__getattribute__(field) == EmitDirective.YES

    @property
    def can_emit_test_results(self) -> bool:
        return self.test_results == EmitDirective.YES


class DBTCommonConfig(StatefulIngestionConfigBase, LineageConfig):
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
    entities_enabled: DBTEntitiesEnabled = Field(
        DBTEntitiesEnabled(),
        description="Controls for enabling / disabling metadata emission for different dbt entities (models, test definitions, test results, etc.)",
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
    enable_meta_mapping = Field(
        default=True,
        description="When enabled, applies the mappings that are defined through the meta_mapping directives.",
    )
    query_tag_mapping: Dict = Field(
        default={},
        description="mapping rules that will be executed against dbt query_tag meta properties. Refer to the section below on dbt meta automated mappings.",
    )
    enable_query_tag_mapping = Field(
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
    backcompat_skip_source_on_lineage_edge: bool = Field(
        False,
        description="Prior to version 0.8.41, lineage edges to sources were directed to the target platform node rather than the dbt source node. This contradicted the established pattern for other lineage edges to point to upstream dbt nodes. To revert lineage logic to this legacy approach, set this flag to true.",
    )

    incremental_lineage: bool = Field(
        # Copied from LineageConfig, and changed the default.
        default=False,
        description="When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="DBT Stateful Ingestion Config."
    )

    @validator("target_platform")
    def validate_target_platform_value(cls, target_platform: str) -> str:
        if target_platform.lower() == DBT_PLATFORM:
            raise ValueError(
                "target_platform cannot be dbt. It should be the platform which dbt is operating on top of. For e.g "
                "postgres."
            )
        return target_platform

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
                    allowed_categories = [
                        value
                        for name, value in vars(OwnershipTypeClass).items()
                        if not name.startswith("_")
                    ]
                    if (owner_category.upper()) not in allowed_categories:
                        raise ValueError(
                            f"Owner category {owner_category} is not one of {allowed_categories}"
                        )
        return meta_mapping


@dataclass
class DBTColumn:
    name: str
    comment: str
    description: str
    index: int
    data_type: str

    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


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

    node_type: str  # source, model
    max_loaded_at: Optional[datetime]
    materialization: Optional[str]  # table, view, ephemeral, incremental
    # see https://docs.getdbt.com/reference/artifacts/manifest-json
    catalog_type: Optional[str]

    owner: Optional[str]

    columns: List[DBTColumn] = field(default_factory=list)
    upstream_nodes: List[str] = field(default_factory=list)

    meta: Dict[str, Any] = field(default_factory=dict)
    query_tag: Dict[str, Any] = field(default_factory=dict)

    tags: List[str] = field(default_factory=list)
    compiled_code: Optional[str] = None

    test_info: Optional["DBTTest"] = None  # only populated if node_type == 'test'
    test_result: Optional["DBTTestResult"] = None

    def get_db_fqn(self) -> str:
        if self.database:
            fqn = f"{self.database}.{self.schema}.{self.name}"
        else:
            fqn = f"{self.schema}.{self.name}"
        return fqn.replace('"', "")

    def get_urn(
        self,
        target_platform: str,
        env: str,
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


def get_custom_properties(node: DBTNode) -> Dict[str, str]:
    # initialize custom properties to node's meta props
    # (dbt-native node properties)
    custom_properties = node.meta

    # additional node attributes to extract to custom properties
    node_attributes = [
        "node_type",
        "materialization",
        "dbt_file_path",
        "catalog_type",
        "language",
    ]

    for attribute in node_attributes:
        node_attribute_value = getattr(node, attribute)

        if node_attribute_value is not None:
            custom_properties[attribute] = node_attribute_value

    custom_properties = {key: str(value) for key, value in custom_properties.items()}

    return custom_properties


def get_upstreams(
    upstreams: List[str],
    all_nodes: Dict[str, DBTNode],
    use_identifiers: bool,
    target_platform: str,
    target_platform_instance: Optional[str],
    environment: str,
    platform_instance: Optional[str],
    legacy_skip_source_lineage: Optional[bool],
) -> List[str]:
    upstream_urns = []

    for upstream in upstreams:
        if upstream not in all_nodes:
            logger.debug(
                f"Upstream node - {upstream} not found in all manifest entities."
            )
            continue

        upstream_manifest_node = all_nodes[upstream]

        # This logic creates lineages among dbt nodes.
        platform_value = DBT_PLATFORM
        platform_instance_value = platform_instance

        materialized = upstream_manifest_node.materialization

        resource_type = upstream_manifest_node.node_type
        if materialized in {"view", "table", "incremental"} or (
            resource_type == "source" and legacy_skip_source_lineage
        ):
            # upstream urns point to the target platform
            platform_value = target_platform
            platform_instance_value = target_platform_instance

        upstream_urns.append(
            upstream_manifest_node.get_urn(
                platform_value,
                environment,
                platform_instance_value,
            )
        )
    return upstream_urns


def get_upstream_lineage(upstream_urns: List[str]) -> UpstreamLineage:
    ucl: List[UpstreamClass] = []

    for dep in upstream_urns:
        uc = UpstreamClass(
            dataset=dep,
            type=DatasetLineageTypeClass.TRANSFORMED,
        )
        uc.auditStamp.time = int(datetime.utcnow().timestamp() * 1000)
        ucl.append(uc)

    return UpstreamLineage(upstreams=ucl)


# See https://github.com/fishtown-analytics/dbt/blob/master/core/dbt/adapters/sql/impl.py
_field_type_mapping = {
    "boolean": BooleanTypeClass,
    "date": DateTypeClass,
    "time": TimeTypeClass,
    "numeric": NumberTypeClass,
    "text": StringTypeClass,
    "timestamp with time zone": DateTypeClass,
    "timestamp without time zone": DateTypeClass,
    "integer": NumberTypeClass,
    "float8": NumberTypeClass,
    "struct": RecordType,
    **POSTGRES_TYPES_MAP,
    **SNOWFLAKE_TYPES_MAP,
    **BIGQUERY_TYPES_MAP,
    **SPARK_SQL_TYPES_MAP,
    **TRINO_SQL_TYPES_MAP,
}


def get_column_type(
    report: DBTSourceReport, dataset_name: str, column_type: str, dbt_adapter: str
) -> SchemaFieldDataType:
    """
    Maps known DBT types to datahub types
    """
    TypeClass: Any = _field_type_mapping.get(column_type)

    if TypeClass is None:
        # resolve modified type
        if dbt_adapter == "trino":
            TypeClass = resolve_trino_modified_type(column_type)
        elif dbt_adapter == "postgres" or dbt_adapter == "redshift":
            # Redshift uses a variant of Postgres, so we can use the same logic.
            TypeClass = resolve_postgres_modified_type(column_type)

    # if still not found, report the warning
    if TypeClass is None:
        report.report_warning(
            dataset_name, f"unable to map type {column_type} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


@dataclass
class AssertionParams:
    scope: Union[DatasetAssertionScopeClass, str]
    operator: Union[AssertionStdOperatorClass, str]
    aggregation: Union[AssertionStdAggregationClass, str]
    parameters: Optional[Callable[[Dict[str, str]], AssertionStdParametersClass]] = None
    logic_fn: Optional[Callable[[Dict[str, str]], Optional[str]]] = None


def _get_name_for_relationship_test(kw_args: Dict[str, str]) -> Optional[str]:
    """
    Try to produce a useful string for the name of a relationship constraint.
    Return None if we fail to
    """
    destination_ref = kw_args.get("to")
    source_ref = kw_args.get("model")
    column_name = kw_args.get("column_name")
    dest_field_name = kw_args.get("field")
    if not destination_ref or not source_ref or not column_name or not dest_field_name:
        # base assertions are violated, bail early
        return None
    m = re.match(r"^ref\(\'(.*)\'\)$", destination_ref)
    if m:
        destination_table = m.group(1)
    else:
        destination_table = destination_ref
    m = re.search(r"ref\(\'(.*)\'\)", source_ref)
    if m:
        source_table = m.group(1)
    else:
        source_table = source_ref
    return f"{source_table}.{column_name} referential integrity to {destination_table}.{dest_field_name}"


@dataclass
class DBTTest:
    qualified_test_name: str
    column_name: Optional[str]
    kw_args: dict

    TEST_NAME_TO_ASSERTION_MAP: ClassVar[Dict[str, AssertionParams]] = {
        "not_null": AssertionParams(
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass.NOT_NULL,
            aggregation=AssertionStdAggregationClass.IDENTITY,
        ),
        "unique": AssertionParams(
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass.EQUAL_TO,
            aggregation=AssertionStdAggregationClass.UNIQUE_PROPOTION,
            parameters=lambda _: AssertionStdParametersClass(
                value=AssertionStdParameterClass(
                    value="1.0",
                    type=AssertionStdParameterTypeClass.NUMBER,
                )
            ),
        ),
        "accepted_values": AssertionParams(
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass.IN,
            aggregation=AssertionStdAggregationClass.IDENTITY,
            parameters=lambda kw_args: AssertionStdParametersClass(
                value=AssertionStdParameterClass(
                    value=json.dumps(kw_args.get("values")),
                    type=AssertionStdParameterTypeClass.SET,
                ),
            ),
        ),
        "relationships": AssertionParams(
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass._NATIVE_,
            aggregation=AssertionStdAggregationClass.IDENTITY,
            parameters=lambda kw_args: AssertionStdParametersClass(
                value=AssertionStdParameterClass(
                    value=json.dumps(kw_args.get("values")),
                    type=AssertionStdParameterTypeClass.SET,
                ),
            ),
            logic_fn=_get_name_for_relationship_test,
        ),
        "dbt_expectations.expect_column_values_to_not_be_null": AssertionParams(
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass.NOT_NULL,
            aggregation=AssertionStdAggregationClass.IDENTITY,
        ),
        "dbt_expectations.expect_column_values_to_be_between": AssertionParams(
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass.BETWEEN,
            aggregation=AssertionStdAggregationClass.IDENTITY,
            parameters=lambda x: AssertionStdParametersClass(
                minValue=AssertionStdParameterClass(
                    value=str(x.get("min_value", "unknown")),
                    type=AssertionStdParameterTypeClass.NUMBER,
                ),
                maxValue=AssertionStdParameterClass(
                    value=str(x.get("max_value", "unknown")),
                    type=AssertionStdParameterTypeClass.NUMBER,
                ),
            ),
        ),
        "dbt_expectations.expect_column_values_to_be_in_set": AssertionParams(
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass.IN,
            aggregation=AssertionStdAggregationClass.IDENTITY,
            parameters=lambda kw_args: AssertionStdParametersClass(
                value=AssertionStdParameterClass(
                    value=json.dumps(kw_args.get("value_set")),
                    type=AssertionStdParameterTypeClass.SET,
                ),
            ),
        ),
    }


@dataclass
class DBTTestResult:
    invocation_id: str

    status: str
    execution_time: datetime

    native_results: Dict[str, str]


def string_map(input_map: Dict[str, Any]) -> Dict[str, str]:
    return {k: str(v) for k, v in input_map.items()}


@platform_name("dbt")
@config_class(DBTCommonConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "", supported=False)
class DBTSourceBase(StatefulIngestionSourceBase):
    def __init__(self, config: DBTCommonConfig, ctx: PipelineContext, platform: str):
        super().__init__(config, ctx)
        self.config = config
        self.platform: str = platform
        self.report: DBTSourceReport = DBTSourceReport()
        self.compiled_owner_extraction_pattern: Optional[Any] = None
        if self.config.owner_extraction_pattern:
            self.compiled_owner_extraction_pattern = re.compile(
                self.config.owner_extraction_pattern
            )
        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=DbtCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

    def get_last_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[StateType]
    ) -> Optional[Checkpoint]:
        last_checkpoint: Optional[Checkpoint]
        is_conversion_required: bool = False
        try:
            # Best-case that last checkpoint state is DbtCheckpointState
            last_checkpoint = super(DBTSourceBase, self).get_last_checkpoint(
                job_id, checkpoint_state_class
            )
        except Exception as e:
            # Backward compatibility for old dbt ingestion source which was saving dbt-nodes in
            # BaseSQLAlchemyCheckpointState
            last_checkpoint = super(DBTSourceBase, self).get_last_checkpoint(
                job_id, BaseSQLAlchemyCheckpointState  # type: ignore
            )
            logger.debug(
                f"Found BaseSQLAlchemyCheckpointState as checkpoint state (got {e})."
            )
            is_conversion_required = True

        if last_checkpoint is not None and is_conversion_required:
            # Map the BaseSQLAlchemyCheckpointState to DbtCheckpointState
            dbt_checkpoint_state: DbtCheckpointState = DbtCheckpointState()
            dbt_checkpoint_state.urns = (
                cast(BaseSQLAlchemyCheckpointState, last_checkpoint.state)
            ).urns
            last_checkpoint.state = dbt_checkpoint_state

        return last_checkpoint

    def create_test_entity_mcps(
        self,
        test_nodes: List[DBTNode],
        custom_props: Dict[str, str],
        all_nodes_map: Dict[str, DBTNode],
    ) -> Iterable[MetadataWorkUnit]:
        for node in test_nodes:
            assertion_urn = mce_builder.make_assertion_urn(
                mce_builder.datahub_guid(
                    {
                        "platform": DBT_PLATFORM,
                        "name": node.dbt_name,
                        "instance": self.config.platform_instance,
                    }
                )
            )
            self.stale_entity_removal_handler.add_entity_to_state(
                type="assertion",
                urn=assertion_urn,
            )

            if self.config.entities_enabled.can_emit_node_type("test"):
                wu = MetadataChangeProposalWrapper(
                    entityUrn=assertion_urn,
                    aspect=DataPlatformInstanceClass(
                        platform=mce_builder.make_data_platform_urn(DBT_PLATFORM)
                    ),
                ).as_workunit()
                self.report.report_workunit(wu)
                yield wu

            upstream_urns = get_upstreams(
                upstreams=node.upstream_nodes,
                all_nodes=all_nodes_map,
                use_identifiers=self.config.use_identifiers,
                target_platform=self.config.target_platform,
                target_platform_instance=self.config.target_platform_instance,
                environment=self.config.env,
                platform_instance=None,
                legacy_skip_source_lineage=self.config.backcompat_skip_source_on_lineage_edge,
            )

            for upstream_urn in upstream_urns:
                if self.config.entities_enabled.can_emit_node_type("test"):
                    wu = self._make_assertion_from_test(
                        custom_props,
                        node,
                        assertion_urn,
                        upstream_urn,
                    )
                    self.report.report_workunit(wu)
                    yield wu

                if node.test_result:
                    if self.config.entities_enabled.can_emit_test_results:
                        wu = self._make_assertion_result_from_test(
                            node, assertion_urn, upstream_urn
                        )
                        self.report.report_workunit(wu)
                        yield wu
                    else:
                        logger.debug(
                            f"Skipping test result {node.name} emission since it is turned off."
                        )

    def _make_assertion_from_test(
        self,
        extra_custom_props: Dict[str, str],
        node: DBTNode,
        assertion_urn: str,
        upstream_urn: str,
    ) -> MetadataWorkUnit:
        assert node.test_info
        qualified_test_name = node.test_info.qualified_test_name
        column_name = node.test_info.column_name
        kw_args = node.test_info.kw_args

        if qualified_test_name in DBTTest.TEST_NAME_TO_ASSERTION_MAP:
            assertion_params = DBTTest.TEST_NAME_TO_ASSERTION_MAP[qualified_test_name]
            assertion_info = AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                customProperties=extra_custom_props,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=upstream_urn,
                    scope=assertion_params.scope,
                    operator=assertion_params.operator,
                    fields=[
                        mce_builder.make_schema_field_urn(upstream_urn, column_name)
                    ]
                    if (
                        assertion_params.scope
                        == DatasetAssertionScopeClass.DATASET_COLUMN
                        and column_name
                    )
                    else [],
                    nativeType=node.name,
                    aggregation=assertion_params.aggregation,
                    parameters=assertion_params.parameters(kw_args)
                    if assertion_params.parameters
                    else None,
                    logic=assertion_params.logic_fn(kw_args)
                    if assertion_params.logic_fn
                    else None,
                    nativeParameters=string_map(kw_args),
                ),
            )
        elif column_name:
            # no match with known test types, column-level test
            assertion_info = AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                customProperties=extra_custom_props,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=upstream_urn,
                    scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                    operator=AssertionStdOperatorClass._NATIVE_,
                    fields=[
                        mce_builder.make_schema_field_urn(upstream_urn, column_name)
                    ],
                    nativeType=node.name,
                    logic=node.compiled_code if node.compiled_code else node.raw_code,
                    aggregation=AssertionStdAggregationClass._NATIVE_,
                    nativeParameters=string_map(kw_args),
                ),
            )
        else:
            # no match with known test types, default to row-level test
            assertion_info = AssertionInfoClass(
                type=AssertionTypeClass.DATASET,
                customProperties=extra_custom_props,
                datasetAssertion=DatasetAssertionInfoClass(
                    dataset=upstream_urn,
                    scope=DatasetAssertionScopeClass.DATASET_ROWS,
                    operator=AssertionStdOperatorClass._NATIVE_,
                    logic=node.compiled_code if node.compiled_code else node.raw_code,
                    nativeType=node.name,
                    aggregation=AssertionStdAggregationClass._NATIVE_,
                    nativeParameters=string_map(kw_args),
                ),
            )

        wu = MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        ).as_workunit()

        return wu

    def _make_assertion_result_from_test(
        self,
        node: DBTNode,
        assertion_urn: str,
        upstream_urn: str,
    ) -> MetadataWorkUnit:
        assert node.test_result
        test_result = node.test_result

        assertionResult = AssertionRunEventClass(
            timestampMillis=int(test_result.execution_time.timestamp() * 1000.0),
            assertionUrn=assertion_urn,
            asserteeUrn=upstream_urn,
            runId=test_result.invocation_id,
            result=AssertionResultClass(
                type=AssertionResultTypeClass.SUCCESS
                if test_result.status == "pass"
                else AssertionResultTypeClass.FAILURE,
                nativeResults=test_result.native_results,
            ),
            status=AssertionRunStatusClass.COMPLETE,
        )

        event = MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertionResult,
        )
        wu = MetadataWorkUnit(
            id=f"{assertion_urn}-assertionRunEvent-{upstream_urn}",
            mcp=event,
        )
        return wu

    @abstractmethod
    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        # return dbt nodes + global custom properties
        raise NotImplementedError()

    # create workunits from dbt nodes
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.config.write_semantics == "PATCH" and not self.ctx.graph:
            raise ConfigurationError(
                "With PATCH semantics, dbt source requires a datahub_api to connect to. "
                "Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe."
            )

        all_nodes, additional_custom_props = self.load_nodes()

        all_nodes_map = {node.dbt_name: node for node in all_nodes}
        nodes = self.filter_nodes(all_nodes)

        additional_custom_props_filtered = {
            key: value
            for key, value in additional_custom_props.items()
            if value is not None
        }

        non_test_nodes = [
            dataset_node for dataset_node in nodes if dataset_node.node_type != "test"
        ]
        test_nodes = [test_node for test_node in nodes if test_node.node_type == "test"]

        yield from self.create_platform_mces(
            non_test_nodes,
            additional_custom_props_filtered,
            all_nodes_map,
            DBT_PLATFORM,
            self.config.platform_instance,
        )

        yield from self.create_platform_mces(
            non_test_nodes,
            additional_custom_props_filtered,
            all_nodes_map,
            self.config.target_platform,
            self.config.target_platform_instance,
        )

        yield from self.create_test_entity_mcps(
            test_nodes,
            additional_custom_props_filtered,
            all_nodes_map,
        )

        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def filter_nodes(self, all_nodes: List[DBTNode]) -> List[DBTNode]:
        nodes = []
        for node in all_nodes:
            key = node.dbt_name

            if not self.config.node_name_pattern.allowed(key):
                continue

            nodes.append(node)

        return nodes

    def create_platform_mces(
        self,
        dbt_nodes: List[DBTNode],
        additional_custom_props_filtered: Dict[str, str],
        all_nodes_map: Dict[str, DBTNode],
        mce_platform: str,
        mce_platform_instance: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        This function creates mce based out of dbt nodes. Since dbt ingestion creates "dbt" nodes
        and nodes for underlying platform the function gets called twice based on the mce_platform
        parameter. Further, this function takes specific actions based on the mce_platform passed in.
        It creates platform entities with all metadata information.
        """
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
        for node in dbt_nodes:
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
            self.stale_entity_removal_handler.add_entity_to_state(
                "dataset", node_datahub_urn
            )

            meta_aspects: Dict[str, Any] = {}
            if self.config.enable_meta_mapping and node.meta:
                meta_aspects = action_processor.process(node.meta)

            if self.config.enable_query_tag_mapping and node.query_tag:
                self.extract_query_tag_aspects(action_processor_tag, meta_aspects, node)

            if mce_platform == DBT_PLATFORM:
                aspects = self._generate_base_aspects(
                    node, additional_custom_props_filtered, mce_platform, meta_aspects
                )

                # add upstream lineage
                upstream_lineage_class = self._create_lineage_aspect_for_dbt_node(
                    node, all_nodes_map
                )
                if upstream_lineage_class:
                    aspects.append(upstream_lineage_class)

                # add view properties aspect
                if node.raw_code and node.language == "sql":
                    view_prop_aspect = self._create_view_properties_aspect(node)
                    aspects.append(view_prop_aspect)

                # emit subtype mcp
                sub_type_wu = self._create_subType_wu(node, node_datahub_urn)
                if sub_type_wu:
                    yield sub_type_wu
                    self.report.report_workunit(sub_type_wu)

            else:
                # We are creating empty node for platform and only add lineage/keyaspect.
                aspects = []
                if node.materialization == "ephemeral" or node.node_type == "test":
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
                    upstreams_lineage_class = get_upstream_lineage([upstream_dbt_urn])
                    if self.config.incremental_lineage:
                        patch_builder: DatasetPatchBuilder = DatasetPatchBuilder(
                            urn=node_datahub_urn
                        )
                        for upstream in upstreams_lineage_class.upstreams:
                            patch_builder.add_upstream_lineage(upstream)

                        lineage_workunits = [
                            MetadataWorkUnit(
                                id=f"upstreamLineage-for-{node_datahub_urn}",
                                mcp_raw=mcp,
                            )
                            for mcp in patch_builder.build()
                        ]
                        for wu in lineage_workunits:
                            yield wu
                            self.report.report_workunit(wu)
                    else:
                        aspects.append(upstreams_lineage_class)

            if len(aspects) == 0:
                continue
            dataset_snapshot = DatasetSnapshot(urn=node_datahub_urn, aspects=aspects)
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            if self.config.write_semantics == "PATCH":
                mce = self.get_patched_mce(mce)
            wu = MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)
            yield wu

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
                mce_builder.make_tag_urn(self.config.tag_prefix),
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

    def _create_view_properties_aspect(self, node: DBTNode) -> ViewPropertiesClass:
        materialized = node.materialization in {"table", "incremental"}
        # this function is only called when raw sql is present. assert is added to satisfy lint checks
        assert node.raw_code is not None
        view_properties = ViewPropertiesClass(
            materialized=materialized,
            viewLanguage="SQL",
            viewLogic=node.raw_code,
        )
        return view_properties

    def _generate_base_aspects(
        self,
        node: DBTNode,
        additional_custom_props_filtered: Dict[str, str],
        mce_platform: str,
        meta_aspects: Dict[str, Any],
    ) -> List[Any]:
        """
        There are some common aspects that get generated for both dbt node and platform node depending on whether dbt
        node creation is enabled or not.
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

            field = SchemaField(
                fieldPath=column.name,
                nativeDataType=column.data_type,
                type=get_column_type(
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
        subtypes: Optional[List[str]]
        if node.node_type == "model":
            if node.materialization:
                subtypes = [node.materialization, "view"]
            else:
                subtypes = ["model", "view"]
        else:
            subtypes = [node.node_type]
        subtype_mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=node_datahub_urn,
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=subtypes),
        )
        subtype_wu = MetadataWorkUnit(
            id=f"{subtype_mcp.entityUrn}-{subtype_mcp.aspectName}",
            mcp=subtype_mcp,
        )
        return subtype_wu

    def _create_lineage_aspect_for_dbt_node(
        self,
        node: DBTNode,
        all_nodes_map: Dict[str, DBTNode],
    ) -> Optional[UpstreamLineageClass]:
        """
        This method creates lineage amongst dbt nodes. A dbt node can be linked to other dbt nodes or a platform node.
        """
        upstream_urns = get_upstreams(
            node.upstream_nodes,
            all_nodes_map,
            self.config.use_identifiers,
            self.config.target_platform,
            self.config.target_platform_instance,
            self.config.env,
            self.config.platform_instance,
            self.config.backcompat_skip_source_on_lineage_edge,
        )

        # if a node is of type source in dbt, its upstream lineage should have the corresponding table/view
        # from the platform. This code block is executed when we are generating entities of type "dbt".
        if node.node_type == "source":
            upstream_urns.append(
                node.get_urn(
                    self.config.target_platform,
                    self.config.env,
                    self.config.target_platform_instance,
                )
            )
        if upstream_urns:
            upstreams_lineage_class = get_upstream_lineage(upstream_urns)
            return upstreams_lineage_class
        return None

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
        tags_prefix_filter: str,
    ) -> List[TagAssociationClass]:
        tag_set = set([new_tag.tag for new_tag in new_tags])

        if self.ctx.graph:
            existing_tags_class = self.ctx.graph.get_tags(entity_urn)
            if existing_tags_class and existing_tags_class.tags:
                for exiting_tag in existing_tags_class.tags:
                    if not exiting_tag.tag.startswith(tags_prefix_filter):
                        tag_set.add(exiting_tag.tag)
        return [TagAssociationClass(tag) for tag in sorted(tag_set)]

    # This method attempts to read-modify and return the glossary terms of a dataset.
    # This will combine all new and existing terms and return the final deduped list.
    def get_transformed_terms(
        self, new_terms: List[GlossaryTermAssociation], entity_urn: str
    ) -> List[GlossaryTermAssociation]:
        term_id_set = set([term.urn for term in new_terms])
        if self.ctx.graph:
            existing_terms_class = self.ctx.graph.get_glossary_terms(entity_urn)
            if existing_terms_class and existing_terms_class.terms:
                for existing_term in existing_terms_class.terms:
                    term_id_set.add(existing_term.urn)
        return [GlossaryTermAssociation(term_urn) for term_urn in sorted(term_id_set)]

    def get_report(self):
        return self.report
