import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)
from urllib.parse import urlparse

import dateutil.parser
import requests
from cached_property import cached_property
from pydantic import BaseModel, root_validator, validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.github import GitHubInfo
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
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
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
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
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
from datahub.utilities.mapping import Constants, OperationProcessor

logger = logging.getLogger(__name__)
DBT_PLATFORM = "dbt"


class DBTStatefulIngestionConfig(StatefulIngestionConfig):
    """
    Specialization of basic StatefulIngestionConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the SQLAlchemyConfig.
    """

    remove_stale_metadata: bool = True


@dataclass
class DBTSourceReport(StatefulIngestionReport):
    soft_deleted_stale_entities: List[str] = field(default_factory=list)

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)


class EmitDirective(Enum):
    """A holder for directives for emission for specific types of entities"""

    YES = "YES"  # Okay to emit for this type
    NO = "NO"  # Do not emit for this type
    ONLY = "ONLY"  # Only emit metadata for this type and no others


class DBTEntitiesEnabled(BaseModel):
    """Controls which dbt entities are going to be emitted by this source"""

    class Config:
        arbitrary_types_allowed = True  # needed to allow cached_property to work
        keep_untouched = (
            cached_property,
        )  # needed to allow cached_property to work. See https://github.com/samuelcolvin/pydantic/issues/1241 for more info.

    models: EmitDirective = Field(
        "Yes", description="Emit metadata for dbt models when set to Yes or Only"
    )
    sources: EmitDirective = Field(
        "Yes", description="Emit metadata for dbt sources when set to Yes or Only"
    )
    seeds: EmitDirective = Field(
        "Yes", description="Emit metadata for dbt seeds when set to Yes or Only"
    )
    test_definitions: EmitDirective = Field(
        "Yes",
        description="Emit metadata for test definitions when enabled when set to Yes or Only",
    )
    test_results: EmitDirective = Field(
        "Yes", description="Emit metadata for test results when set to Yes or Only"
    )

    @validator("*", pre=True, always=True)
    def to_upper(cls, v):
        return v.upper() if isinstance(v, str) else v

    @root_validator
    def only_one_can_be_set_to_only(cls, values):
        only_values = [k for k in values if values.get(k) == EmitDirective.ONLY]
        if len(only_values) > 1:
            raise ValueError(
                f"Cannot have more than 1 type of entity emission set to ONLY. Found {only_values}"
            )
        return values

    def _any_other_only_set(self, attribute: str) -> bool:
        """Return true if any attribute other than the one passed in is set to ONLY"""
        other_onlies = [
            k
            for k, v in self.__dict__.items()
            if k != attribute and v == EmitDirective.ONLY
        ]
        return len(other_onlies) != 0

    @cached_property  # type: ignore
    def node_type_emit_decision_cache(self) -> Dict[str, bool]:
        node_type_for_field_map = {
            "models": "model",
            "sources": "source",
            "seeds": "seed",
            "test_definitions": "test",
        }
        return {
            node_type_for_field_map[k]: False
            if self._any_other_only_set(k)
            or self.__getattribute__(k) == EmitDirective.NO
            else True
            for k in ["models", "sources", "seeds", "test_definitions"]
        }

    def can_emit_node_type(self, node_type: str) -> bool:
        return self.node_type_emit_decision_cache.get(node_type, False)

    @property
    def can_emit_test_results(self) -> bool:
        return (
            not self._any_other_only_set("test_results")
            and self.test_results != EmitDirective.NO
        )


class DBTConfig(StatefulIngestionConfigBase):
    manifest_path: str = Field(
        description="Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json Note this can be a local file or a URI."
    )
    catalog_path: str = Field(
        description="Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json Note this can be a local file or a URI."
    )
    sources_path: Optional[str] = Field(
        default=None,
        description="Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. If not specified, last-modified fields will not be populated. Note this can be a local file or a URI.",
    )
    test_results_path: Optional[str] = Field(
        default=None,
        description="Path to output of dbt test run as run_results file in JSON format. See https://docs.getdbt.com/reference/artifacts/run-results-json. If not specified, test execution results will not be populated in DataHub.",
    )
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
    node_type_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Deprecated: use entities_enabled instead. Regex patterns for dbt nodes to filter in ingestion.",
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
    aws_connection: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="When fetching manifest files from s3, configuration for aws connection details",
    )
    backcompat_skip_source_on_lineage_edge: bool = Field(
        False,
        description="Prior to version 0.8.41, lineage edges to sources were directed to the target platform node rather than the dbt source node. This contradicted the established pattern for other lineage edges to point to upstream dbt nodes. To revert lineage logic to this legacy approach, set this flag to true.",
    )
    github_info: Optional[GitHubInfo] = Field(
        None,
        description="Reference to your github location to enable easy navigation from DataHub to your dbt files.",
    )

    @property
    def s3_client(self):
        assert self.aws_connection
        return self.aws_connection.get_s3_client()

    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[DBTStatefulIngestionConfig] = Field(
        default=None, description=""
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

    @validator("aws_connection")
    def aws_connection_needed_if_s3_uris_present(
        cls, aws_connection: Optional[AwsConnectionConfig], values: Dict, **kwargs: Any
    ) -> Optional[AwsConnectionConfig]:
        # first check if there are fields that contain s3 uris
        uri_containing_fields = [
            f
            for f in ["manifest_path", "catalog_path", "sources_path"]
            if (values.get(f) or "").startswith("s3://")
        ]

        if uri_containing_fields and not aws_connection:
            raise ValueError(
                f"Please provide aws_connection configuration, since s3 uris have been provided in fields {uri_containing_fields}"
            )
        return aws_connection

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
    tags: List[str] = field(default_factory=list)

    def __repr__(self):
        fields = tuple("{}={}".format(k, v) for k, v in self.__dict__.items())
        return self.__class__.__name__ + str(tuple(sorted(fields))).replace("'", "")


@dataclass
class DBTNode:
    database: Optional[str]
    schema: str
    name: str  # name, identifier
    alias: Optional[str]  # alias if present
    comment: str
    description: str
    raw_sql: Optional[str]

    dbt_adapter: str
    dbt_name: str
    dbt_file_path: str

    node_type: str  # source, model
    max_loaded_at: Optional[str]
    materialization: Optional[str]  # table, view, ephemeral, incremental
    # see https://docs.getdbt.com/reference/artifacts/manifest-json
    catalog_type: Optional[str]

    owner: Optional[str]

    columns: List[DBTColumn] = field(default_factory=list)
    upstream_nodes: List[str] = field(default_factory=list)

    meta: Dict[str, Any] = field(default_factory=dict)
    query_tag: Dict[str, Any] = field(default_factory=dict)

    tags: List[str] = field(default_factory=list)
    compiled_sql: Optional[str] = None

    def __repr__(self):
        fields = tuple("{}={}".format(k, v) for k, v in self.__dict__.items())
        return self.__class__.__name__ + str(tuple(sorted(fields))).replace("'", "")


def get_columns(
    catalog_node: dict, manifest_node: dict, tag_prefix: str
) -> List[DBTColumn]:
    columns = []

    manifest_columns = manifest_node.get("columns", {})

    raw_columns = catalog_node["columns"]

    for key in raw_columns:
        raw_column = raw_columns[key]

        tags = manifest_columns.get(key.lower(), {}).get("tags", [])
        tags = [tag_prefix + tag for tag in tags]

        dbtCol = DBTColumn(
            name=raw_column["name"].lower(),
            comment=raw_column.get("comment", ""),
            description=manifest_columns.get(key.lower(), {}).get("description", ""),
            data_type=raw_column["type"],
            index=raw_column["index"],
            tags=tags,
        )
        columns.append(dbtCol)
    return columns


def extract_dbt_entities(
    all_manifest_entities: Dict[str, Dict[str, Any]],
    all_catalog_entities: Dict[str, Dict[str, Any]],
    sources_results: List[Dict[str, Any]],
    manifest_adapter: str,
    use_identifiers: bool,
    tag_prefix: str,
    node_type_pattern: AllowDenyPattern,
    report: DBTSourceReport,
    node_name_pattern: AllowDenyPattern,
    entities_enabled: DBTEntitiesEnabled,
) -> List[DBTNode]:
    sources_by_id = {x["unique_id"]: x for x in sources_results}

    dbt_entities = []
    for key, manifest_node in all_manifest_entities.items():
        # check if node pattern allowed based on config file
        if not node_type_pattern.allowed(manifest_node["resource_type"]):
            logger.debug(
                f"Not extracting dbt entity {key} since node type {manifest_node['resource_type']} is disabled"
            )
            continue

        name = manifest_node["name"]

        if "identifier" in manifest_node and use_identifiers:
            name = manifest_node["identifier"]

        if (
            manifest_node.get("alias") is not None
            and manifest_node.get("resource_type")
            != "test"  # tests have non-human-friendly aliases, so we don't want to use it for tests
        ):
            name = manifest_node["alias"]

        if not node_name_pattern.allowed(key):
            continue

        # initialize comment to "" for consistency with descriptions
        # (since dbt null/undefined descriptions as "")
        comment = ""

        if key in all_catalog_entities and all_catalog_entities[key]["metadata"].get(
            "comment"
        ):
            comment = all_catalog_entities[key]["metadata"]["comment"]

        materialization = None
        upstream_nodes = []

        if "materialized" in manifest_node.get("config", {}):
            # It's a model
            materialization = manifest_node["config"]["materialized"]
            upstream_nodes = manifest_node["depends_on"]["nodes"]

        # It's a source
        catalog_node = all_catalog_entities.get(key)
        catalog_type = None

        if catalog_node is None:
            if materialization != "test":
                report.report_warning(
                    key,
                    f"Entity {key} ({name}) is in manifest but missing from catalog",
                )
        else:
            catalog_type = all_catalog_entities[key]["metadata"]["type"]

        query_tag_props = manifest_node.get("query_tag", {})

        meta = manifest_node.get("meta", {})

        owner = meta.get("owner")
        if owner is None:
            owner = manifest_node.get("config", {}).get("meta", {}).get("owner")

        tags = manifest_node.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]
        meta_props = manifest_node.get("meta", {})
        if not meta:
            meta_props = manifest_node.get("config", {}).get("meta", {})
        dbtNode = DBTNode(
            dbt_name=key,
            dbt_adapter=manifest_adapter,
            database=manifest_node["database"],
            schema=manifest_node["schema"],
            name=name,
            alias=manifest_node.get("alias"),
            dbt_file_path=manifest_node["original_file_path"],
            node_type=manifest_node["resource_type"],
            max_loaded_at=sources_by_id.get(key, {}).get("max_loaded_at"),
            comment=comment,
            description=manifest_node.get("description", ""),
            raw_sql=manifest_node.get("raw_sql"),
            upstream_nodes=upstream_nodes,
            materialization=materialization,
            catalog_type=catalog_type,
            columns=[],
            meta=meta_props,
            query_tag=query_tag_props,
            tags=tags,
            owner=owner,
            compiled_sql=manifest_node.get("compiled_sql"),
        )

        # overwrite columns from catalog
        if dbtNode.materialization not in [
            "ephemeral",
            "test",
        ]:  # we don't want columns if platform isn't 'dbt'
            logger.debug("Loading schema info")
            catalog_node = all_catalog_entities.get(key)

            if catalog_node is None:
                report.report_warning(
                    key,
                    f"Entity {dbtNode.dbt_name} is in manifest but missing from catalog",
                )
            else:
                dbtNode.columns = get_columns(catalog_node, manifest_node, tag_prefix)

        else:
            dbtNode.columns = []

        dbt_entities.append(dbtNode)

    return dbt_entities


def get_db_fqn(database: Optional[str], schema: str, name: str) -> str:
    if database is not None:
        fqn = f"{database}.{schema}.{name}"
    else:
        fqn = f"{schema}.{name}"
    return fqn.replace('"', "")


def get_urn_from_dbtNode(
    database: Optional[str],
    schema: str,
    name: str,
    target_platform: str,
    env: str,
    data_platform_instance: Optional[str],
) -> str:
    db_fqn = get_db_fqn(database, schema, name)
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
    node_attributes = ["node_type", "materialization", "dbt_file_path", "catalog_type"]

    for attribute in node_attributes:
        node_attribute_value = getattr(node, attribute)

        if node_attribute_value is not None:
            custom_properties[attribute] = node_attribute_value

    custom_properties = {key: str(value) for key, value in custom_properties.items()}

    return custom_properties


def get_upstreams(
    upstreams: List[str],
    all_nodes: Dict[str, Dict[str, Any]],
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
        if "identifier" in all_nodes[upstream] and use_identifiers:
            name = all_nodes[upstream]["identifier"]
        else:
            name = all_nodes[upstream]["name"]

        if "alias" in all_nodes[upstream]:
            name = all_nodes[upstream]["alias"]

        upstream_manifest_node = all_nodes[upstream]

        # This logic creates lineages among dbt nodes.
        platform_value = DBT_PLATFORM
        platform_instance_value = platform_instance

        materialized = upstream_manifest_node.get("config", {}).get("materialized")
        resource_type = upstream_manifest_node["resource_type"]
        if materialized in {"view", "table", "incremental"} or (
            resource_type == "source" and legacy_skip_source_lineage
        ):
            # upstream urns point to the target platform
            platform_value = target_platform
            platform_instance_value = target_platform_instance

        upstream_urns.append(
            get_urn_from_dbtNode(
                all_nodes[upstream]["database"],
                all_nodes[upstream]["schema"],
                name,
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
        elif dbt_adapter == "postgres":
            TypeClass = resolve_postgres_modified_type(column_type)

    # if still not found, report the warning
    if TypeClass is None:
        report.report_warning(
            dataset_name, f"unable to map type {column_type} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(
    report: DBTSourceReport, node: DBTNode, platform: str
) -> SchemaMetadata:
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

        globalTags = None
        if column.tags:
            globalTags = GlobalTagsClass(
                tags=[
                    TagAssociationClass(mce_builder.make_tag_urn(tag))
                    for tag in column.tags
                ]
            )

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
        )

        canonical_schema.append(field)

    last_modified = None
    if node.max_loaded_at is not None:
        actor = mce_builder.make_user_urn("dbt_executor")
        last_modified = AuditStamp(
            time=int(dateutil.parser.parse(node.max_loaded_at).timestamp() * 1000),
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


class DBTTestStep(BaseModel):
    name: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class DBTTestResult(BaseModel):
    class Config:
        extra = "allow"

    status: str
    timing: List[DBTTestStep] = []
    unique_id: str
    failures: Optional[int] = None
    message: Optional[str] = None


class DBTRunMetadata(BaseModel):
    dbt_schema_version: str
    dbt_version: str
    generated_at: str
    invocation_id: str


class DBTTest:

    test_name_to_assertion_map = {
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

    @staticmethod
    def load_test_results(
        config: DBTConfig,
        test_results_json: Dict[str, Any],
        test_nodes: List[DBTNode],
        manifest_nodes: Dict[str, Any],
    ) -> Iterable[MetadataWorkUnit]:
        if not config.entities_enabled.can_emit_test_results:
            logger.debug("Skipping test result emission since it is turned off.")
            return []

        args = test_results_json.get("args", {})
        dbt_metadata = DBTRunMetadata.parse_obj(test_results_json.get("metadata", {}))
        test_nodes_map: Dict[str, DBTNode] = {x.dbt_name: x for x in test_nodes}
        if "test" in args.get("which", "") or "test" in args.get("rpc_method", ""):
            # this was a test run
            results = test_results_json.get("results", [])
            for result in results:
                try:
                    test_result = DBTTestResult.parse_obj(result)
                    id = test_result.unique_id
                    test_node = test_nodes_map.get(id)
                    assert test_node, f"Failed to find test_node {id} in the catalog"
                    upstream_urns = get_upstreams(
                        test_node.upstream_nodes,
                        manifest_nodes,
                        config.use_identifiers,
                        config.target_platform,
                        config.target_platform_instance,
                        config.env,
                        config.platform_instance,
                        config.backcompat_skip_source_on_lineage_edge,
                    )
                    assertion_urn = mce_builder.make_assertion_urn(
                        mce_builder.datahub_guid(
                            {
                                "platform": DBT_PLATFORM,
                                "name": test_result.unique_id,
                                "instance": config.platform_instance,
                            }
                        )
                    )

                    if test_result.status != "pass":
                        native_results = {"message": test_result.message or ""}
                        if test_result.failures:
                            native_results.update(
                                {"failures": str(test_result.failures)}
                            )
                    else:
                        native_results = {}

                    stage_timings = {x.name: x.started_at for x in test_result.timing}
                    # look for execution start time, fall back to compile start time and finally generation time
                    execution_timestamp = (
                        stage_timings.get("execute")
                        or stage_timings.get("compile")
                        or dbt_metadata.generated_at
                    )

                    execution_timestamp_parsed = datetime.strptime(
                        execution_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ"
                    )

                    for upstream in upstream_urns:
                        assertionResult = AssertionRunEventClass(
                            timestampMillis=int(
                                execution_timestamp_parsed.timestamp() * 1000.0
                            ),
                            assertionUrn=assertion_urn,
                            asserteeUrn=upstream,
                            runId=dbt_metadata.invocation_id,
                            result=AssertionResultClass(
                                type=AssertionResultTypeClass.SUCCESS
                                if test_result.status == "pass"
                                else AssertionResultTypeClass.FAILURE,
                                nativeResults=native_results,
                            ),
                            status=AssertionRunStatusClass.COMPLETE,
                        )

                        event = MetadataChangeProposalWrapper(
                            entityType="assertion",
                            entityUrn=assertion_urn,
                            changeType=ChangeTypeClass.UPSERT,
                            aspectName="assertionRunEvent",
                            aspect=assertionResult,
                        )
                        yield MetadataWorkUnit(
                            id=f"{assertion_urn}-assertionRunEvent-{upstream}",
                            mcp=event,
                        )
                except Exception as e:
                    logger.debug(f"Failed to process test result {result} due to {e}")


@platform_name("dbt")
@config_class(DBTConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "", supported=False)
class DBTSource(StatefulIngestionSourceBase):
    """
    This plugin pulls metadata from dbt's artifact files and generates:
    - dbt Tables: for nodes in the dbt manifest file that are models materialized as tables
    - dbt Views: for nodes in the dbt manifest file that are models materialized as views
    - dbt Ephemeral: for nodes in the dbt manifest file that are ephemeral models
    - dbt Sources: for nodes that are sources on top of the underlying platform tables
    - dbt Seed: for seed entities
    - dbt Tests as Assertions: for dbt test entities (starting with version 0.8.38.1)

    Note:
    1. It also generates lineage between the `dbt` nodes (e.g. ephemeral nodes that depend on other dbt sources) as well as lineage between the `dbt` nodes and the underlying (target) platform nodes (e.g. BigQuery Table -> dbt Source, dbt View -> BigQuery View).
    2. We also support automated actions (like add a tag, term or owner) based on properties defined in dbt meta.

    The artifacts used by this source are:
    - [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
      - This file contains model, source, tests and lineage data.
    - [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
      - This file contains schema data.
      - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
    - [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
      - This file contains metadata for sources with freshness checks.
      - We transfer dbt's freshness checks to DataHub's last-modified fields.
      - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.
    - [dbt run_results file](https://docs.getdbt.com/reference/artifacts/run-results-json)
      - This file contains metadata from the result of a dbt run, e.g. dbt test
      - When provided, we transfer dbt test run results into assertion run events to see a timeline of test runs on the dataset
    """

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTConfig.parse_obj(config_dict)
        return cls(config, ctx, "dbt")

    def __init__(self, config: DBTConfig, ctx: PipelineContext, platform: str):
        super().__init__(config, ctx)
        self.config: DBTConfig = config
        self.platform: str = platform
        self.report: DBTSourceReport = DBTSourceReport()
        self.compiled_owner_extraction_pattern: Optional[Any] = None
        if self.config.owner_extraction_pattern:
            self.compiled_owner_extraction_pattern = re.compile(
                self.config.owner_extraction_pattern
            )

    def get_last_dbt_checkpoint(
        self, job_id: JobId, checkpoint_state_class: Type[DbtCheckpointState]
    ) -> Optional[Checkpoint]:

        last_checkpoint: Optional[Checkpoint]
        is_conversion_required: bool = False
        try:
            # Best-case that last checkpoint state is DbtCheckpointState
            last_checkpoint = self.get_last_checkpoint(job_id, checkpoint_state_class)
        except Exception as e:
            # Backward compatibility for old dbt ingestion source which was saving dbt-nodes in
            # BaseSQLAlchemyCheckpointState
            last_checkpoint = self.get_last_checkpoint(
                job_id, BaseSQLAlchemyCheckpointState
            )
            logger.debug(
                f"Found BaseSQLAlchemyCheckpointState as checkpoint state (got {e})."
            )
            is_conversion_required = True

        if last_checkpoint is not None and is_conversion_required:
            # Map the BaseSQLAlchemyCheckpointState to DbtCheckpointState
            dbt_checkpoint_state: DbtCheckpointState = DbtCheckpointState()
            dbt_checkpoint_state.encoded_node_urns = (
                cast(BaseSQLAlchemyCheckpointState, last_checkpoint.state)
            ).encoded_table_urns
            # Old dbt source was not supporting the assertion
            dbt_checkpoint_state.encoded_assertion_urns = []
            last_checkpoint.state = dbt_checkpoint_state

        return last_checkpoint

    # TODO: Consider refactoring this logic out for use across sources as it is leading to a significant amount of
    #  code duplication.
    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        last_checkpoint: Optional[Checkpoint] = self.get_last_dbt_checkpoint(
            self.get_default_ingestion_job_id(), DbtCheckpointState
        )
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )

        if (
            self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
            and last_checkpoint is not None
            and last_checkpoint.state is not None
            and cur_checkpoint is not None
            and cur_checkpoint.state is not None
        ):
            logger.debug("Checking for stale entity removal.")

            def get_soft_delete_item_workunit(urn: str, type: str) -> MetadataWorkUnit:

                logger.info(f"Soft-deleting stale entity of type {type} - {urn}.")
                mcp = MetadataChangeProposalWrapper(
                    entityType=type,
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=StatusClass(removed=True),
                )
                wu = MetadataWorkUnit(id=f"soft-delete-{type}-{urn}", mcp=mcp)
                self.report.report_workunit(wu)
                self.report.report_stale_entity_soft_deleted(urn)
                return wu

            last_checkpoint_state = cast(DbtCheckpointState, last_checkpoint.state)
            cur_checkpoint_state = cast(DbtCheckpointState, cur_checkpoint.state)

            urns_to_soft_delete_by_type: Dict = {
                "dataset": [
                    node_urn
                    for node_urn in last_checkpoint_state.get_node_urns_not_in(
                        cur_checkpoint_state
                    )
                ],
                "assertion": [
                    assertion_urn
                    for assertion_urn in last_checkpoint_state.get_assertion_urns_not_in(
                        cur_checkpoint_state
                    )
                ],
            }
            for entity_type in urns_to_soft_delete_by_type:
                for urn in urns_to_soft_delete_by_type[entity_type]:
                    yield get_soft_delete_item_workunit(urn, entity_type)

    def load_file_as_json(self, uri: str) -> Any:
        if re.match("^https?://", uri):
            return json.loads(requests.get(uri).text)
        elif re.match("^s3://", uri):
            u = urlparse(uri)
            response = self.config.s3_client.get_object(
                Bucket=u.netloc, Key=u.path.lstrip("/")
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        else:
            with open(uri, "r") as f:
                return json.load(f)

    def loadManifestAndCatalog(
        self,
        manifest_path: str,
        catalog_path: str,
        sources_path: Optional[str],
        use_identifiers: bool,
        tag_prefix: str,
        node_type_pattern: AllowDenyPattern,
        report: DBTSourceReport,
        node_name_pattern: AllowDenyPattern,
    ) -> Tuple[
        List[DBTNode],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Dict[str, Dict[str, Any]],
    ]:
        dbt_manifest_json = self.load_file_as_json(manifest_path)

        dbt_catalog_json = self.load_file_as_json(catalog_path)

        if sources_path is not None:
            dbt_sources_json = self.load_file_as_json(sources_path)
            sources_results = dbt_sources_json["results"]
        else:
            sources_results = {}

        manifest_schema = dbt_manifest_json.get("metadata", {}).get(
            "dbt_schema_version"
        )
        manifest_version = dbt_manifest_json.get("metadata", {}).get("dbt_version")
        manifest_adapter = dbt_manifest_json.get("metadata", {}).get("adapter_type")

        catalog_schema = dbt_catalog_json.get("metadata", {}).get("dbt_schema_version")
        catalog_version = dbt_catalog_json.get("metadata", {}).get("dbt_version")

        manifest_nodes = dbt_manifest_json["nodes"]
        manifest_sources = dbt_manifest_json["sources"]

        all_manifest_entities = {**manifest_nodes, **manifest_sources}

        catalog_nodes = dbt_catalog_json["nodes"]
        catalog_sources = dbt_catalog_json["sources"]

        all_catalog_entities = {**catalog_nodes, **catalog_sources}

        nodes = extract_dbt_entities(
            all_manifest_entities,
            all_catalog_entities,
            sources_results,
            manifest_adapter,
            use_identifiers,
            tag_prefix,
            node_type_pattern,
            report,
            node_name_pattern,
            self.config.entities_enabled,
        )

        return (
            nodes,
            manifest_schema,
            manifest_version,
            manifest_adapter,
            catalog_schema,
            catalog_version,
            all_manifest_entities,
        )

    def create_test_entity_mcps(
        self,
        test_nodes: List[DBTNode],
        custom_props: Dict[str, str],
        manifest_nodes: Dict[str, Dict[str, Any]],
    ) -> Iterable[MetadataWorkUnit]:
        def string_map(input_map: Dict[str, Any]) -> Dict[str, str]:
            return {k: str(v) for k, v in input_map.items()}

        if not self.config.entities_enabled.can_emit_node_type("test"):
            return []

        for node in test_nodes:
            node_datahub_urn = mce_builder.make_assertion_urn(
                mce_builder.datahub_guid(
                    {
                        "platform": DBT_PLATFORM,
                        "name": node.dbt_name,
                        "instance": self.config.platform_instance,
                    }
                )
            )
            self.save_checkpoint(node_datahub_urn, "assertion")

            dpi_mcp = MetadataChangeProposalWrapper(
                entityType="assertion",
                entityUrn=node_datahub_urn,
                changeType=ChangeTypeClass.UPSERT,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=mce_builder.make_data_platform_urn(DBT_PLATFORM)
                ),
            )
            wu = MetadataWorkUnit(
                id=f"{node_datahub_urn}-dataplatformInstance", mcp=dpi_mcp
            )
            self.report.report_workunit(wu)
            yield wu

            upstream_urns = get_upstreams(
                upstreams=node.upstream_nodes,
                all_nodes=manifest_nodes,
                use_identifiers=self.config.use_identifiers,
                target_platform=self.config.target_platform,
                target_platform_instance=self.config.target_platform_instance,
                environment=self.config.env,
                platform_instance=None,
                legacy_skip_source_lineage=self.config.backcompat_skip_source_on_lineage_edge,
            )

            raw_node = manifest_nodes.get(node.dbt_name)
            if raw_node is None:
                logger.warning(
                    f"Failed to find test node {node.dbt_name} in the manifest"
                )
                continue

            test_metadata = raw_node.get("test_metadata", {})
            kw_args = test_metadata.get("kwargs", {})
            for upstream_urn in upstream_urns:
                qualified_test_name = (
                    (test_metadata.get("namespace") or "")
                    + "."
                    + (test_metadata.get("name") or "")
                )
                qualified_test_name = (
                    qualified_test_name[1:]
                    if qualified_test_name.startswith(".")
                    else qualified_test_name
                )

                if qualified_test_name in DBTTest.test_name_to_assertion_map:
                    assertion_params: AssertionParams = (
                        DBTTest.test_name_to_assertion_map[qualified_test_name]
                    )
                    assertion_info = AssertionInfoClass(
                        type=AssertionTypeClass.DATASET,
                        customProperties=custom_props,
                        datasetAssertion=DatasetAssertionInfoClass(
                            dataset=upstream_urn,
                            scope=assertion_params.scope,
                            operator=assertion_params.operator,
                            fields=[
                                mce_builder.make_schema_field_urn(
                                    upstream_urn, kw_args.get("column_name")
                                )
                            ]
                            if assertion_params.scope
                            == DatasetAssertionScopeClass.DATASET_COLUMN
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
                elif kw_args.get("column_name"):
                    # no match with known test types, column-level test
                    assertion_info = AssertionInfoClass(
                        type=AssertionTypeClass.DATASET,
                        customProperties=custom_props,
                        datasetAssertion=DatasetAssertionInfoClass(
                            dataset=upstream_urn,
                            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                            operator=AssertionStdOperatorClass._NATIVE_,
                            fields=[
                                mce_builder.make_schema_field_urn(
                                    upstream_urn, kw_args.get("column_name")
                                )
                            ],
                            nativeType=node.name,
                            logic=node.compiled_sql
                            if node.compiled_sql
                            else node.raw_sql,
                            aggregation=AssertionStdAggregationClass._NATIVE_,
                            nativeParameters=string_map(kw_args),
                        ),
                    )
                else:
                    # no match with known test types, default to row-level test
                    assertion_info = AssertionInfoClass(
                        type=AssertionTypeClass.DATASET,
                        customProperties=custom_props,
                        datasetAssertion=DatasetAssertionInfoClass(
                            dataset=upstream_urn,
                            scope=DatasetAssertionScopeClass.DATASET_ROWS,
                            operator=AssertionStdOperatorClass._NATIVE_,
                            logic=node.compiled_sql
                            if node.compiled_sql
                            else node.raw_sql,
                            nativeType=node.name,
                            aggregation=AssertionStdAggregationClass._NATIVE_,
                            nativeParameters=string_map(kw_args),
                        ),
                    )
                wu = MetadataWorkUnit(
                    id=f"{node_datahub_urn}-assertioninfo",
                    mcp=MetadataChangeProposalWrapper(
                        entityType="assertion",
                        entityUrn=node_datahub_urn,
                        changeType=ChangeTypeClass.UPSERT,
                        aspectName="assertionInfo",
                        aspect=assertion_info,
                    ),
                )
                self.report.report_workunit(wu)
                yield wu

    # create workunits from dbt nodes
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        if self.config.write_semantics == "PATCH" and not self.ctx.graph:
            raise ConfigurationError(
                "With PATCH semantics, dbt source requires a datahub_api to connect to. "
                "Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe."
            )

        (
            nodes,
            manifest_schema,
            manifest_version,
            manifest_adapter,
            catalog_schema,
            catalog_version,
            manifest_nodes_raw,
        ) = self.loadManifestAndCatalog(
            self.config.manifest_path,
            self.config.catalog_path,
            self.config.sources_path,
            self.config.use_identifiers,
            self.config.tag_prefix,
            self.config.node_type_pattern,
            self.report,
            self.config.node_name_pattern,
        )

        additional_custom_props = {
            "manifest_schema": manifest_schema,
            "manifest_version": manifest_version,
            "manifest_adapter": manifest_adapter,
            "catalog_schema": catalog_schema,
            "catalog_version": catalog_version,
        }

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
            manifest_nodes_raw,
            DBT_PLATFORM,
            self.config.platform_instance,
        )

        yield from self.create_platform_mces(
            non_test_nodes,
            additional_custom_props_filtered,
            manifest_nodes_raw,
            self.config.target_platform,
            self.config.target_platform_instance,
        )

        yield from self.create_test_entity_mcps(
            test_nodes,
            additional_custom_props_filtered,
            manifest_nodes_raw,
        )

        if self.config.test_results_path:
            yield from DBTTest.load_test_results(
                self.config,
                self.load_file_as_json(self.config.test_results_path),
                test_nodes,
                manifest_nodes_raw,
            )

        if self.is_stateful_ingestion_configured():
            # Clean up stale entities.
            yield from self.gen_removed_entity_workunits()

    def remove_duplicate_urns_from_checkpoint_state(self) -> None:
        """
        During MCEs creation process some nodes getting processed more than once and hence
        duplicates URN are getting added in checkpoint_state.
        This function will remove duplicates
        """
        if not self.is_stateful_ingestion_configured():
            return

        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )

        if cur_checkpoint is not None:
            checkpoint_state = cast(DbtCheckpointState, cur_checkpoint.state)
            checkpoint_state.encoded_node_urns = list(
                set(checkpoint_state.encoded_node_urns)
            )
            checkpoint_state.encoded_assertion_urns = list(
                set(checkpoint_state.encoded_assertion_urns)
            )

    def create_platform_mces(
        self,
        dbt_nodes: List[DBTNode],
        additional_custom_props_filtered: Dict[str, str],
        manifest_nodes_raw: Dict[str, Dict[str, Any]],
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

            node_datahub_urn = get_urn_from_dbtNode(
                node.database,
                node.schema,
                node.name,
                mce_platform,
                self.config.env,
                mce_platform_instance,
            )
            if not self.config.entities_enabled.can_emit_node_type(node.node_type):
                logger.debug(
                    f"Skipping emission of node {node_datahub_urn} because node_type {node.node_type} is disabled"
                )
                continue
            self.save_checkpoint(node_datahub_urn, "dataset")

            meta_aspects: Dict[str, Any] = {}
            if self.config.enable_meta_mapping and node.meta:
                meta_aspects = action_processor.process(node.meta)

            if self.config.enable_query_tag_mapping and node.query_tag:
                self.extract_query_tag_aspects(action_processor_tag, meta_aspects, node)

            aspects = self._generate_base_aspects(
                node, additional_custom_props_filtered, mce_platform, meta_aspects
            )

            if mce_platform == DBT_PLATFORM:
                # add upstream lineage
                upstream_lineage_class = self._create_lineage_aspect_for_dbt_node(
                    node, manifest_nodes_raw
                )
                if upstream_lineage_class:
                    aspects.append(upstream_lineage_class)

                # add view properties aspect
                if node.raw_sql:
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
                    upstream_dbt_urn = get_urn_from_dbtNode(
                        node.database,
                        node.schema,
                        node.name,
                        DBT_PLATFORM,
                        self.config.env,
                        self.config.platform_instance,
                    )
                    upstreams_lineage_class = get_upstream_lineage([upstream_dbt_urn])
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

    def save_checkpoint(self, urn: str, entity_type: str) -> None:
        # if stateful ingestion is not configured then return
        if not self.is_stateful_ingestion_configured():
            return

        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        # if no checkpoint found then return
        if cur_checkpoint is None:
            return

        # Cast and set the state
        checkpoint_state = cast(DbtCheckpointState, cur_checkpoint.state)
        checkpoint_state.set_checkpoint_urn(urn, entity_type)

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
        if self.config.github_info is not None:
            github_file_url = self.config.github_info.get_url_for_file_path(
                node.dbt_file_path
            )
            dbt_properties.externalUrl = github_file_url

        return dbt_properties

    def _create_view_properties_aspect(self, node: DBTNode) -> ViewPropertiesClass:
        materialized = node.materialization in {"table", "incremental"}
        # this function is only called when raw sql is present. assert is added to satisfy lint checks
        assert node.raw_sql is not None
        view_properties = ViewPropertiesClass(
            materialized=materialized,
            viewLanguage="SQL",
            viewLogic=node.raw_sql,
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
        schema_metadata = get_schema_metadata(self.report, node, mce_platform)
        aspects.append(schema_metadata)
        return aspects

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
            id=f"{self.platform}-{subtype_mcp.entityUrn}-{subtype_mcp.aspectName}",
            mcp=subtype_mcp,
        )
        return subtype_wu

    def _create_lineage_aspect_for_dbt_node(
        self, node: DBTNode, manifest_nodes_raw: Dict[str, Dict[str, Any]]
    ) -> Optional[UpstreamLineageClass]:
        """
        This method creates lineage amongst dbt nodes. A dbt node can be linked to other dbt nodes or a platform node.
        """
        upstream_urns = get_upstreams(
            node.upstream_nodes,
            manifest_nodes_raw,
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
                get_urn_from_dbtNode(
                    node.database,
                    node.schema,
                    node.name,
                    self.config.target_platform,
                    self.config.env,
                    self.config.target_platform_instance,
                )
            )
        if upstream_urns:
            upstreams_lineage_class = get_upstream_lineage(upstream_urns)
            return upstreams_lineage_class
        return None

    def _create_lineage_aspect_for_platform_node(
        self, node: DBTNode, manifest_nodes_raw: Dict[str, Dict[str, Any]]
    ) -> Optional[UpstreamLineage]:
        """
        This methods created lineage amongst platform nodes. Called only when dbt creation is turned off.
        """
        upstream_urns = get_upstreams(
            node.upstream_nodes,
            manifest_nodes_raw,
            self.config.use_identifiers,
            self.config.target_platform,
            self.config.target_platform_instance,
            self.config.env,
            self.config.platform_instance,
            self.config.backcompat_skip_source_on_lineage_edge,
        )
        if upstream_urns:
            return get_upstream_lineage(upstream_urns)
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
        # TODO: Remove. keeping this till PR review
        # assert owner_class is not None
        # owner = owner_class.owner
        # type = str(owner_class.type)
        # source_type = "None" if not owner_class.source else str(owner_class.source.type)
        # source_url = "None" if not owner_class.source else str(owner_class.source.url)
        # return f"{owner}-{type}-{source_type}-{source_url}"

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

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Create the custom checkpoint with empty state for the job.
        """
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.config,
                state=DbtCheckpointState(),
            )
        return None

    def get_platform_instance_id(self) -> str:
        """
        DBT project identifier is used as platform instance.
        """

        project_id = (
            self.load_file_as_json(self.config.manifest_path)
            .get("metadata", {})
            .get("project_id")
        )
        if project_id is None:
            raise ValueError("DBT project identifier is not found in manifest")

        return f"{self.platform}_{project_id}"

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
            job_id == self.get_default_ingestion_job_id()
            and self.is_stateful_ingestion_configured()
            and self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def get_default_ingestion_job_id(self) -> JobId:
        """
        DBT ingestion job name.
        """
        return JobId(f"{self.platform}_stateful_ingestion")

    def close(self):
        self.remove_duplicate_urns_from_checkpoint_state()
        self.prepare_for_commit()

    @property
    def __bases__(self) -> Tuple[Type]:
        return (DBTSource,)
