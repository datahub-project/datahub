import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, cast

import dateutil.parser
import requests
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
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
    resolve_postgres_modified_type,
)
from datahub.ingestion.source.state.checkpoint import Checkpoint
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
    ChangeTypeClass,
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
    env: str = Field(
        default=mce_builder.DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )
    target_platform: str = Field(
        description="The platform that dbt is loading onto. (e.g. bigquery / redshift / postgres etc.)"
    )
    load_schemas: bool = Field(
        default=True,
        description="This flag is only consulted when disable_dbt_node_creation is set to True. Load schemas for target_platform entities from dbt catalog file, not necessary when you are already ingesting this metadata from the data platform directly. If set to False, table schema details (e.g. columns) will not be ingested.",
    )
    use_identifiers: bool = Field(
        default=False,
        description="Use model identifier instead of model name if defined (if not, default to model name).",
    )
    node_type_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for dbt nodes to filter in ingestion.",
    )
    tag_prefix: str = Field(
        default=f"{DBT_PLATFORM}:", description="Prefix added to tags during ingestion."
    )
    node_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for dbt model names to filter in ingestion.",
    )
    disable_dbt_node_creation = Field(
        default=False,
        description="Whether to suppress dbt dataset metadata creation. When set to True, this flag applies the dbt metadata to the target_platform entities (e.g. populating schema and column descriptions from dbt into the postgres / bigquery table metadata in DataHub) and generates lineage between the platform entities.",
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
    owner_extraction_pattern: Optional[str] = Field(
        default=None,
        description='Regex string to extract owner from the dbt node using the `(?P<name>...) syntax` of the [match object](https://docs.python.org/3/library/re.html#match-objects), where the group name must be `owner`. Examples: (1)`r"(?P<owner>(.*)): (\w+) (\w+)"` will extract `jdoe` as the owner from `"jdoe: John Doe"` (2) `r"@(?P<owner>(.*))"` will extract `alice` as the owner from `"@alice"`.',  # noqa: W605
    )

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
    comment: str
    description: str
    raw_sql: Optional[str]

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
    load_schemas: bool,
    use_identifiers: bool,
    tag_prefix: str,
    node_type_pattern: AllowDenyPattern,
    report: DBTSourceReport,
    node_name_pattern: AllowDenyPattern,
) -> List[DBTNode]:
    sources_by_id = {x["unique_id"]: x for x in sources_results}

    dbt_entities = []
    for key, manifest_node in all_manifest_entities.items():
        # check if node pattern allowed based on config file
        if not node_type_pattern.allowed(manifest_node["resource_type"]):
            continue

        name = manifest_node["name"]
        if "identifier" in manifest_node and use_identifiers:
            name = manifest_node["identifier"]

        if manifest_node.get("alias") is not None:
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
            database=manifest_node["database"],
            schema=manifest_node["schema"],
            name=name,
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
        )

        # overwrite columns from catalog
        if (
            dbtNode.materialization != "ephemeral"
        ):  # we don't want columns if platform isn't 'dbt'
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


def load_file_as_json(uri: str) -> Any:
    if re.match("^https?://", uri):
        return json.loads(requests.get(uri).text)
    else:
        with open(uri, "r") as f:
            return json.load(f)


def loadManifestAndCatalog(
    manifest_path: str,
    catalog_path: str,
    sources_path: Optional[str],
    load_schemas: bool,
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
    Dict[str, Dict[str, Any]],
]:
    dbt_manifest_json = load_file_as_json(manifest_path)

    dbt_catalog_json = load_file_as_json(catalog_path)

    if sources_path is not None:
        dbt_sources_json = load_file_as_json(sources_path)
        sources_results = dbt_sources_json["results"]
    else:
        sources_results = {}

    manifest_schema = dbt_manifest_json.get("metadata", {}).get("dbt_schema_version")
    manifest_version = dbt_manifest_json.get("metadata", {}).get("dbt_version")

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
        load_schemas,
        use_identifiers,
        tag_prefix,
        node_type_pattern,
        report,
        node_name_pattern,
    )

    return (
        nodes,
        manifest_schema,
        manifest_version,
        catalog_schema,
        catalog_version,
        all_manifest_entities,
    )


def get_db_fqn(database: Optional[str], schema: str, name: str) -> str:
    if database is not None:
        fqn = f"{database}.{schema}.{name}"
    else:
        fqn = f"{schema}.{name}"
    return fqn.replace('"', "")


def get_urn_from_dbtNode(
    database: Optional[str], schema: str, name: str, target_platform: str, env: str
) -> str:
    db_fqn = get_db_fqn(database, schema, name)
    return mce_builder.make_dataset_urn(target_platform, db_fqn, env)


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
    environment: str,
    disable_dbt_node_creation: bool,
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

        # This function is called to create lineages among platform nodes or dbt nodes. When we are creating lineages
        # for platform nodes, implies that dbt node creation is turned off (because otherwise platform nodes only
        # have one lineage edge to their corresponding dbt node). So, when disable_dbt_node_creation is true we only
        # create lineages for platform nodes otherwise, for dbt node, we connect it to another dbt node or a platform
        # node.
        platform_value = DBT_PLATFORM

        if disable_dbt_node_creation:
            platform_value = target_platform
        else:
            materialized = upstream_manifest_node.get("config", {}).get("materialized")
            resource_type = upstream_manifest_node["resource_type"]

            if (
                materialized in {"view", "table", "incremental"}
                or resource_type == "source"
            ):
                platform_value = target_platform

        upstream_urns.append(
            get_urn_from_dbtNode(
                all_nodes[upstream]["database"],
                all_nodes[upstream]["schema"],
                name,
                platform_value,
                environment,
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
}


def get_column_type(
    report: DBTSourceReport, dataset_name: str, column_type: str
) -> SchemaFieldDataType:
    """
    Maps known DBT types to datahub types
    """
    TypeClass: Any = _field_type_mapping.get(column_type)

    if TypeClass is None:
        # attempt Postgres modified type
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
            type=get_column_type(report, node.dbt_name, column.data_type),
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
    - dbt Test: for dbt test entities

    Note:
    1. It also generates lineage between the `dbt` nodes (e.g. ephemeral nodes that depend on other dbt sources) as well as lineage between the `dbt` nodes and the underlying (target) platform nodes (e.g. BigQuery Table -> dbt Source, dbt View -> BigQuery View).
    2. The previous version of this source (`acryl_datahub<=0.8.16.2`) did not generate `dbt` entities and lineage between `dbt` entities and platform entities. For backwards compatibility with the previous version of this source, there is a config flag `disable_dbt_node_creation` that falls back to the old behavior.
    3. We also support automated actions (like add a tag, term or owner) based on properties defined in dbt meta.

    The artifacts used by this source are:
    - [dbt manifest file](https://docs.getdbt.com/reference/artifacts/manifest-json)
      - This file contains model, source and lineage data.
    - [dbt catalog file](https://docs.getdbt.com/reference/artifacts/catalog-json)
      - This file contains schema data.
      - dbt does not record schema data for Ephemeral models, as such datahub will show Ephemeral models in the lineage, however there will be no associated schema for Ephemeral models
    - [dbt sources file](https://docs.getdbt.com/reference/artifacts/sources-json)
      - This file contains metadata for sources with freshness checks.
      - We transfer dbt's freshness checks to DataHub's last-modified fields.
      - Note that this file is optional – if not specified, we'll use time of ingestion instead as a proxy for time last-modified.

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

    # TODO: Consider refactoring this logic out for use across sources as it is leading to a significant amount of
    #  code duplication.
    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), BaseSQLAlchemyCheckpointState
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

            def soft_delete_item(urn: str, type: str) -> Iterable[MetadataWorkUnit]:

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
                yield wu

            last_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, last_checkpoint.state
            )
            cur_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, cur_checkpoint.state
            )

            for table_urn in last_checkpoint_state.get_table_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_item(table_urn, "dataset")

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
            catalog_schema,
            catalog_version,
            manifest_nodes_raw,
        ) = loadManifestAndCatalog(
            self.config.manifest_path,
            self.config.catalog_path,
            self.config.sources_path,
            self.config.load_schemas,
            self.config.use_identifiers,
            self.config.tag_prefix,
            self.config.node_type_pattern,
            self.report,
            self.config.node_name_pattern,
        )

        additional_custom_props = {
            "manifest_schema": manifest_schema,
            "manifest_version": manifest_version,
            "catalog_schema": catalog_schema,
            "catalog_version": catalog_version,
        }

        additional_custom_props_filtered = {
            key: value
            for key, value in additional_custom_props.items()
            if value is not None
        }

        if not self.config.disable_dbt_node_creation:
            yield from self.create_platform_mces(
                nodes,
                additional_custom_props_filtered,
                manifest_nodes_raw,
                DBT_PLATFORM,
            )

        yield from self.create_platform_mces(
            nodes,
            additional_custom_props_filtered,
            manifest_nodes_raw,
            self.config.target_platform,
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
            # Utilizing BaseSQLAlchemyCheckpointState class to save state
            checkpoint_state = cast(BaseSQLAlchemyCheckpointState, cur_checkpoint.state)
            checkpoint_state.encoded_table_urns = list(
                set(checkpoint_state.encoded_table_urns)
            )

    def create_platform_mces(
        self,
        dbt_nodes: List[DBTNode],
        additional_custom_props_filtered: Dict[str, str],
        manifest_nodes_raw: Dict[str, Dict[str, Any]],
        mce_platform: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        This function creates mce based out of dbt nodes. Since dbt ingestion creates "dbt" nodes
        and nodes for underlying platform the function gets called twice based on the mce_platform
        parameter. Further, this function takes specific actions based on the mce_platform passed in.
        If  disable_dbt_node_creation = True,
            Create empty entities of the underlying platform with only lineage/key aspect.
            Create dbt entities with all metadata information.
        If  disable_dbt_node_creation = False
            Create platform entities with all metadata information.
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
            )
            self.save_checkpoint(node_datahub_urn)

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
                if not self.config.disable_dbt_node_creation:
                    # if dbt node creation is enabled we are creating empty node for platform and only add
                    # lineage/keyaspect.
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
                        )
                        upstreams_lineage_class = get_upstream_lineage(
                            [upstream_dbt_urn]
                        )
                        aspects.append(upstreams_lineage_class)
                else:
                    # add upstream lineage
                    platform_upstream_aspect = (
                        self._create_lineage_aspect_for_platform_node(
                            node, manifest_nodes_raw
                        )
                    )
                    if platform_upstream_aspect:
                        aspects.append(platform_upstream_aspect)

            if len(aspects) == 0:
                continue
            dataset_snapshot = DatasetSnapshot(urn=node_datahub_urn, aspects=aspects)
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            if self.config.write_semantics == "PATCH":
                mce = self.get_patched_mce(mce)
            wu = MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def save_checkpoint(self, node_datahub_urn: str) -> None:
        if self.is_stateful_ingestion_configured():
            cur_checkpoint = self.get_current_checkpoint(
                self.get_default_ingestion_job_id()
            )

            if cur_checkpoint is not None:
                # Utilizing BaseSQLAlchemyCheckpointState class to save state
                checkpoint_state = cast(
                    BaseSQLAlchemyCheckpointState, cur_checkpoint.state
                )
                checkpoint_state.add_table_urn(node_datahub_urn)

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

    # TODO: Remove. keeping this till PR review
    # def get_owners_from_dataset_snapshot(self, dataset_snapshot: DatasetSnapshot) -> Optional[OwnershipClass]:
    #     for aspect in dataset_snapshot.aspects:
    #         if isinstance(aspect, OwnershipClass):
    #             return aspect
    #     return None
    #
    # def get_tag_aspect_from_dataset_snapshot(self, dataset_snapshot: DatasetSnapshot) -> Optional[GlobalTagsClass]:
    #     for aspect in dataset_snapshot.aspects:
    #         if isinstance(aspect, GlobalTagsClass):
    #             return aspect
    #     return None
    #
    # def get_term_aspect_from_dataset_snapshot(self, dataset_snapshot: DatasetSnapshot) -> Optional[GlossaryTermsClass]:
    #     for aspect in dataset_snapshot.aspects:
    #         if isinstance(aspect, GlossaryTermsClass):
    #             return aspect
    #     return None

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
        description = None
        if self.config.disable_dbt_node_creation:
            if node.comment and node.description and node.comment != node.description:
                description = f"{self.config.target_platform} comment: {node.comment}\n\ndbt model description: {node.description}"
            elif node.comment:
                description = node.comment
            elif node.description:
                description = node.description
        else:
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
        # When generating these aspects for a dbt node, we will always include schema information. When generating
        # these aspects for a platform node (which only happens when disable_dbt_node_creation is set to true) we
        # honor the flag.
        if mce_platform == DBT_PLATFORM:
            aspects.append(schema_metadata)
        else:
            if self.config.load_schemas:
                aspects.append(schema_metadata)
        return aspects

    def _aggregate_owners(
        self, node: DBTNode, meta_owner_aspects: Any
    ) -> List[OwnerClass]:
        owner_list: List[OwnerClass] = []
        if node.owner:
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

        if meta_owner_aspects and self.config.enable_meta_mapping:
            owner_list.extend(meta_owner_aspects.owners)

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
            self.config.env,
            self.config.disable_dbt_node_creation,
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
            self.config.env,
            self.config.disable_dbt_node_creation,
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
                # Reusing BaseSQLAlchemyCheckpointState as it has needed functionality to support statefulness of DBT
                state=BaseSQLAlchemyCheckpointState(),
            )
        return None

    def get_platform_instance_id(self) -> str:
        """
        DBT project identifier is used as platform instance.
        """

        project_id = (
            load_file_as_json(self.config.manifest_path)
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
