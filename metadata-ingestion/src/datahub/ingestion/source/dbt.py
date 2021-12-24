import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import dateutil.parser
import requests
from pydantic import validator

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter import mce_builder
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import (
    BIGQUERY_TYPES_MAP,
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    resolve_postgres_modified_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
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
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetKeyClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)

logger = logging.getLogger(__name__)
DBT_PLATFORM = "dbt"


class DBTConfig(ConfigModel):
    manifest_path: str
    catalog_path: str
    sources_path: Optional[str]
    env: str = DEFAULT_ENV
    target_platform: str
    load_schemas: bool = True
    use_identifiers: bool = False
    node_type_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    tag_prefix: str = f"{DBT_PLATFORM}:"
    node_name_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    disable_dbt_node_creation = False

    @validator("target_platform")
    def validate_target_platform_value(cls, target_platform: str) -> str:
        if target_platform.lower() == DBT_PLATFORM:
            raise ValueError(
                "target_platform cannot be dbt. It should be the platform which dbt is operating on top of. For e.g "
                "postgres."
            )
        return target_platform


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
    database: str
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
    report: SourceReport,
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

        meta = manifest_node.get("meta", {})

        owner = meta.get("owner")
        if owner is None:
            owner = manifest_node.get("config", {}).get("meta", {}).get("owner")

        tags = manifest_node.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]
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
            meta=manifest_node.get("meta", {}),
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
    report: SourceReport,
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


def get_db_fqn(database: str, schema: str, name: str) -> str:
    return f"{database}.{schema}.{name}".replace('"', "")


def get_urn_from_dbtNode(
    database: str, schema: str, name: str, target_platform: str, env: str
) -> str:
    db_fqn = get_db_fqn(database, schema, name)
    return f"urn:li:dataset:(urn:li:dataPlatform:{target_platform},{db_fqn},{env})"


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
    **POSTGRES_TYPES_MAP,
    **SNOWFLAKE_TYPES_MAP,
    **BIGQUERY_TYPES_MAP,
}


def get_column_type(
    report: SourceReport, dataset_name: str, column_type: str
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
    report: SourceReport, node: DBTNode, platform: str
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
                tags=[TagAssociationClass(f"urn:li:tag:{tag}") for tag in column.tags]
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
        actor = "urn:li:corpuser:dbt_executor"
        last_modified = AuditStamp(
            time=int(dateutil.parser.parse(node.max_loaded_at).timestamp() * 1000),
            actor=actor,
        )

    return SchemaMetadata(
        schemaName=node.dbt_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
        lastModified=last_modified,
        fields=canonical_schema,
    )


class DBTSource(Source):
    """Extract DBT metadata for ingestion to Datahub"""

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTConfig.parse_obj(config_dict)
        return cls(config, ctx, "dbt")

    def __init__(self, config: DBTConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SourceReport()

    # create workunits from dbt nodes
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
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
        for node in dbt_nodes:
            node_datahub_urn = get_urn_from_dbtNode(
                node.database,
                node.schema,
                node.name,
                mce_platform,
                self.config.env,
            )
            aspects = self._generate_base_aspects(
                node, additional_custom_props_filtered, mce_platform
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
                        dataset_key = mce_builder.dataset_urn_to_key(node_datahub_urn)
                        assert dataset_key is not None
                        key_aspect = DatasetKeyClass(
                            "urn:li:dataPlatform:" + dataset_key.platform,
                            dataset_key.name,
                            dataset_key.origin,
                        )
                        aspects.append(key_aspect)
                else:
                    # add upstream lineage
                    aspects.append(
                        self._create_lineage_aspect_for_platform_node(
                            node, manifest_nodes_raw
                        )
                    )

            dataset_snapshot = DatasetSnapshot(urn=node_datahub_urn, aspects=aspects)
            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)
            yield wu

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
        )
        return dbt_properties

    def _get_owners_aspect(self, node: DBTNode) -> OwnershipClass:
        owners = [
            OwnerClass(
                owner=f"urn:li:corpuser:{node.owner}",
                type=OwnershipTypeClass.DATAOWNER,
            )
        ]
        return OwnershipClass(
            owners=owners,
        )

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

        # add owners aspect
        if node.owner:
            aspects.append(self._get_owners_aspect(node))

        # add tags aspects
        if node.tags:
            aspects.append(mce_builder.make_global_tag_aspect_with_tag_list(node.tags))

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
    ) -> UpstreamLineage:
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
        return get_upstream_lineage(upstream_urns)

    def get_report(self):
        return self.report

    def close(self):
        pass
