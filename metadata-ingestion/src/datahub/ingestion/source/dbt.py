import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import dateutil.parser

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import DEFAULT_ENV
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
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger = logging.getLogger(__name__)


class DBTConfig(ConfigModel):
    manifest_path: str
    catalog_path: str
    sources_path: Optional[str]
    env: str = DEFAULT_ENV
    target_platform: str
    load_schemas: bool
    use_identifiers: bool = False
    node_type_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class DBTColumn:
    name: str
    comment: str
    description: str
    index: int
    data_type: str

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

    datahub_urn: str

    dbt_name: str
    dbt_file_path: str

    node_type: str  # source, model
    max_loaded_at: Optional[str]
    materialization: Optional[str]  # table, view, ephemeral, incremental
    # see https://docs.getdbt.com/reference/artifacts/manifest-json
    catalog_type: Optional[str]

    columns: List[DBTColumn] = field(default_factory=list)
    upstream_urns: List[str] = field(default_factory=list)

    meta: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self):
        fields = tuple("{}={}".format(k, v) for k, v in self.__dict__.items())
        return self.__class__.__name__ + str(tuple(sorted(fields))).replace("'", "")


def get_columns(catalog_node: dict, manifest_node: dict) -> List[DBTColumn]:
    columns = []

    manifest_columns = manifest_node.get("columns", {})

    raw_columns = catalog_node["columns"]

    for key in raw_columns:
        raw_column = raw_columns[key]

        dbtCol = DBTColumn(
            name=raw_column["name"].lower(),
            comment=raw_column.get("comment", ""),
            description=manifest_columns.get(key.lower(), {}).get("description", ""),
            data_type=raw_column["type"],
            index=raw_column["index"],
        )
        columns.append(dbtCol)
    return columns


def extract_dbt_entities(
    all_manifest_entities: Dict[str, Dict[str, Any]],
    all_catalog_entities: Dict[str, Dict[str, Any]],
    sources_results: List[Dict[str, Any]],
    load_schemas: bool,
    use_identifiers: bool,
    target_platform: str,
    environment: str,
    node_type_pattern: AllowDenyPattern,
    report: SourceReport,
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

        # initialize comment to "" for consistency with descriptions
        # (since dbt null/undefined descriptions as "")
        comment = ""

        if key in all_catalog_entities and all_catalog_entities[key]["metadata"].get(
            "comment"
        ):
            comment = all_catalog_entities[key]["metadata"]["comment"]

        materialization = None
        upstream_urns = []

        if "materialized" in manifest_node.get("config", {}).keys():
            # It's a model
            materialization = manifest_node["config"]["materialized"]
            upstream_urns = get_upstreams(
                manifest_node["depends_on"]["nodes"],
                all_manifest_entities,
                load_schemas,
                use_identifiers,
                target_platform,
                environment,
            )

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

        dbtNode = DBTNode(
            dbt_name=key,
            database=manifest_node["database"],
            schema=manifest_node["schema"],
            dbt_file_path=manifest_node["original_file_path"],
            node_type=manifest_node["resource_type"],
            max_loaded_at=sources_by_id.get(key, {}).get("max_loaded_at"),
            name=name,
            comment=comment,
            description=manifest_node.get("description", ""),
            upstream_urns=upstream_urns,
            materialization=materialization,
            catalog_type=catalog_type,
            columns=[],
            datahub_urn=get_urn_from_dbtNode(
                manifest_node["database"],
                manifest_node["schema"],
                name,
                target_platform,
                environment,
            ),
            meta=manifest_node.get("meta", {}),
        )

        # overwrite columns from catalog
        if (
            dbtNode.materialization != "ephemeral" and load_schemas
        ):  # we don't want columns if platform isn't 'dbt'
            logger.debug("Loading schema info")
            catalog_node = all_catalog_entities.get(key)

            if catalog_node is None:
                report.report_warning(
                    key,
                    f"Entity {dbtNode.dbt_name} is in manifest but missing from catalog",
                )
            else:
                dbtNode.columns = get_columns(catalog_node, manifest_node)

        else:
            dbtNode.columns = []

        dbt_entities.append(dbtNode)

    return dbt_entities


def loadManifestAndCatalog(
    manifest_path: str,
    catalog_path: str,
    sources_path: Optional[str],
    load_schemas: bool,
    use_identifiers: bool,
    target_platform: str,
    environment: str,
    node_type_pattern: AllowDenyPattern,
    report: SourceReport,
) -> Tuple[List[DBTNode], Optional[str], Optional[str], Optional[str], Optional[str]]:
    with open(manifest_path, "r") as manifest:
        dbt_manifest_json = json.load(manifest)

    with open(catalog_path, "r") as catalog:
        dbt_catalog_json = json.load(catalog)

    if sources_path is not None:
        with open(sources_path, "r") as sources:
            dbt_sources_json = json.load(sources)
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
        target_platform,
        environment,
        node_type_pattern,
        report,
    )

    return nodes, manifest_schema, manifest_version, catalog_schema, catalog_version


def get_urn_from_dbtNode(
    database: str, schema: str, name: str, target_platform: str, env: str
) -> str:

    db_fqn = f"{database}.{schema}.{name}".replace('"', "")
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
    all_nodes: Dict[str, dict],
    load_schemas: bool,
    use_identifiers: bool,
    target_platform: str,
    environment: str,
) -> List[str]:
    upstream_urns = []

    for upstream in upstreams:

        if "identifier" in all_nodes[upstream] and use_identifiers:
            name = all_nodes[upstream]["identifier"]
        else:
            name = all_nodes[upstream]["name"]

        if "alias" in all_nodes[upstream]:
            name = all_nodes[upstream]["alias"]

        upstream_urns.append(
            get_urn_from_dbtNode(
                all_nodes[upstream]["database"],
                all_nodes[upstream]["schema"],
                name,
                target_platform,
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

        field = SchemaField(
            fieldPath=column.name,
            nativeDataType=column.data_type,
            type=get_column_type(report, node.dbt_name, column.data_type),
            description=description,
            nullable=False,  # TODO: actually autodetect this
            recursive=False,
        )

        canonical_schema.append(field)

    last_modified = None
    if node.max_loaded_at is not None:
        actor = "urn:li:corpuser:dbt_executor"
        last_modified = AuditStamp(
            time=int(dateutil.parser.parse(node.max_loaded_at).timestamp() * 1000),
            actor=actor,
        )

    description = None

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

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        (
            nodes,
            manifest_schema,
            manifest_version,
            catalog_schema,
            catalog_version,
        ) = loadManifestAndCatalog(
            self.config.manifest_path,
            self.config.catalog_path,
            self.config.sources_path,
            self.config.load_schemas,
            self.config.use_identifiers,
            self.config.target_platform,
            self.config.env,
            self.config.node_type_pattern,
            self.report,
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

        for node in nodes:

            dataset_snapshot = DatasetSnapshot(
                urn=node.datahub_urn,
                aspects=[],
            )

            description = None

            if node.comment and node.description and node.comment != node.description:
                description = f"{self.config.target_platform} comment: {node.comment}\n\ndbt model description: {node.description}"
            elif node.comment:
                description = node.comment
            elif node.description:
                description = node.description

            custom_props = {
                **get_custom_properties(node),
                **additional_custom_props_filtered,
            }

            dbt_properties = DatasetPropertiesClass(
                description=description,
                customProperties=custom_props,
                tags=[],
            )
            dataset_snapshot.aspects.append(dbt_properties)

            upstreams = get_upstream_lineage(node.upstream_urns)
            if upstreams is not None:
                dataset_snapshot.aspects.append(upstreams)

            if self.config.load_schemas:
                schema_metadata = get_schema_metadata(
                    self.report, node, self.config.target_platform
                )
                dataset_snapshot.aspects.append(schema_metadata)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = MetadataWorkUnit(id=dataset_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)

            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
