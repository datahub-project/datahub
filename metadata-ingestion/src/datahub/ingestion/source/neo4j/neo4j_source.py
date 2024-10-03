import logging
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd
from neo4j import GraphDatabase
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    GlobalTagsClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
    UnionTypeClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Neo4jConfig(ConfigModel):
    username: str = Field(default=None, description="Neo4j Username")
    password: str = Field(default=None, description="Neo4j Password")
    uri: str = Field(default=None, description="The URI for the Neo4j server")
    gms_server: str = Field(default=None, description="Address for the gms server")
    environment: str = Field(default=None, description="Neo4j env")
    node_tag: str = Field(
        default="Node",
        description="The tag that will be used to show that the Neo4j object is a Node",
    )
    relationship_tag: str = Field(
        default="Relationship",
        description="The tag that will be used to show that the Neo4j object is a Relationship",
    )
    platform: str = Field(default="neo4j", description="Neo4j platform")
    type_mapping = {
        "string": StringTypeClass(),
        "boolean": BooleanTypeClass(),
        "float": NumberTypeClass(),
        "integer": NumberTypeClass(),
        "date": DateTypeClass(),
        "relationship": StringTypeClass(),
        "node": StringTypeClass(),
        "local_date_time": DateTypeClass(),
        "list": UnionTypeClass(),
    }


@dataclass
class Neo4jSourceReport(SourceReport):
    obj_failures: int = 0
    obj_created: int = 0


@platform_name("Metadata File")
@config_class(Neo4jConfig)
@support_status(SupportStatus.CERTIFIED)
class Neo4jSource(Source):
    def __init__(self, ctx: PipelineContext, config: Neo4jConfig):
        self.ctx = ctx
        self.config = config
        self.report = Neo4jSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = Neo4jConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_schema_field_class(
        self, col_name: str, col_type: str, **kwargs
    ) -> SchemaFieldClass:
        if kwargs["obj_type"] == "node" and col_type == "relationship":
            col_type = "node"
        else:
            col_type = col_type
        return SchemaFieldClass(
            fieldPath=col_name,
            type=SchemaFieldDataTypeClass(type=self.config.type_mapping[col_type]),
            nativeDataType=col_type,
            description=col_type.upper()
            if col_type in ("node", "relationship")
            else col_type,
            lastModified=AuditStampClass(
                time=round(time.time() * 1000), actor="urn:li:corpuser:ingestion"
            ),
        )

    def add_properties(
        self, dataset: str, description=None, custom_properties=None
    ) -> MetadataChangeProposalWrapper:
        dataset_properties = DatasetPropertiesClass(
            description=description,
            customProperties=custom_properties,
        )
        return MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn(
                platform=self.config.platform, name=dataset, env=self.config.environment
            ),
            aspect=dataset_properties,
        )

    def generate_neo4j_object(
        self, platform: str, dataset: str, columns: list, obj_type=None
    ) -> MetadataChangeProposalWrapper:
        try:
            fields = [
                self.get_schema_field_class(key, value.lower(), obj_type=obj_type)
                for d in columns
                for key, value in d.items()
            ]
            mcp = MetadataChangeProposalWrapper(
                entityUrn=make_dataset_urn(
                    platform=platform, name=dataset, env=self.config.environment
                ),
                aspect=SchemaMetadataClass(
                    schemaName=dataset,
                    platform=make_data_platform_urn(platform),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    lastModified=AuditStampClass(
                        time=round(time.time() * 1000),
                        actor="urn:li:corpuser:ingestion",
                    ),
                    fields=fields,
                ),
                systemMetadata=DatasetPropertiesClass(
                    customProperties={"properties": "property on object"}
                ),
            )
            self.report.obj_created += 1
            return mcp
        except Exception as e:
            log.error(e)
            self.report.obj_failures += 1

    def add_tag_to_dataset(
        self, table_name: str, tag_name: str
    ) -> MetadataChangeProposalWrapper:
        graph = DataHubGraph(DatahubClientConfig(server=self.config.gms_server))
        dataset_urn = make_dataset_urn(
            platform=self.config.platform, name=table_name, env=self.config.environment
        )
        current_tags: Optional[GlobalTagsClass] = graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=GlobalTagsClass,
        )
        tag_to_add = make_tag_urn(tag_name)
        tag_association_to_add = TagAssociationClass(tag=tag_to_add)

        if current_tags:
            if tag_to_add not in [x.tag for x in current_tags.tags]:
                current_tags.tags.append(TagAssociationClass(tag_to_add))
        else:
            current_tags = GlobalTagsClass(tags=[tag_association_to_add])
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=current_tags,
        )

    def get_neo4j_metadata(self, query: str) -> pd.DataFrame:
        driver = GraphDatabase.driver(
            self.config.uri, auth=(self.config.username, self.config.password)
        )
        """
        This process retrieves the metadata for Neo4j objects using an APOC query, which returns a dictionary
        with two columns: key and value. The key represents the Neo4j object, while the value contains the
        corresponding metadata.

        When data is returned from Neo4j, much of the relationship metadata is stored with the relevant node's
        metadata. Consequently, the objects are organized into two separate dataframes: one for nodes and one for
        relationships.

        In the node dataframe, several fields are extracted and added as new columns. Similarly, in the relationship
        dataframe, certain fields are parsed out, while others require metadata from the nodes dataframe.

        Once the data is parsed and these two dataframes are created, we combine a subset of their columns into a
        single dataframe, which will be used to create the DataHub objects.

        See the docs for examples of metadata:  metadata-ingestion/docs/sources/neo4j/neo4j.md
        """
        log.info(f"{query}")
        with driver.session() as session:
            result = session.run(query)
            data = [record for record in result]
        log.info("Closing Neo4j driver")
        driver.close()

        node_df = self.process_nodes(data)
        rel_df = self.process_relationships(data, node_df)

        union_cols = ["key", "obj_type", "property_data_types", "description"]
        df = pd.concat([node_df[union_cols], rel_df[union_cols]])

        return df

    def process_nodes(self, data):
        nodes = [record for record in data if record["value"]["type"] == "node"]
        node_df = pd.DataFrame(
            nodes,
            columns=["key", "value"],
        )
        node_df["obj_type"] = node_df["value"].apply(
            lambda record: self.get_obj_type(record)
        )
        node_df["relationships"] = node_df["value"].apply(
            lambda record: self.get_relationships(record)
        )
        node_df["properties"] = node_df["value"].apply(
            lambda record: self.get_properties(record)
        )
        node_df["property_data_types"] = node_df["properties"].apply(
            lambda record: self.get_property_data_types(record)
        )
        node_df["description"] = node_df.apply(
            lambda record: self.get_node_description(record, node_df), axis=1
        )
        return node_df

    def process_relationships(self, data, node_df):
        rels = [record for record in data if record["value"]["type"] == "relationship"]
        rel_df = pd.DataFrame(rels, columns=["key", "value"])
        rel_df["obj_type"] = rel_df["value"].apply(
            lambda record: self.get_obj_type(record)
        )
        rel_df["properties"] = rel_df["value"].apply(
            lambda record: self.get_properties(record)
        )
        rel_df["property_data_types"] = rel_df["properties"].apply(
            lambda record: self.get_property_data_types(record)
        )
        rel_df["description"] = rel_df.apply(
            lambda record: self.get_rel_descriptions(record, node_df), axis=1
        )
        return rel_df

    def get_obj_type(self, record: dict) -> str:
        return record["type"]

    def get_rel_descriptions(self, record: dict, df: pd.DataFrame) -> str:
        descriptions = []
        for _, row in df.iterrows():
            relationships = row.get("relationships", {})
            for relationship, props in relationships.items():
                if record["key"] == relationship:
                    if props["direction"] == "in":
                        for prop in props["labels"]:
                            descriptions.append(
                                f"({row['key']})-[{record['key']}]->({prop})"
                            )
        return "\n".join(descriptions)

    def get_node_description(self, record: dict, df: pd.DataFrame) -> str:
        descriptions = []
        for _, row in df.iterrows():
            if record["key"] == row["key"]:
                for relationship, props in row["relationships"].items():
                    direction = props["direction"]
                    for node in set(props["labels"]):
                        if direction == "in":
                            descriptions.append(
                                f"({row['key']})<-[{relationship}]-({node})"
                            )
                        elif direction == "out":
                            descriptions.append(
                                f"({row['key']})-[{relationship}]->({node})"
                            )

        return "\n".join(descriptions)

    def get_property_data_types(self, record: dict) -> list[dict]:
        return [{k: v["type"]} for k, v in record.items()]

    def get_properties(self, record: dict) -> str:
        return record["properties"]

    def get_relationships(self, record: dict) -> dict:
        return record.get("relationships", None)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        df = self.get_neo4j_metadata(
            "CALL apoc.meta.schema() YIELD value UNWIND keys(value) AS key RETURN key, value[key] AS value;"
        )
        for index, row in df.iterrows():
            try:
                yield MetadataWorkUnit(
                    id=row["key"],
                    mcp_raw=self.generate_neo4j_object(
                        columns=row["property_data_types"],
                        dataset=row["key"],
                        platform=self.config.platform,
                    ),
                )

                yield MetadataWorkUnit(
                    id=row["key"],
                    mcp=self.add_tag_to_dataset(
                        table_name=row["key"],
                        tag_name=self.config.node_tag
                        if row["obj_type"] == "node"
                        else self.config.relationship_tag,
                    ),
                )

                yield MetadataWorkUnit(
                    id=row["key"],
                    mcp=self.add_properties(
                        dataset=row["key"],
                        custom_properties=None,
                        description=row["description"],
                    ),
                )

            except Exception as e:
                raise e

    def get_report(self):
        return self.report
