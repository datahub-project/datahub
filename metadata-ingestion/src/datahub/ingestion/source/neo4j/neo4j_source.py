import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Type, Union

import pandas as pd
from neo4j import GraphDatabase
from pydantic.fields import Field

from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
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
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaFieldDataType
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    UnionTypeClass,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

_type_mapping: Dict[Union[Type, str], Type] = {
    "list": UnionTypeClass,
    "boolean": BooleanTypeClass,
    "integer": NumberTypeClass,
    "local_date_time": DateTypeClass,
    "float": NumberTypeClass,
    "string": StringTypeClass,
    "date": DateTypeClass,
    "node": StringTypeClass,
    "relationship": StringTypeClass,
}


class Neo4jConfig(EnvConfigMixin):
    username: str = Field(description="Neo4j Username")
    password: str = Field(description="Neo4j Password")
    uri: str = Field(description="The URI for the Neo4j server")
    env: str = Field(description="Neo4j env")


@dataclass
class Neo4jSourceReport(SourceReport):
    obj_failures: int = 0
    obj_created: int = 0


@platform_name("Neo4j", id="neo4j")
@config_class(Neo4jConfig)
@support_status(SupportStatus.CERTIFIED)
class Neo4jSource(Source):
    NODE = "node"
    RELATIONSHIP = "relationship"
    PLATFORM = "neo4j"

    def __init__(self, ctx: PipelineContext, config: Neo4jConfig):
        self.ctx = ctx
        self.config = config
        self.report = Neo4jSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = Neo4jConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_field_type(self, attribute_type: Union[type, str]) -> SchemaFieldDataType:
        type_class: type = _type_mapping.get(attribute_type, NullTypeClass)
        return SchemaFieldDataType(type=type_class())

    def get_schema_field_class(
        self, col_name: str, col_type: str, **kwargs: Any
    ) -> SchemaFieldClass:
        if kwargs["obj_type"] == self.NODE and col_type == self.RELATIONSHIP:
            col_type = self.NODE
        else:
            col_type = col_type
        return SchemaFieldClass(
            fieldPath=col_name,
            type=self.get_field_type(col_type),
            nativeDataType=col_type,
            description=col_type.upper()
            if col_type in (self.NODE, self.RELATIONSHIP)
            else col_type,
            lastModified=AuditStampClass(
                time=round(time.time() * 1000), actor="urn:li:corpuser:ingestion"
            ),
        )

    def add_properties(
        self,
        dataset: str,
        description: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> MetadataChangeProposalWrapper:
        dataset_properties = DatasetPropertiesClass(
            description=description,
            customProperties=custom_properties,
        )
        return MetadataChangeProposalWrapper(
            entityUrn=make_dataset_urn(
                platform=self.PLATFORM, name=dataset, env=self.config.env
            ),
            aspect=dataset_properties,
        )

    def generate_neo4j_object(
        self, dataset: str, columns: list, obj_type: Optional[str] = None
    ) -> MetadataChangeProposalWrapper:
        try:
            fields = [
                self.get_schema_field_class(key, value.lower(), obj_type=obj_type)
                for d in columns
                for key, value in d.items()
            ]
            mcp = MetadataChangeProposalWrapper(
                entityUrn=make_dataset_urn(
                    platform=self.PLATFORM, name=dataset, env=self.config.env
                ),
                aspect=SchemaMetadataClass(
                    schemaName=dataset,
                    platform=make_data_platform_urn(self.PLATFORM),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    lastModified=AuditStampClass(
                        time=round(time.time() * 1000),
                        actor="urn:li:corpuser:ingestion",
                    ),
                    fields=fields,
                ),
            )
            self.report.obj_created += 1
        except Exception as e:
            log.error(e)
            self.report.obj_failures += 1
        return mcp

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
        try:
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
        except Exception as e:
            self.report.failure(
                message="Failed to get neo4j metadata",
                exc=e,
            )

        return df

    def process_nodes(self, data: list) -> pd.DataFrame:
        nodes = [record for record in data if record["value"]["type"] == self.NODE]
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

    def process_relationships(self, data: list, node_df: pd.DataFrame) -> pd.DataFrame:
        rels = [
            record for record in data if record["value"]["type"] == self.RELATIONSHIP
        ]
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

    def get_property_data_types(self, record: dict) -> List[dict]:
        return [{k: v["type"]} for k, v in record.items()]

    def get_properties(self, record: dict) -> str:
        return record["properties"]

    def get_relationships(self, record: dict) -> dict:
        return record.get("relationships", None)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        df = self.get_neo4j_metadata(
            "CALL apoc.meta.schema() YIELD value UNWIND keys(value) AS key RETURN key, value[key] AS value;"
        )
        for _, row in df.iterrows():
            try:
                yield MetadataWorkUnit(
                    id=row["key"],
                    mcp=self.generate_neo4j_object(
                        columns=row["property_data_types"],
                        dataset=row["key"],
                    ),
                    is_primary_source=True,
                )

                yield MetadataWorkUnit(
                    id=row["key"],
                    mcp=MetadataChangeProposalWrapper(
                        entityUrn=make_dataset_urn(
                            platform=self.PLATFORM,
                            name=row["key"],
                            env=self.config.env,
                        ),
                        aspect=SubTypesClass(
                            typeNames=[
                                DatasetSubTypes.NEO4J_NODE
                                if row["obj_type"] == self.NODE
                                else DatasetSubTypes.NEO4J_RELATIONSHIP
                            ]
                        ),
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
