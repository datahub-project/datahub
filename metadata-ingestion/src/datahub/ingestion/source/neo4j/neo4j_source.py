import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from neo4j import GraphDatabase
from pydantic import Field

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.sdk.dataset import Dataset

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Neo4j object types
_NODE = "node"
_RELATIONSHIP = "relationship"


class Neo4jConfig(
    StatefulIngestionConfigBase, EnvConfigMixin, PlatformInstanceConfigMixin
):
    username: str = Field(description="Neo4j Username")
    password: str = Field(description="Neo4j Password")
    uri: str = Field(description="The URI for the Neo4j server")

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class Neo4jSourceReport(StatefulIngestionReport):
    obj_failures: int = 0
    obj_created: int = 0


@platform_name("Neo4j", id="neo4j")
@config_class(Neo4jConfig)
@capability(
    SourceCapability.PLATFORM_INSTANCE, "Supported via the `platform_instance` config"
)
@support_status(SupportStatus.CERTIFIED)
class Neo4jSource(StatefulIngestionSourceBase):
    config: Neo4jConfig
    report: Neo4jSourceReport

    def __init__(self, config: Neo4jConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.platform = "neo4j"
        self.report: Neo4jSourceReport = Neo4jSourceReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "Neo4jSource":
        config = Neo4jConfig.model_validate(config_dict)
        return cls(config, ctx)

    def create_schema_field_tuple(
        self, col_name: str, col_type: str, obj_type: Optional[str]
    ) -> Tuple[str, str, str]:
        """Convert Neo4j property to (field_name, field_type, description) tuple."""
        # Special case: when a node has a relationship-typed property, treat it as a node reference
        # This ensures relationship properties within nodes are described as "NODE" rather than "RELATIONSHIP"
        column_type = (
            _NODE if obj_type == _NODE and col_type == _RELATIONSHIP else col_type
        )

        description = (
            column_type.upper()
            if column_type in (_NODE, _RELATIONSHIP)
            else column_type
        )

        return (col_name, column_type, description)

    def get_subtype_from_obj_type(self, obj_type: str) -> str:
        """Map Neo4j object type to DataHub subtype."""
        if obj_type == _NODE:
            return DatasetSubTypes.NEO4J_NODE
        elif obj_type == _RELATIONSHIP:
            return DatasetSubTypes.NEO4J_RELATIONSHIP
        return DatasetSubTypes.NEO4J_NODE  # default fallback

    def create_neo4j_dataset(
        self,
        dataset: str,
        columns: list,
        obj_type: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[Dataset]:
        """Create Dataset entity with Neo4j schema and metadata."""
        try:
            schema_fields = [
                self.create_schema_field_tuple(
                    col_name=key, col_type=value.lower(), obj_type=obj_type
                )
                for d in columns
                for key, value in d.items()
            ]

            return Dataset(
                platform=self.platform,
                name=dataset,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                schema=schema_fields,
                subtype=self.get_subtype_from_obj_type(obj_type or _NODE),
                description=description,
            )

        except Exception as e:
            log.error(e)
            self.report.report_failure(
                message="Failed to process dataset",
                context=dataset,
                exc=e,
            )
            return None

    def get_neo4j_metadata(self, query: str) -> Optional[pd.DataFrame]:
        driver = GraphDatabase.driver(
            self.config.uri, auth=(self.config.username, self.config.password)
        )
        """
        This process retrieves the metadata for Neo4j objects using an APOC query,
        which returns a dictionary with two columns: key and value. The key represents
        the Neo4j object, while the value contains the corresponding metadata.

        When data is returned from Neo4j, much of the relationship metadata is stored
        with the relevant node's metadata. Consequently, the objects are organized
        into two separate dataframes: one for nodes and one for relationships.

        In the node dataframe, several fields are extracted and added as new columns.
        Similarly, in the relationship dataframe, certain fields are parsed out,
        while others require metadata from the nodes dataframe.

        Once the data is parsed and these two dataframes are created, we combine
        a subset of their columns into a single dataframe, which will be used to
        create the DataHub objects.

        See the docs for examples of metadata:
        metadata-ingestion/docs/sources/neo4j/neo4j.md
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
            return df
        except Exception as e:
            self.report.failure(
                message="Failed to get neo4j metadata",
                exc=e,
            )

        return None

    def process_nodes(self, data: list) -> pd.DataFrame:
        nodes = [record for record in data if record["value"]["type"] == _NODE]
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
        rels = [record for record in data if record["value"]["type"] == _RELATIONSHIP]
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
        return record.get("relationships", {})

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        query = (
            "CALL apoc.meta.schema() YIELD value UNWIND keys(value) AS key "
            "RETURN key, value[key] AS value;"
        )
        df = self.get_neo4j_metadata(query)
        if df is None:
            log.warning("No metadata retrieved from Neo4j")
            return

        for _, row in df.iterrows():
            try:
                dataset_obj = self.create_neo4j_dataset(
                    dataset=row["key"],
                    columns=row["property_data_types"],
                    obj_type=row["obj_type"],
                    description=row["description"],
                )

                if dataset_obj:
                    yield from dataset_obj.as_workunits()
                    self.report.obj_created += 1
                else:
                    log.warning(f"Failed to create dataset object for {row['key']}")
                    self.report.obj_failures += 1

            except Exception as e:
                log.warning(f"Failed to process row {row['key']}: {str(e)}")
                self.report.report_warning(
                    title="Error processing Neo4j metadata",
                    message="Some entities will be missed",
                    context=row["key"],
                    exc=e,
                )
                self.report.obj_failures += 1

    def get_report(self) -> "Neo4jSourceReport":
        return self.report
