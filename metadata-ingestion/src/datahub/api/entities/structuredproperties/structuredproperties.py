import logging
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Generator, List, Optional

import yaml
from pydantic import validator
from ruamel.yaml import YAML

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)
from datahub.utilities.urns.urn import Urn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StructuredPropertiesConfig:
    """Configuration class to hold the graph client"""

    _graph: Optional[DataHubGraph] = None

    @classmethod
    @contextmanager
    def use_graph(cls, graph: DataHubGraph) -> Generator[None, None, None]:
        """Context manager to temporarily set a custom graph"""
        previous_graph = cls._graph
        cls._graph = graph
        try:
            yield
        finally:
            cls._graph = previous_graph

    @classmethod
    def get_graph(cls) -> DataHubGraph:
        """Get the current graph, falling back to default if none set"""
        return cls._graph if cls._graph is not None else get_default_graph()


class AllowedTypes(Enum):
    STRING = "string"
    RICH_TEXT = "rich_text"
    NUMBER = "number"
    DATE = "date"
    URN = "urn"

    @staticmethod
    def check_allowed_type(value: str) -> bool:
        return value in [allowed_type.value for allowed_type in AllowedTypes]

    @staticmethod
    def values():
        return ", ".join([allowed_type.value for allowed_type in AllowedTypes])


class AllowedValue(ConfigModel):
    value: str
    description: Optional[str] = None


VALID_ENTITY_TYPES_PREFIX_STRING = ", ".join(
    [
        f"urn:li:entityType:datahub.{x}"
        for x in ["dataset", "dashboard", "dataFlow", "schemaField"]
    ]
)
VALID_ENTITY_TYPES_STRING = f"Valid entity type urns are {VALID_ENTITY_TYPES_PREFIX_STRING}, etc... Ensure that the entity type is valid."


class TypeQualifierAllowedTypes(ConfigModel):
    allowed_types: List[str]

    @validator("allowed_types", each_item=True)
    def validate_allowed_types(cls, v):
        if v:
            graph = StructuredPropertiesConfig.get_graph()
            validated_urn = Urn.make_entity_type_urn(v)
            if not graph.exists(validated_urn):
                raise ValueError(
                    f"Input {v} is not a valid entity type urn. {VALID_ENTITY_TYPES_STRING}"
                )
            v = str(validated_urn)
        return v


class StructuredProperties(ConfigModel):
    id: Optional[str] = None
    urn: Optional[str] = None
    qualified_name: Optional[str] = None
    type: str
    value_entity_types: Optional[List[str]] = None
    description: Optional[str] = None
    display_name: Optional[str] = None
    entity_types: Optional[List[str]] = None
    cardinality: Optional[str] = None
    allowed_values: Optional[List[AllowedValue]] = None
    type_qualifier: Optional[TypeQualifierAllowedTypes] = None
    immutable: Optional[bool] = False

    @validator("entity_types", each_item=True)
    def validate_entity_types(cls, v):
        if v:
            graph = StructuredPropertiesConfig.get_graph()
            validated_urn = Urn.make_entity_type_urn(v)
            if not graph.exists(validated_urn):
                raise ValueError(
                    f"Input {v} is not a valid entity type urn. {VALID_ENTITY_TYPES_STRING}"
                )
            v = str(validated_urn)
        return v

    @property
    def fqn(self) -> str:
        assert self.urn is not None
        return (
            self.qualified_name
            or self.id
            or Urn.create_from_string(self.urn).get_entity_id()[0]
        )

    @validator("urn", pre=True, always=True)
    def urn_must_be_present(cls, v, values):
        if not v:
            if "id" not in values:
                raise ValueError("id must be present if urn is not")
            return f"urn:li:structuredProperty:{values['id']}"
        return v

    @staticmethod
    def create(file: str, graph: Optional[DataHubGraph] = None) -> None:
        emitter: DataHubGraph = graph if graph else get_default_graph()
        with StructuredPropertiesConfig.use_graph(emitter):
            print("Using graph")
            with open(file) as fp:
                structuredproperties: List[dict] = yaml.safe_load(fp)
                for structuredproperty_raw in structuredproperties:
                    structuredproperty = StructuredProperties.parse_obj(
                        structuredproperty_raw
                    )
                    if not structuredproperty.type.islower():
                        structuredproperty.type = structuredproperty.type.lower()
                        logger.warn(
                            f"Structured property type should be lowercase. Updated to {structuredproperty.type}"
                        )
                    if not AllowedTypes.check_allowed_type(structuredproperty.type):
                        raise ValueError(
                            f"Type {structuredproperty.type} is not allowed. Allowed types are {AllowedTypes.values()}"
                        )
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=structuredproperty.urn,
                        aspect=StructuredPropertyDefinitionClass(
                            qualifiedName=structuredproperty.fqn,
                            valueType=Urn.make_data_type_urn(structuredproperty.type),
                            displayName=structuredproperty.display_name,
                            description=structuredproperty.description,
                            entityTypes=[
                                Urn.make_entity_type_urn(entity_type)
                                for entity_type in structuredproperty.entity_types or []
                            ],
                            cardinality=structuredproperty.cardinality,
                            immutable=structuredproperty.immutable,
                            allowedValues=(
                                [
                                    PropertyValueClass(
                                        value=v.value, description=v.description
                                    )
                                    for v in structuredproperty.allowed_values
                                ]
                                if structuredproperty.allowed_values
                                else None
                            ),
                            typeQualifier=(
                                {
                                    "allowedTypes": structuredproperty.type_qualifier.allowed_types
                                }
                                if structuredproperty.type_qualifier
                                else None
                            ),
                        ),
                    )
                    emitter.emit_mcp(mcp)

                    logger.info(f"Created structured property {structuredproperty.urn}")

    @classmethod
    def from_datahub(cls, graph: DataHubGraph, urn: str) -> "StructuredProperties":
        with StructuredPropertiesConfig.use_graph(graph):
            structured_property: Optional[
                StructuredPropertyDefinitionClass
            ] = graph.get_aspect(urn, StructuredPropertyDefinitionClass)
            if structured_property is None:
                raise Exception(
                    "StructuredPropertyDefinition aspect is None. Unable to create structured property."
                )
            return StructuredProperties(
                urn=urn,
                qualified_name=structured_property.qualifiedName,
                display_name=structured_property.displayName,
                type=structured_property.valueType,
                description=structured_property.description,
                entity_types=structured_property.entityTypes,
                cardinality=structured_property.cardinality,
                allowed_values=(
                    [
                        AllowedValue(
                            value=av.value,
                            description=av.description,
                        )
                        for av in structured_property.allowedValues or []
                    ]
                    if structured_property.allowedValues is not None
                    else None
                ),
                type_qualifier=(
                    {
                        "allowed_types": structured_property.typeQualifier.get(
                            "allowedTypes"
                        )
                    }
                    if structured_property.typeQualifier
                    else None
                ),
            )

    def to_yaml(
        self,
        file: Path,
    ) -> None:
        with open(file, "w") as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            yaml.indent(mapping=2, sequence=4, offset=2)
            yaml.default_flow_style = False
            yaml.dump(self.dict(), fp)
