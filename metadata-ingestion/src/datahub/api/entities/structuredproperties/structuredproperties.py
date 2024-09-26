import logging
from enum import Enum
from pathlib import Path
from typing import List, Optional

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


class TypeQualifierAllowedTypes(ConfigModel):
    allowed_types: List[str]

    @validator("allowed_types")
    def validate_allowed_types(cls, v):
        validated_entity_type_urns = []
        if v:
            with get_default_graph() as graph:
                for et in v:
                    validated_urn = Urn.make_entity_type_urn(et)
                    if graph.exists(validated_urn):
                        validated_entity_type_urns.append(validated_urn)
                    else:
                        logger.warn(
                            f"Input {et} is not a valid entity type urn. Skipping."
                        )
        v = validated_entity_type_urns
        if not v:
            logger.warn("No allowed_types given within type_qualifier.")
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
            assert "id" in values, "id must be present if urn is not"
            return f"urn:li:structuredProperty:{values['id']}"
        return v

    @staticmethod
    def create(file: str) -> None:
        emitter: DataHubGraph

        with get_default_graph() as emitter:
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

        structured_property: Optional[
            StructuredPropertyDefinitionClass
        ] = graph.get_aspect(urn, StructuredPropertyDefinitionClass)
        assert structured_property is not None
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
                {"allowed_types": structured_property.typeQualifier.get("allowedTypes")}
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
