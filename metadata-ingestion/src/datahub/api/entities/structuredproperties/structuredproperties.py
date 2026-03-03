import logging
from enum import Enum
from pathlib import Path
from typing import Iterable, List, Optional, Type, Union

import yaml
from pydantic import Field, StrictStr, field_validator, model_validator
from ruamel.yaml import YAML

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)
from datahub.metadata.urns import DataTypeUrn, StructuredPropertyUrn, Urn
from datahub.utilities.urns._urn_base import URN_TYPES

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
    value: Union[StrictStr, float]
    description: Optional[str] = None


VALID_ENTITY_TYPE_URNS = [
    Urn.make_entity_type_urn(entity_type) for entity_type in URN_TYPES
]
_VALID_ENTITY_TYPES_STRING = f"Valid entity type urns are {', '.join(VALID_ENTITY_TYPE_URNS)}, etc... Ensure that the entity type is valid."


def _validate_entity_type_urn(cls: Type, v: str) -> str:
    urn = Urn.make_entity_type_urn(v)
    if urn not in VALID_ENTITY_TYPE_URNS:
        raise ValueError(
            f"Input {v} is not a valid entity type urn. {_VALID_ENTITY_TYPES_STRING}"
        )
    v = str(urn)
    return v


class TypeQualifierAllowedTypes(ConfigModel):
    allowed_types: List[str]

    @field_validator("allowed_types", mode="before")
    @classmethod
    def _check_allowed_types(cls, v: Union[str, List[str]]) -> Union[str, List[str]]:
        if isinstance(v, list):
            return [_validate_entity_type_urn(cls, item) for item in v]
        return _validate_entity_type_urn(cls, v)


class StructuredProperties(ConfigModel):
    id: Optional[str] = None
    urn: Optional[str] = Field(None, validate_default=True)
    qualified_name: Optional[str] = None
    type: str
    version: Optional[str] = None
    value_entity_types: Optional[List[str]] = None
    description: Optional[str] = None
    display_name: Optional[str] = None
    entity_types: Optional[List[str]] = None
    cardinality: Optional[str] = None
    allowed_values: Optional[List[AllowedValue]] = None
    type_qualifier: Optional[TypeQualifierAllowedTypes] = None
    immutable: Optional[bool] = False

    @field_validator("version", mode="before")
    @classmethod
    def _coerce_version_to_str(cls, v: Union[str, int, None]) -> Optional[str]:
        if v is None:
            return None
        return str(v)

    @field_validator("entity_types", mode="before")
    @classmethod
    def _check_entity_types(cls, v: Union[str, List[str]]) -> Union[str, List[str]]:
        if isinstance(v, list):
            return [_validate_entity_type_urn(cls, item) for item in v]
        return _validate_entity_type_urn(cls, v)

    @field_validator("type", mode="after")
    @classmethod
    def validate_type(cls, v: str) -> str:
        # This logic is somewhat hacky, since we need to deal with
        # 1. fully qualified urns
        # 2. raw data types, that need to get the datahub namespace prefix
        # While keeping the user-facing interface and error messages clean.

        if not v.startswith("urn:li:") and not v.islower():
            # Convert to lowercase if needed
            v = v.lower()
            logger.warning(
                f"Structured property type should be lowercase. Updated to {v}"
            )

        urn = Urn.make_data_type_urn(v)

        # Check if type is allowed
        data_type_urn = DataTypeUrn.from_string(urn)
        unqualified_data_type = data_type_urn.id
        if unqualified_data_type.startswith("datahub."):
            unqualified_data_type = unqualified_data_type[len("datahub.") :]
        if not AllowedTypes.check_allowed_type(unqualified_data_type):
            raise ValueError(
                f"Type {unqualified_data_type} is not allowed. Allowed types are {AllowedTypes.values()}"
            )

        return urn

    @property
    def fqn(self) -> str:
        assert self.urn is not None
        id = StructuredPropertyUrn.from_string(self.urn).id
        if self.qualified_name is not None:
            # ensure that qualified name and ID match
            assert self.qualified_name == id, (
                "ID in the urn and the qualified_name must match"
            )
        return id

    @model_validator(mode="after")
    def urn_must_be_present(self) -> "StructuredProperties":
        if not self.urn:
            if not hasattr(self, "id") or not self.id:
                raise ValueError("id must be present if urn is not")
            self.urn = f"urn:li:structuredProperty:{self.id}"
        return self

    @staticmethod
    def from_yaml(file: str) -> List["StructuredProperties"]:
        with open(file) as fp:
            structuredproperties: List[dict] = yaml.safe_load(fp)

        result: List[StructuredProperties] = []
        for structuredproperty_raw in structuredproperties:
            result.append(StructuredProperties.model_validate(structuredproperty_raw))
        return result

    def generate_mcps(self) -> List[MetadataChangeProposalWrapper]:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.urn,
            aspect=StructuredPropertyDefinitionClass(
                qualifiedName=self.fqn,
                valueType=Urn.make_data_type_urn(self.type),
                displayName=self.display_name,
                description=self.description,
                entityTypes=[
                    Urn.make_entity_type_urn(entity_type)
                    for entity_type in self.entity_types or []
                ],
                cardinality=self.cardinality,
                immutable=self.immutable,
                allowedValues=(
                    [
                        PropertyValueClass(value=v.value, description=v.description)
                        for v in self.allowed_values
                    ]
                    if self.allowed_values
                    else None
                ),
                typeQualifier=(
                    {"allowedTypes": self.type_qualifier.allowed_types}
                    if self.type_qualifier
                    else None
                ),
                version=self.version,
            ),
        )
        return [mcp]

    @staticmethod
    def create(file: str, graph: DataHubGraph) -> None:
        # TODO: Deprecate this method.
        structuredproperties = StructuredProperties.from_yaml(file)
        for structuredproperty in structuredproperties:
            for mcp in structuredproperty.generate_mcps():
                graph.emit_mcp(mcp)

            logger.info(f"Created structured property {structuredproperty.urn}")

    @classmethod
    def from_datahub(cls, graph: DataHubGraph, urn: str) -> "StructuredProperties":
        structured_property: Optional[StructuredPropertyDefinitionClass] = (
            graph.get_aspect(urn, StructuredPropertyDefinitionClass)
        )
        if structured_property is None:
            raise Exception(
                "StructuredPropertyDefinition aspect is None. Unable to create structured property."
            )
        return StructuredProperties(
            urn=urn,
            qualified_name=structured_property.qualifiedName,
            display_name=structured_property.displayName,
            type=structured_property.valueType,
            version=structured_property.version,
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
            yaml.dump(self.model_dump(), fp)

    @staticmethod
    def list_urns(graph: DataHubGraph) -> Iterable[str]:
        return graph.get_urns_by_filter(
            entity_types=["structuredProperty"],
        )

    @staticmethod
    def list(graph: DataHubGraph) -> Iterable["StructuredProperties"]:
        for urn in StructuredProperties.list_urns(graph):
            yield StructuredProperties.from_datahub(graph, urn)
