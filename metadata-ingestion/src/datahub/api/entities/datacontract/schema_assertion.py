from __future__ import annotations

import json
from typing import List, Union

import pydantic
from typing_extensions import Literal

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.extractor.json_schema_util import get_schema_metadata
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionTypeClass,
    SchemaAssertionInfoClass,
    SchemaFieldClass,
    SchemalessClass,
    SchemaMetadataClass,
)


class JsonSchemaContract(ConfigModel):
    type: Literal["json-schema"]

    json_schema: dict = pydantic.Field(alias="json-schema")

    _schema_metadata: SchemaMetadataClass

    def _init_private_attributes(self) -> None:
        super()._init_private_attributes()
        self._schema_metadata = get_schema_metadata(
            platform="urn:li:dataPlatform:datahub",
            name="",
            json_schema=self.json_schema,
            raw_schema_string=json.dumps(self.json_schema),
        )


class FieldListSchemaContract(ConfigModel, arbitrary_types_allowed=True):
    type: Literal["field-list"]

    fields: List[SchemaFieldClass]

    _schema_metadata: SchemaMetadataClass

    def _init_private_attributes(self) -> None:
        super()._init_private_attributes()
        self._schema_metadata = SchemaMetadataClass(
            schemaName="",
            platform="urn:li:dataPlatform:datahub",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=self.fields,
        )


class SchemaAssertion(ConfigModel):
    __root__: Union[JsonSchemaContract, FieldListSchemaContract] = pydantic.Field(
        discriminator="type"
    )

    @property
    def id(self):
        return self.__root__.type

    def generate_mcp(
        self, assertion_urn: str, entity_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        schema_metadata = self.__root__._schema_metadata

        assertionInfo = AssertionInfoClass(
            type=AssertionTypeClass.DATA_SCHEMA,
            schemaAssertion=SchemaAssertionInfoClass(
                entity=entity_urn, schema=schema_metadata
            ),
        )

        return [
            MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=assertionInfo)
        ]
