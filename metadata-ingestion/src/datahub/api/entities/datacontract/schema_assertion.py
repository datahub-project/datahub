from __future__ import annotations

import json
from typing import List, Union

from typing_extensions import Literal

from datahub.api.entities.datacontract.assertion import BaseAssertion
from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel, v1_Field
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


class JsonSchemaContract(BaseAssertion):
    type: Literal["json-schema"]

    json_schema: dict = v1_Field(alias="json-schema")

    _schema_metadata: SchemaMetadataClass

    def _init_private_attributes(self) -> None:
        super()._init_private_attributes()
        self._schema_metadata = get_schema_metadata(
            platform="urn:li:dataPlatform:datahub",
            name="",
            json_schema=self.json_schema,
            raw_schema_string=json.dumps(self.json_schema),
        )


class FieldListSchemaContract(BaseAssertion):
    class Config:
        arbitrary_types_allowed = True

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


class SchemaAssertion(v1_ConfigModel):
    __root__: Union[JsonSchemaContract, FieldListSchemaContract] = v1_Field(
        discriminator="type"
    )

    @property
    def id(self):
        return self.__root__.type

    def generate_mcp(
        self, assertion_urn: str, entity_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        aspect = AssertionInfoClass(
            type=AssertionTypeClass.DATA_SCHEMA,
            schemaAssertion=SchemaAssertionInfoClass(
                entity=entity_urn,
                schema=self.__root__._schema_metadata,
            ),
            description=self.__root__.description,
        )

        return [MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=aspect)]
