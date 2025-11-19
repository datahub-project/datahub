from __future__ import annotations

import json
from typing import List, Union

from pydantic import ConfigDict, Field, RootModel
from typing_extensions import Literal

from datahub.api.entities.datacontract.assertion import BaseAssertion
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

    json_schema: dict = Field(alias="json-schema")

    _schema_metadata: SchemaMetadataClass

    def model_post_init(self, __context: object) -> None:
        self._schema_metadata = get_schema_metadata(
            platform="urn:li:dataPlatform:datahub",
            name="",
            json_schema=self.json_schema,
            raw_schema_string=json.dumps(self.json_schema),
        )


class FieldListSchemaContract(BaseAssertion):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: Literal["field-list"]

    fields: List[SchemaFieldClass]

    _schema_metadata: SchemaMetadataClass

    def model_post_init(self, __context: object) -> None:
        self._schema_metadata = SchemaMetadataClass(
            schemaName="",
            platform="urn:li:dataPlatform:datahub",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=self.fields,
        )


class SchemaAssertion(RootModel[Union[JsonSchemaContract, FieldListSchemaContract]]):
    root: Union[JsonSchemaContract, FieldListSchemaContract] = Field(
        discriminator="type"
    )

    @property
    def id(self):
        return self.root.type

    def generate_mcp(
        self, assertion_urn: str, entity_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        aspect = AssertionInfoClass(
            type=AssertionTypeClass.DATA_SCHEMA,
            schemaAssertion=SchemaAssertionInfoClass(
                entity=entity_urn,
                schema=self.root._schema_metadata,
            ),
            description=self.root.description,
        )

        return [MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=aspect)]
