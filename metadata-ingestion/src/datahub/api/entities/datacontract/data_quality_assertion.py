from typing import List, Optional, Union

import pydantic
from typing_extensions import Literal

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
)


class IdConfigMixin(ConfigModel):
    id_raw: Optional[str] = pydantic.Field(
        default=None,
        alias="id",
        description="The id of the assertion. If not provided, one will be generated using the type.",
    )

    def generate_default_id(self) -> str:
        raise NotImplementedError


class CustomSQLAssertion(IdConfigMixin, ConfigModel):
    type: Literal["custom_sql"]

    sql: str

    def generate_dataset_assertion_info(
        self, entity_urn: str
    ) -> DatasetAssertionInfoClass:
        return DatasetAssertionInfoClass(
            dataset=entity_urn,
            scope=DatasetAssertionScopeClass.UNKNOWN,
            fields=[],
            operator=AssertionStdOperatorClass._NATIVE_,
            aggregation=AssertionStdAggregationClass._NATIVE_,
            logic=self.sql,
        )


class ColumnUniqueAssertion(IdConfigMixin, ConfigModel):
    type: Literal["unique"]

    # TODO: support multiple columns?
    column: str

    def generate_default_id(self) -> str:
        return f"{self.type}-{self.column}"

    def generate_dataset_assertion_info(
        self, entity_urn: str
    ) -> DatasetAssertionInfoClass:
        return DatasetAssertionInfoClass(
            dataset=entity_urn,
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            fields=[builder.make_schema_field_urn(entity_urn, self.column)],
            operator=AssertionStdOperatorClass.EQUAL_TO,
            aggregation=AssertionStdAggregationClass.UNIQUE_PROPOTION,  # purposely using the misspelled version to work with gql
            parameters=AssertionStdParametersClass(
                value=AssertionStdParameterClass(
                    value="1", type=AssertionStdParameterTypeClass.NUMBER
                )
            ),
        )


class DataQualityAssertion(ConfigModel):
    __root__: Union[
        CustomSQLAssertion,
        ColumnUniqueAssertion,
    ] = pydantic.Field(discriminator="type")

    @property
    def id(self) -> str:
        if self.__root__.id_raw:
            return self.__root__.id_raw
        try:
            return self.__root__.generate_default_id()
        except NotImplementedError:
            return self.__root__.type

    def generate_mcp(
        self, assertion_urn: str, entity_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        dataset_assertion_info = self.__root__.generate_dataset_assertion_info(
            entity_urn
        )

        return [
            MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=AssertionInfoClass(
                    type=AssertionTypeClass.DATASET,
                    datasetAssertion=dataset_assertion_info,
                ),
            )
        ]
