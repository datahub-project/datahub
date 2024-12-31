from typing import List, Optional, Union

from typing_extensions import Literal

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datacontract.assertion import BaseAssertion
from datahub.api.entities.datacontract.assertion_operator import Operators
from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel, v1_Field
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    AssertionValueChangeTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    SqlAssertionInfoClass,
    SqlAssertionTypeClass,
)


class IdConfigMixin(BaseAssertion):
    id_raw: Optional[str] = v1_Field(
        default=None,
        alias="id",
        description="The id of the assertion. If not provided, one will be generated using the type.",
    )

    def generate_default_id(self) -> str:
        raise NotImplementedError


class CustomSQLAssertion(IdConfigMixin, BaseAssertion):
    type: Literal["custom_sql"]
    sql: str
    operator: Operators = v1_Field(discriminator="type")

    def generate_default_id(self) -> str:
        return f"{self.type}-{self.sql}-{self.operator.id()}"

    def generate_assertion_info(self, entity_urn: str) -> AssertionInfoClass:
        sql_assertion_info = SqlAssertionInfoClass(
            entity=entity_urn,
            statement=self.sql,
            operator=self.operator.operator,
            parameters=self.operator.generate_parameters(),
            # TODO: Support other types of assertions
            type=SqlAssertionTypeClass.METRIC,
            changeType=AssertionValueChangeTypeClass.ABSOLUTE,
        )
        return AssertionInfoClass(
            type=AssertionTypeClass.SQL,
            sqlAssertion=sql_assertion_info,
            description=self.description,
        )


class ColumnUniqueAssertion(IdConfigMixin, BaseAssertion):
    type: Literal["unique"]

    # TODO: support multiple columns?
    column: str

    def generate_default_id(self) -> str:
        return f"{self.type}-{self.column}"

    def generate_assertion_info(self, entity_urn: str) -> AssertionInfoClass:
        dataset_assertion_info = DatasetAssertionInfoClass(
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
        return AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=dataset_assertion_info,
            description=self.description,
        )


class DataQualityAssertion(v1_ConfigModel):
    __root__: Union[
        CustomSQLAssertion,
        ColumnUniqueAssertion,
    ] = v1_Field(discriminator="type")

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
        return [
            MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=self.__root__.generate_assertion_info(entity_urn),
            )
        ]
