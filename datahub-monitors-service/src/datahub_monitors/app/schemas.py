from typing import Dict, Optional

from pydantic import BaseModel, Field, validator

from datahub_monitors.types import (
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionType,
    AuditLogSpec,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    FreshnessAssertion,
    FreshnessFieldKind,
    SQLAssertion,
    VolumeAssertion,
)


class SchemaFieldSpecSchema(BaseModel):
    path: str
    type: str
    native_type: str = Field(alias="nativeType")
    kind: Optional[FreshnessFieldKind]


class FreshnessAssertionParametersSchema(BaseModel):
    source_type: DatasetFreshnessSourceType = Field(alias="sourceType")
    field: Optional[SchemaFieldSpecSchema] = None
    audit_log: Optional[AuditLogSpec] = Field(alias="auditLog")

    @validator("field", always=True)
    def validate_field(
        cls, field: Optional[SchemaFieldSpecSchema], values: dict
    ) -> Optional[SchemaFieldSpecSchema]:
        if (
            values.get("source_type") == DatasetFreshnessSourceType.FIELD_VALUE
            and field is None
        ):
            raise ValueError(
                f"field is required when sourceType is {DatasetFreshnessSourceType.FIELD_VALUE.value}"
            )
        return field

    def to_internal_params(self) -> DatasetFreshnessAssertionParameters:
        return DatasetFreshnessAssertionParameters(
            sourceType=self.source_type,
            field=self.field,
            auditLog=self.audit_log,
        )


class VolumeAssertionParametersSchema(BaseModel):
    source_type: DatasetVolumeSourceType = Field(alias="sourceType")

    def to_internal_params(self) -> DatasetVolumeAssertionParameters:
        return DatasetVolumeAssertionParameters(sourceType=self.source_type)


class AssertionInfoSchema(BaseModel):
    freshness_assertion: Optional[FreshnessAssertion] = Field(
        alias="freshnessAssertion"
    )
    volume_assertion: Optional[VolumeAssertion] = Field(alias="volumeAssertion")
    sql_assertion: Optional[SQLAssertion] = Field(alias="sqlAssertion")


class AssertionEvaluationParametersSchema(BaseModel):
    # The type of the parameters"""
    type: AssertionEvaluationParametersType

    # Dataset FRESHNESS Parameters. Present if the type is DATASET_FRESHNESS
    dataset_freshness_parameters: Optional[FreshnessAssertionParametersSchema] = Field(
        alias="datasetFreshnessParameters"
    )

    # Dataset VOLUME Parameters. Present if the type is DATASET_VOLUME
    dataset_volume_parameters: Optional[VolumeAssertionParametersSchema] = Field(
        alias="datasetVolumeParameters"
    )

    def to_internal_params(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=self.type,
            dataset_freshness_parameters=self.dataset_freshness_parameters.to_internal_params()
            if self.dataset_freshness_parameters
            else None,
            dataset_volume_parameters=self.dataset_volume_parameters.to_internal_params()
            if self.dataset_volume_parameters
            else None,
        )


class EvaluateAssertionInputSchema(BaseModel):
    type: AssertionType
    connectionUrn: str
    entityUrn: str
    assertion: AssertionInfoSchema
    parameters: AssertionEvaluationParametersSchema
    dryRun: bool = True


class EvaluateAssertionUrnInputSchema(BaseModel):
    assertionUrn: str
    parameters: AssertionEvaluationParametersSchema
    dryRun: bool = True


class AssertionResultErrorSchema(BaseModel):
    type: str
    properties: Optional[Dict[str, str]]

    class Config:
        orm_mode = True


class AssertionResultSchema(BaseModel):
    type: str
    rowCount: Optional[int]
    missingCount: Optional[int]
    unexpectedCount: Optional[int]
    actualAggValue: Optional[float]
    nativeResults: Optional[Dict[str, str]]
    externalUrl: Optional[str]
    error: Optional[AssertionResultErrorSchema]

    class Config:
        orm_mode = True
