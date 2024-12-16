from typing import Dict, List, Optional

from pydantic import BaseModel, Field, validator

from datahub_executor.common.types import (
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionType,
    AuditLogSpec,
    DatasetFieldAssertionParameters,
    DatasetFieldSourceType,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    DatasetSchemaAssertionParameters,
    DatasetSchemaSourceType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    FieldAssertion,
    FreshnessAssertion,
    FreshnessFieldKind,
    SchemaAssertion,
    SQLAssertion,
    VolumeAssertion,
)


class SchemaFieldSpecSchema(BaseModel):
    path: str
    type: str
    native_type: str = Field(alias="nativeType")
    kind: Optional[FreshnessFieldKind]


class FreshnessFieldSpecSchema(BaseModel):
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


class FieldAssertionParametersSchema(BaseModel):
    source_type: DatasetFieldSourceType = Field(alias="sourceType")

    changed_rows_field: Optional[FreshnessFieldSpecSchema] = Field(
        alias="changedRowsField"
    )

    def to_internal_params(self) -> DatasetFieldAssertionParameters:
        return DatasetFieldAssertionParameters(
            sourceType=self.source_type, changedRowsField=self.changed_rows_field
        )


class SchemaAssertionParametersSchema(BaseModel):
    source_type: DatasetSchemaSourceType = Field(alias="sourceType")

    def to_internal_params(self) -> DatasetSchemaAssertionParameters:
        return DatasetSchemaAssertionParameters(sourceType=self.source_type)


class AssertionInfoSchema(BaseModel):
    freshness_assertion: Optional[FreshnessAssertion] = Field(
        alias="freshnessAssertion"
    )
    volume_assertion: Optional[VolumeAssertion] = Field(alias="volumeAssertion")
    sql_assertion: Optional[SQLAssertion] = Field(alias="sqlAssertion")
    field_assertion: Optional[FieldAssertion] = Field(alias="fieldAssertion")
    schema_assertion: Optional[SchemaAssertion] = Field(alias="schemaAssertion")


class AssertionEvaluationParametersSchema(BaseModel):
    # Dataset FRESHNESS Parameters. Present if the type is DATASET_FRESHNESS
    dataset_freshness_parameters: Optional[FreshnessAssertionParametersSchema] = Field(
        alias="datasetFreshnessParameters"
    )

    # Dataset VOLUME Parameters. Present if the type is DATASET_VOLUME
    dataset_volume_parameters: Optional[VolumeAssertionParametersSchema] = Field(
        alias="datasetVolumeParameters"
    )

    # Dataset FIELD Parameters. Present if the type is DATASET_FIELD
    dataset_field_parameters: Optional[FieldAssertionParametersSchema] = Field(
        alias="datasetFieldParameters"
    )

    # Dataset SCHEMA Parameters. Present if the type is DATASET_FIELD
    dataset_schema_parameters: Optional[SchemaAssertionParametersSchema] = Field(
        alias="datasetSchemaParameters"
    )

    # The type of the parameters"""
    type: AssertionEvaluationParametersType

    @validator("type", always=True)
    def validate_type(
        cls, type: AssertionEvaluationParametersType, values: dict
    ) -> AssertionEvaluationParametersType:
        if (
            type == AssertionEvaluationParametersType.DATASET_FRESHNESS
            and values.get("dataset_freshness_parameters") is None
        ):
            raise ValueError(
                f"datasetFreshnessParameters is required when type is {AssertionEvaluationParametersType.DATASET_FRESHNESS.value}"
            )
        if (
            type == AssertionEvaluationParametersType.DATASET_VOLUME
            and values.get("dataset_volume_parameters") is None
        ):
            raise ValueError(
                f"datasetVolumeParameters is required when type is {AssertionEvaluationParametersType.DATASET_VOLUME.value}"
            )
        if (
            type == AssertionEvaluationParametersType.DATASET_FIELD
            and values.get("dataset_field_parameters") is None
        ):
            raise ValueError(
                f"datasetFieldParameters is required when type is {AssertionEvaluationParametersType.DATASET_FIELD.value}"
            )
        if (
            type == AssertionEvaluationParametersType.DATASET_SCHEMA
            and values.get("dataset_schema_parameters") is None
        ):
            raise ValueError(
                f"datasetSchemaParameters is required when type is {AssertionEvaluationParametersType.DATASET_SCHEMA.value}"
            )

        return type

    def to_internal_params(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=self.type,
            dataset_freshness_parameters=(
                self.dataset_freshness_parameters.to_internal_params()
                if self.dataset_freshness_parameters
                else None
            ),
            dataset_volume_parameters=(
                self.dataset_volume_parameters.to_internal_params()
                if self.dataset_volume_parameters
                else None
            ),
            dataset_field_parameters=(
                self.dataset_field_parameters.to_internal_params()
                if self.dataset_field_parameters
                else None
            ),
            dataset_schema_parameters=(
                self.dataset_schema_parameters.to_internal_params()
                if self.dataset_schema_parameters
                else None
            ),
        )


class EvaluateAssertionInputSchema(BaseModel):
    type: AssertionType
    connectionUrn: str
    entityUrn: str
    assertion: AssertionInfoSchema
    parameters: AssertionEvaluationParametersSchema
    dryRun: bool = True

    @validator("dryRun", pre=True, always=True)
    def validate_dry_run(cls, dry_run: Optional[bool]) -> bool:
        if dry_run is None:
            return True
        return dry_run


class EvaluateAssertionUrnsInputSchema(BaseModel):
    urns: List[str]
    dryRun: bool = True
    parameters: Optional[Dict]
    asyncFlag: bool = Field(alias="async", default=False)

    @validator("dryRun", pre=True, always=True)
    def validate_dry_run(cls, dry_run: Optional[bool]) -> bool:
        if dry_run is None:
            return True
        return dry_run


class EvaluateAssertionUrnInputSchema(BaseModel):
    assertionUrn: str
    parameters: Optional[AssertionEvaluationParametersSchema]
    dryRun: bool = True

    @validator("dryRun", pre=True, always=True)
    def validate_dry_run(cls, dry_run: Optional[bool]) -> bool:
        if dry_run is None:
            return True
        return dry_run


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


class AssertionsResultItemSchema(BaseModel):
    urn: str
    result: AssertionResultSchema


class AssertionsResultSchema(BaseModel):
    results: List[AssertionsResultItemSchema]
