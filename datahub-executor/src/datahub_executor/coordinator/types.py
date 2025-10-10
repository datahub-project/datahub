from typing import Dict, List, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator

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
    FreshnessFieldSpec,
    SchemaAssertion,
    SQLAssertion,
    VolumeAssertion,
)


class SchemaFieldSpecSchema(BaseModel):
    path: str
    type: str
    native_type: str = Field(alias="nativeType")
    kind: Optional[FreshnessFieldKind] = None


class FreshnessFieldSpecSchema(BaseModel):
    path: str
    type: str
    native_type: str = Field(alias="nativeType")
    kind: Optional[FreshnessFieldKind] = None


class FreshnessAssertionParametersSchema(BaseModel):
    source_type: DatasetFreshnessSourceType = Field(alias="sourceType")
    field: Optional[SchemaFieldSpecSchema] = None
    audit_log: Optional[AuditLogSpec] = Field(alias="auditLog", default=None)

    @field_validator("field")
    @classmethod
    def validate_field(
        cls, field: Optional[SchemaFieldSpecSchema], info: ValidationInfo
    ) -> Optional[SchemaFieldSpecSchema]:
        if (
            info.data.get("source_type") == DatasetFreshnessSourceType.FIELD_VALUE
            and field is None
        ):
            raise ValueError(
                f"field is required when sourceType is {DatasetFreshnessSourceType.FIELD_VALUE.value}"
            )
        return field

    def to_internal_params(self) -> DatasetFreshnessAssertionParameters:
        # FreshnessFieldSpec is expected but we have SchemaFieldSpecSchema
        # Convert SchemaFieldSpecSchema to FreshnessFieldSpec using model_validate
        freshness_field_spec: Optional[FreshnessFieldSpec] = (
            FreshnessFieldSpec.model_validate(self.field.model_dump())
            if self.field
            else None
        )

        return DatasetFreshnessAssertionParameters(
            source_type=self.source_type,
            field=freshness_field_spec,
            audit_log=self.audit_log,
        )


class VolumeAssertionParametersSchema(BaseModel):
    source_type: DatasetVolumeSourceType = Field(alias="sourceType")

    def to_internal_params(self) -> DatasetVolumeAssertionParameters:
        return DatasetVolumeAssertionParameters(source_type=self.source_type)


class FieldAssertionParametersSchema(BaseModel):
    source_type: DatasetFieldSourceType = Field(alias="sourceType")

    changed_rows_field: Optional[FreshnessFieldSpecSchema] = Field(
        alias="changedRowsField", default=None
    )

    def to_internal_params(self) -> DatasetFieldAssertionParameters:
        # FreshnessFieldSpec is expected but we have FreshnessFieldSpecSchema
        # Convert FreshnessFieldSpecSchema to FreshnessFieldSpec using model_validate
        changed_rows_field_spec: Optional[FreshnessFieldSpec] = (
            FreshnessFieldSpec.model_validate(self.changed_rows_field.model_dump())
            if self.changed_rows_field
            else None
        )

        return DatasetFieldAssertionParameters(
            source_type=self.source_type, changed_rows_field=changed_rows_field_spec
        )


class SchemaAssertionParametersSchema(BaseModel):
    source_type: DatasetSchemaSourceType = Field(alias="sourceType")

    def to_internal_params(self) -> DatasetSchemaAssertionParameters:
        return DatasetSchemaAssertionParameters(source_type=self.source_type)


class AssertionInfoSchema(BaseModel):
    freshness_assertion: Optional[FreshnessAssertion] = Field(
        alias="freshnessAssertion", default=None
    )
    volume_assertion: Optional[VolumeAssertion] = Field(
        alias="volumeAssertion", default=None
    )
    sql_assertion: Optional[SQLAssertion] = Field(alias="sqlAssertion", default=None)
    field_assertion: Optional[FieldAssertion] = Field(
        alias="fieldAssertion", default=None
    )
    schema_assertion: Optional[SchemaAssertion] = Field(
        alias="schemaAssertion", default=None
    )


class AssertionEvaluationParametersSchema(BaseModel):
    # Dataset FRESHNESS Parameters. Present if the type is DATASET_FRESHNESS
    dataset_freshness_parameters: Optional[FreshnessAssertionParametersSchema] = Field(
        alias="datasetFreshnessParameters", default=None
    )

    # Dataset VOLUME Parameters. Present if the type is DATASET_VOLUME
    dataset_volume_parameters: Optional[VolumeAssertionParametersSchema] = Field(
        alias="datasetVolumeParameters", default=None
    )

    # Dataset FIELD Parameters. Present if the type is DATASET_FIELD
    dataset_field_parameters: Optional[FieldAssertionParametersSchema] = Field(
        alias="datasetFieldParameters", default=None
    )

    # Dataset SCHEMA Parameters. Present if the type is DATASET_FIELD
    dataset_schema_parameters: Optional[SchemaAssertionParametersSchema] = Field(
        alias="datasetSchemaParameters", default=None
    )

    # The type of the parameters"""
    type: AssertionEvaluationParametersType

    @field_validator("type")
    @classmethod
    def validate_type(
        cls, type: AssertionEvaluationParametersType, info: ValidationInfo
    ) -> AssertionEvaluationParametersType:
        if (
            type == AssertionEvaluationParametersType.DATASET_FRESHNESS
            and info.data.get("dataset_freshness_parameters") is None
        ):
            raise ValueError(
                f"datasetFreshnessParameters is required when type is {AssertionEvaluationParametersType.DATASET_FRESHNESS.value}"
            )
        if (
            type == AssertionEvaluationParametersType.DATASET_VOLUME
            and info.data.get("dataset_volume_parameters") is None
        ):
            raise ValueError(
                f"datasetVolumeParameters is required when type is {AssertionEvaluationParametersType.DATASET_VOLUME.value}"
            )
        if (
            type == AssertionEvaluationParametersType.DATASET_FIELD
            and info.data.get("dataset_field_parameters") is None
        ):
            raise ValueError(
                f"datasetFieldParameters is required when type is {AssertionEvaluationParametersType.DATASET_FIELD.value}"
            )
        if (
            type == AssertionEvaluationParametersType.DATASET_SCHEMA
            and info.data.get("dataset_schema_parameters") is None
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

    @field_validator("dryRun", mode="before")
    @classmethod
    def validate_dry_run(cls, dry_run: Optional[bool]) -> bool:
        if dry_run is None:
            return True
        return dry_run


class EvaluateAssertionUrnsInputSchema(BaseModel):
    urns: List[str]
    dryRun: bool = True
    parameters: Optional[Dict] = None
    asyncFlag: bool = Field(alias="async", default=False)

    @field_validator("dryRun", mode="before")
    @classmethod
    def validate_dry_run(cls, dry_run: Optional[bool]) -> bool:
        if dry_run is None:
            return True
        return dry_run


class EvaluateAssertionUrnInputSchema(BaseModel):
    assertionUrn: str
    parameters: Optional[AssertionEvaluationParametersSchema] = None
    dryRun: bool = True

    @field_validator("dryRun", mode="before")
    @classmethod
    def validate_dry_run(cls, dry_run: Optional[bool]) -> bool:
        if dry_run is None:
            return True
        return dry_run


class TrainAssertionMonitorInputSchema(BaseModel):
    monitorUrn: str


class AssertionResultErrorSchema(BaseModel):
    type: str
    properties: Optional[Dict[str, str]] = None

    class Config:
        # orm_mode = True in pydantic v1, renamed to from_attributes in v2
        from_attributes = True


class AssertionResultSchema(BaseModel):
    type: str
    rowCount: Optional[int] = None
    missingCount: Optional[int] = None
    unexpectedCount: Optional[int] = None
    actualAggValue: Optional[float] = None
    nativeResults: Optional[Dict[str, str]] = None
    externalUrl: Optional[str] = None
    error: Optional[AssertionResultErrorSchema] = None

    class Config:
        # orm_mode = True in pydantic v1, renamed to from_attributes in v2
        from_attributes = True


class AssertionsResultItemSchema(BaseModel):
    urn: str
    result: AssertionResultSchema


class AssertionsResultSchema(BaseModel):
    results: List[AssertionsResultItemSchema]


class TrainAssertionMonitorResultSchema(BaseModel):
    success: bool
