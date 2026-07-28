from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from datahub.metadata.schema_classes import SchemaFieldClass


class UnknownColumnType(BaseModel):
    """A column whose source type literal (CDS or EDM) is not in a parser's type
    map. Emitted by the SAP parsers so a source can report it uniformly."""

    model_config = ConfigDict(frozen=True)

    type: str
    column: str


class EdmxParseResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    fields: List[SchemaFieldClass]
    field_custom_props: Dict[str, Dict[str, str]]  # fieldPath → {key: value}
    entity_label: Optional[str]
    entity_custom_props: Dict[str, str]
    error: Optional[str] = None  # set when parse failed; None on success
    unknown_edm_types: List[UnknownColumnType] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_failure_carries_no_payload(self) -> "EdmxParseResult":
        # A failure result must not smuggle partial schema through: callers branch
        # on ``error`` and skip the payload, so any fields/props set alongside an
        # error would be silently dropped and mask a parser bug.
        if self.error is not None and (
            self.fields
            or self.field_custom_props
            or self.entity_label is not None
            or self.entity_custom_props
            or self.unknown_edm_types
        ):
            raise ValueError(
                "EdmxParseResult with an error must carry no fields/props "
                f"(error={self.error!r})"
            )
        return self
