"""Shared ownership configuration for Fabric connectors."""

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import validate_ownership_type


class FabricOwnershipConfigMixin(ConfigModel):
    """DataHub ownership type for a Fabric item's default identity."""

    ownership_type: str = Field(
        default="TECHNICAL_OWNER",
        description=(
            "DataHub ownership type applied to the item default identity. "
            "One of TECHNICAL_OWNER, BUSINESS_OWNER, DATA_STEWARD, NONE, "
            "or a custom ownership type URN (`urn:li:ownershipType:<id>`)."
        ),
    )
    cascade_ownership: bool = Field(
        default=True,
        description=(
            "When a lower-level entity has no owner of its own, copy the "
            "parent item's owner onto it. OneLake copies a lakehouse or "
            "warehouse owner onto its schemas, tables, and views. Data "
            "Factory copies a pipeline owner onto its activities."
        ),
    )

    @field_validator("ownership_type")
    @classmethod
    def _validate_ownership_type(cls, value: str) -> str:
        ownership_type, ownership_type_urn = validate_ownership_type(value)
        if ownership_type_urn:
            return ownership_type_urn
        return ownership_type
