from enum import Enum

from pydantic import BaseModel, Field


class SourceDetails(BaseModel):
    origin: str | None = Field(
        default=None,
        description="Origin entity for the documentation. This is the entity that triggered the documentation propagation.",
    )
    via: str | None = Field(
        default=None,
        description="Via entity for the documentation. This is the direct entity that the documentation was propagated through.",
    )
    propagated: bool | None = Field(
        default=None,
        description="Indicates whether the metadata element was propagated.",
    )
    actor: str | None = Field(
        default=None,
        description="Actor that triggered the metadata propagation.",
    )
    propagation_started_at: int | None = Field(
        default=None,
        description="Timestamp when the metadata propagation event happened.",
    )
    propagation_depth: int | None = Field(
        default=0,
        description="Depth of metadata propagation.",
    )

    def for_metadata_attribution(self) -> dict[str, str]:
        """
        Convert the SourceDetails object to a dictionary that can be used in
        Metadata Attribution MCPs.
        """
        result = {}
        for k, v in self.model_dump(exclude_none=True).items():
            if isinstance(v, Enum):
                result[k] = v.value  # Use the enum's value
            elif isinstance(v, bool):
                result[k] = str(v).lower()
            else:
                result[k] = str(v)  # Convert everything else to string
        return result
