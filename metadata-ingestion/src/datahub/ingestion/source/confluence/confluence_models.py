from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ConfluenceUser(BaseModel):
    """A user from the Confluence REST ``/rest/api/user`` endpoint."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    display_name: Optional[str] = Field(default=None, alias="displayName")
    # Cloud returns publicName for users whose display name is hidden.
    public_name: Optional[str] = Field(default=None, alias="publicName")

    @property
    def name(self) -> Optional[str]:
        return self.display_name or self.public_name
