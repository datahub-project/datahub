from typing import Optional

from pydantic import BaseModel, ConfigDict


# Lenient models: only the fields the client depends on are typed; the full Collibra
# payload is preserved via extra="allow" for the mapper. VERIFY exact nested shapes
# (type/relation/attribute objects) against a real env before relying on extras.
class CollibraEntity(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    name: Optional[str] = None


class ApplicationInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: Optional[str] = None
