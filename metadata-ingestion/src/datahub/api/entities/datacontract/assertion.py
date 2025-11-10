from typing import Optional

from pydantic import BaseModel


class BaseAssertion(BaseModel):
    model_config = {"extra": "forbid"}

    description: Optional[str] = None
