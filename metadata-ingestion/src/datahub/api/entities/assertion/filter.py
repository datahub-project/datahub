from pydantic import BaseModel
from typing_extensions import Literal


class SqlFilter(BaseModel):
    model_config = {"extra": "forbid"}

    type: Literal["sql"]
    sql: str


DatasetFilter = SqlFilter
