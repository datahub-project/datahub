from typing_extensions import Literal

from datahub.configuration.common import ConfigModel


class SqlFilter(ConfigModel):
    type: Literal["sql"]
    sql: str


DatasetFilter = SqlFilter
