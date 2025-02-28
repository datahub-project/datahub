from typing_extensions import Literal

from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel


class SqlFilter(v1_ConfigModel):
    type: Literal["sql"]
    sql: str


DatasetFilter = SqlFilter
# class DatasetFilter(v1_ConfigModel):
#    __root__: Union[SqlFilter] = v1_Field(discriminator="type")
