from typing import Optional

# This import verifies that the dependencies are available.
import cx_Oracle  # noqa: F401
import pydantic

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class OracleConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "oracle+cx_oracle"

    service_name: Optional[str]

    @pydantic.validator("service_name")
    def check_service_name(cls, v, values):
        if values.get("database") and v:
            raise ValueError(
                "specify one of 'database' and 'service_name', but not both"
            )
        return v

    def get_sql_alchemy_url(self):
        url = super().get_sql_alchemy_url()
        if self.service_name:
            assert not self.database
            url = f"{url}/?service_name={self.service_name}"
        return url


class OracleSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "oracle")

    @classmethod
    def create(cls, config_dict, ctx):
        config = OracleConfig.parse_obj(config_dict)
        return cls(config, ctx)
