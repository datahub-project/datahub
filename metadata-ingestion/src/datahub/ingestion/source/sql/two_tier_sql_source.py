import typing
from typing import Any, Dict

import pydantic
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp_builder import PlatformKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    logger,
    make_sqlalchemy_uri,
)


class TwoTierSQLAlchemyConfig(BasicSQLAlchemyConfig):

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion.",
    )
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Deprecated in favour of database_pattern. Regex patterns for schemas to filter in ingestion. "
        "Specify regex to only match the schema name. e.g. to match all tables in schema analytics, "
        "use the regex 'analytics'",
    )

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        allow_all_pattern = AllowDenyPattern.allow_all()
        schema_pattern = values.get("schema_pattern")
        database_pattern = values.get("database_pattern")
        if (
            database_pattern == allow_all_pattern
            and schema_pattern != allow_all_pattern
        ):
            logger.warning(
                "Updating 'database_pattern' to 'schema_pattern'. Please stop using deprecated "
                "'schema_pattern'. Use 'database_pattern' instead. "
            )
            values["database_pattern"] = schema_pattern
        return values

    def get_sql_alchemy_url(
        self,
        uri_opts: typing.Optional[typing.Dict[str, typing.Any]] = None,
        current_db: typing.Optional[str] = None,
    ) -> str:
        return self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,  # type: ignore
            self.username,
            self.password.get_secret_value() if self.password else None,
            self.host_port,  # type: ignore
            current_db if current_db else self.database,
            uri_opts=uri_opts,
        )


class TwoTierSQLAlchemySource(SQLAlchemySource):
    def __init__(self, config, ctx, platform):
        super().__init__(config, ctx, platform)
        self.current_database = None
        self.config: TwoTierSQLAlchemyConfig = config

    def get_parent_container_key(self, db_name: str, schema: str) -> PlatformKey:
        return self.gen_database_key(db_name)

    def get_allowed_schemas(
        self, inspector: Inspector, db_name: str
    ) -> typing.Iterable[str]:
        # This method returns schema names but for 2 tier databases there is no schema layer at all hence passing
        # dbName itself as an allowed schema
        yield db_name

    def get_inspectors(self):
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            if self.config.database and self.config.database != "":
                databases = [self.config.database]
            else:
                databases = inspector.get_schema_names()
            for db in databases:
                if self.config.database_pattern.allowed(db):
                    url = self.config.get_sql_alchemy_url(current_db=db)
                    inspector = inspect(
                        create_engine(url, **self.config.options).connect()
                    )
                    self.current_database = db
                    yield inspector

    def gen_schema_containers(
        self, schema: str, db_name: str
    ) -> typing.Iterable[MetadataWorkUnit]:
        return []
