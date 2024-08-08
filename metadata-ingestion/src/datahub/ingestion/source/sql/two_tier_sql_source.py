import typing
import urllib.parse
from typing import Any, Dict, Iterable, Optional, Tuple

from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource, logger
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    make_sqlalchemy_uri,
)
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_key,
)


class TwoTierSQLAlchemyConfig(BasicSQLAlchemyConfig):
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion.",
    )
    schema_pattern: AllowDenyPattern = Field(
        # The superclass contains a `schema_pattern` field, so we need this here
        # to override the documentation.
        default=AllowDenyPattern.allow_all(),
        hidden_from_docs=True,
        description="Deprecated in favour of database_pattern.",
    )

    _schema_pattern_deprecated = pydantic_renamed_field(
        "schema_pattern", "database_pattern"
    )

    def get_sql_alchemy_url(
        self,
        uri_opts: typing.Optional[typing.Dict[str, typing.Any]] = None,
        current_db: typing.Optional[str] = None,
    ) -> str:
        if self.sqlalchemy_uri:
            parsed_url = urllib.parse.urlsplit(self.sqlalchemy_uri)
            url = URL.create(
                drivername=parsed_url.scheme,
                username=parsed_url.username,
                password=parsed_url.password,
                host=parsed_url.hostname,
                port=parsed_url.port,
                database=current_db or parsed_url.path.lstrip("/"),
                query=urllib.parse.parse_qs(parsed_url.query),
            ).update_query_dict(uri_opts or {})
            return str(url)
        else:
            return make_sqlalchemy_uri(
                self.scheme,
                self.username,
                self.password.get_secret_value() if self.password else None,
                self.host_port,
                current_db or self.database,
                uri_opts=uri_opts,
            )


class TwoTierSQLAlchemySource(SQLAlchemySource):
    def __init__(self, config, ctx, platform):
        super().__init__(config, ctx, platform)
        self.config: TwoTierSQLAlchemyConfig = config

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        schema, _view = dataset_identifier.split(".", 1)
        return None, schema

    def get_database_container_key(self, db_name: str, schema: str) -> ContainerKey:
        # Because our overridden get_allowed_schemas method returns db_name as the schema name,
        # the db_name and schema here will be the same. Hence, we just ignore the schema parameter.
        assert db_name == schema
        return gen_database_key(
            db_name,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def add_table_to_schema_container(
        self,
        dataset_urn: str,
        db_name: str,
        schema: str,
        schema_container_key: Optional[ContainerKey] = None,
    ) -> Iterable[MetadataWorkUnit]:
        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=self.get_database_container_key(db_name, schema),
        )

    def get_allowed_schemas(
        self, inspector: Inspector, db_name: str
    ) -> typing.Iterable[str]:
        # This method returns schema names but for 2 tier databases there is no schema layer at all hence passing
        # dbName itself as an allowed schema
        yield db_name

    def gen_schema_key(self, db_name: str, schema: str) -> ContainerKey:
        # Sanity check that we don't try to generate schema containers for 2 tier databases.
        raise NotImplementedError

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
                    with create_engine(url, **self.config.options).connect() as conn:
                        inspector = inspect(conn)
                        yield inspector

    def gen_schema_containers(
        self,
        schema: str,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        return []

    def get_db_name(self, inspector: Inspector) -> str:
        engine = inspector.engine

        if engine and hasattr(engine, "url") and hasattr(engine.url, "database"):
            return str(engine.url.database).strip('"')
        else:
            raise Exception("Unable to get database name from Sqlalchemy inspector")
