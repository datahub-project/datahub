import json
import logging
import typing
from typing import Dict, List, Optional, Tuple

from pyathena.common import BaseCursor
from pyathena.model import AthenaTableMetadata
from sqlalchemy.engine.reflection import Inspector

from datahub.emitter.mcp_builder import DatabaseKey, gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemyConfig,
    SQLAlchemySource,
    make_sqlalchemy_uri,
)


class AthenaConfig(SQLAlchemyConfig):
    scheme: str = "awsathena+rest"
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    aws_region: str
    s3_staging_dir: str
    work_group: str

    include_views = False  # not supported for Athena

    def get_sql_alchemy_url(self):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username or "",
            self.password,
            f"athena.{self.aws_region}.amazonaws.com:443",
            self.database,
            uri_opts={
                "s3_staging_dir": self.s3_staging_dir,
                "work_group": self.work_group,
            },
        )


class AthenaSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "athena")
        self.cursor: Optional[BaseCursor] = None

    @classmethod
    def create(cls, config_dict, ctx):
        config = AthenaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Optional[Dict[str, str]], Optional[str]]:
        if not self.cursor:
            self.cursor = inspector.dialect._raw_connection(inspector.engine).cursor()

        assert self.cursor
        # Unfortunately properties can be only get through private methods as those are not exposed
        # https://github.com/laughingman7743/PyAthena/blob/9e42752b0cc7145a87c3a743bb2634fe125adfa7/pyathena/model.py#L201
        metadata: AthenaTableMetadata = self.cursor._get_table_metadata(
            table_name=table, schema_name=schema
        )
        description = metadata.comment
        custom_properties: Dict[str, str] = {}
        custom_properties["partition_keys"] = json.dumps(
            [
                {
                    "name": partition.name,
                    "type": partition.type,
                    "comment": partition.comment if partition.comment else "",
                }
                for partition in metadata.partition_keys
            ]
        )
        for key, value in metadata.parameters.items():
            custom_properties[key] = value if value else ""

        custom_properties["create_time"] = (
            str(metadata.create_time) if metadata.create_time else ""
        )
        custom_properties["last_access_time"] = (
            str(metadata.last_access_time) if metadata.last_access_time else ""
        )
        custom_properties["table_type"] = (
            metadata.table_type if metadata.table_type else ""
        )

        location: Optional[str] = custom_properties.get("location", None)
        if location is not None:
            if location.startswith("s3://"):
                location = make_s3_urn(location, self.config.env)
            else:
                logging.debug(
                    f"Only s3 url supported for location. Skipping {location}"
                )
                location = None

        return description, custom_properties, location

    # It seems like database/schema filter in the connection string does not work and this to work around that
    def get_schema_names(self, inspector: Inspector) -> List[str]:
        athena_config = typing.cast(AthenaConfig, self.config)
        schemas = inspector.get_schema_names()
        if athena_config.database:
            return [schema for schema in schemas if schema == athena_config.database]
        return schemas

    def gen_database_containers(
        self, database: str
    ) -> typing.Iterable[MetadataWorkUnit]:
        # In Athena the schema is the database and database is not existing
        return []

    def gen_schema_key(self, db_name: str, schema: str) -> DatabaseKey:
        return DatabaseKey(
            database=schema,
            platform=self.platform,
            instance=self.config.platform_instance
            if self.config.platform_instance is not None
            else self.config.env,
        )

    def gen_schema_containers(
        self, schema: str, db_name: str
    ) -> typing.Iterable[MetadataWorkUnit]:
        database_container_key = self.gen_database_key(database=schema)

        container_workunits = gen_containers(
            database_container_key,
            schema,
            ["Database"],
        )

        for wu in container_workunits:
            self.report.report_workunit(wu)
            yield wu

    def close(self):
        if self.cursor:
            self.cursor.close()
