import json
import logging
import typing
from typing import Any, Dict, Iterable, List, Optional, Tuple, cast

import pydantic
from pyathena.common import BaseCursor
from pyathena.model import AthenaTableMetadata
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mcp_builder import ContainerKey, DatabaseKey
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig, make_sqlalchemy_uri
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_database_key,
)


class AthenaConfig(SQLCommonConfig):
    scheme: str = "awsathena+rest"
    username: Optional[str] = pydantic.Field(
        default=None,
        description="Username credential. If not specified, detected with boto3 rules. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html",
    )
    password: Optional[pydantic.SecretStr] = pydantic.Field(
        default=None, description="Same detection scheme as username"
    )
    database: Optional[str] = pydantic.Field(
        default=None,
        description="The athena database to ingest from. If not set it will be autodetected",
    )
    aws_region: str = pydantic.Field(
        description="Aws region where your Athena database is located"
    )
    aws_role_arn: Optional[str] = pydantic.Field(
        default=None,
        description="AWS Role arn for Pyathena to assume in its connection",
    )
    aws_role_assumption_duration: int = pydantic.Field(
        default=3600,
        description="Duration to assume the AWS Role for. Maximum of 43200 (12 hours)",
    )
    s3_staging_dir: Optional[str] = pydantic.Field(
        default=None,
        deprecated=True,
        description="[deprecated in favor of `query_result_location`] S3 query location",
    )
    work_group: str = pydantic.Field(
        description="The name of your Amazon Athena Workgroups"
    )
    catalog_name: str = pydantic.Field(
        default="awsdatacatalog",
        description="Athena Catalog Name",
    )

    query_result_location: str = pydantic.Field(
        description="S3 path to the [query result bucket](https://docs.aws.amazon.com/athena/latest/ug/querying.html#query-results-specify-location) which should be used by AWS Athena to store results of the"
        "queries executed by DataHub."
    )

    # overwrite default behavior of SQLAlchemyConfing
    include_views: Optional[bool] = pydantic.Field(
        default=False, description="Whether views should be ingested."
    )

    _s3_staging_dir_population = pydantic_renamed_field(
        old_name="s3_staging_dir",
        new_name="query_result_location",
        print_warning=True,
    )

    def get_sql_alchemy_url(self):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username or "",
            self.password.get_secret_value() if self.password else None,
            f"athena.{self.aws_region}.amazonaws.com:443",
            self.database,
            uri_opts={
                # as an URI option `s3_staging_dir` is still used due to PyAthena
                "s3_staging_dir": self.query_result_location,
                "work_group": self.work_group,
                "catalog_name": self.catalog_name,
                "role_arn": self.aws_role_arn,
                "duration_seconds": str(self.aws_role_assumption_duration),
            },
        )


@platform_name("Athena")
@support_status(SupportStatus.CERTIFIED)
@config_class(AthenaConfig)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration. Profiling uses sql queries on whole table which can be expensive operation.",
)
@capability(SourceCapability.LINEAGE_COARSE, "Supported for S3 tables")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
class AthenaSource(SQLAlchemySource):
    """
    This plugin supports extracting the following metadata from Athena
    - Tables, schemas etc.
    - Lineage for S3 tables.
    - Profiling when enabled.
    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "athena")
        self.cursor: Optional[BaseCursor] = None

    @classmethod
    def create(cls, config_dict, ctx):
        config = AthenaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        if not self.cursor:
            self.cursor = cast(BaseCursor, inspector.engine.raw_connection().cursor())
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

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        # In Athena the schema is the database and database is not existing
        return []

    def gen_schema_containers(
        self,
        schema: str,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.get_database_container_key(
            db_name=database, schema=schema
        )

        yield from gen_database_container(
            database=database_container_key.database,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.DATABASE],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def get_database_container_key(self, db_name: str, schema: str) -> DatabaseKey:
        # Because our overridden get_allowed_schemas method returns db_name as the schema name,
        # the db_name and schema here will be the same. Hence, we just ignore the schema parameter.
        # Based on community feedback, db_name only available if it is explicitly specified in the connection string.
        # If it is not available then we should use schema as db_name

        if not db_name:
            db_name = schema

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

    # It seems like database/schema filter in the connection string does not work and this to work around that
    def get_schema_names(self, inspector: Inspector) -> List[str]:
        athena_config = typing.cast(AthenaConfig, self.config)
        schemas = inspector.get_schema_names()
        if athena_config.database:
            return [schema for schema in schemas if schema == athena_config.database]
        return schemas

    def close(self):
        if self.cursor:
            self.cursor.close()
        super().close()
