from typing import Dict, Optional, Tuple

from pyathena.model import AthenaTableMetadata
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemyConfig,
    SQLAlchemySource,
    make_sqlalchemy_uri,
)
from pyathena.common import BaseCursor


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
    cursor: Optional[BaseCursor] = None

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "athena")

    @classmethod
    def create(cls, config_dict, ctx):
        config = AthenaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        if not self.cursor:
            self.cursor = inspector.dialect._raw_connection(inspector.engine).cursor()

        assert self.cursor
        # Unfortunately properties can be only get through private methods as those are not exposed
        # https://github.com/laughingman7743/PyAthena/blob/9e42752b0cc7145a87c3a743bb2634fe125adfa7/pyathena/model.py#L201
        metadata: AthenaTableMetadata = self.cursor._get_table_metadata(
            table_name=table, schema_name=schema
        )
        description = metadata.comment
        parameters = {k: v for k, v in metadata.parameters.items() if v}

        custom_properties = {
            **parameters,
            **{
                k: str(v)
                for k, v in metadata.__dict__.items()
                if (
                    k
                    not in [
                        "_columns",
                        "_parameters",
                        "_name",
                        "comment",
                        "_partition_keys",
                    ]
                    and v
                )
            },
        }
        if metadata.partition_keys:
            partition_key = []
            for key in metadata.partition_keys:
                partition_key.append(key.__dict__)
                custom_properties["_partition_keys"] = str(partition_key)

        return description, custom_properties

    def __exit__(self):
        if self.cursor:
            self.cursor.close()
