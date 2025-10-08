import logging
from typing import (
    Dict,
    Iterable,
    Optional,
    Tuple,
)

import pydantic
from ibm_db_sa import dialect as DB2Dialect
from sqlalchemy.engine.reflection import Inspector
from sqlglot.dialects.dialect import Dialect as SQLGlotDialect, NormalizationStrategy

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri

logger = logging.getLogger(__name__)


class Db2(SQLGlotDialect):
    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE


class DB2DialectWithoutNormalization(DB2Dialect):
    def initialize(self, connection):
        # see:
        # - https://github.com/ibmdb/python-ibmdbsa/issues/153
        # - https://github.com/ibmdb/python-ibmdbsa/issues/170
        super().initialize(connection)
        self._reflector.normalize_name = lambda s: s
        self._reflector.denormalize_name = lambda s: s

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        # see: https://github.com/ibmdb/python-ibmdbsa/issues/171
        try:
            comment = super().get_table_comment(
                connection, table_name, schema=schema, **kwargs
            )
            if comment and comment.get("text"):
                return comment
        except NotImplementedError:
            pass

        if self.has_table(connection, "TABLES", schema="SYSCAT"):
            result = connection.execute(
                """
                select REMARKS
                from SYSCAT.TABLES
                where TABSCHEMA = ?
                and TABNAME = ?
            """,
                (schema, table_name),
            )
            return {"text": result.scalar()}

        return {"text": ""}


class Db2Config(BasicSQLAlchemyConfig):
    # Override defaults
    host_port: str = pydantic.Field(default="localhost:50000")
    scheme: str = pydantic.Field(default="db2+ibm_db")  # TODO: hide this?
    username: str
    password: str
    database: str

    def get_sql_alchemy_url(self):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password,
            self.host_port,
        )


@platform_name("IBM Db2", id="db2")
@config_class(Db2Config)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
class Db2Source(SQLAlchemySource):
    def __init__(self, config: Db2Config, ctx: PipelineContext):
        super().__init__(config, ctx, "db2")

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "Db2Source":
        config = Db2Config.parse_obj(config_dict)
        return cls(config, ctx)

    def get_view_default_db_schema(
        self, _dataset_name: str, inspector: Inspector, schema: str, view: str
    ) -> Tuple[Optional[str], str]:
        try:
            result = inspector.bind.execute(
                """
                select QUALIFIER
                from SYSCAT.VIEWS
                where VIEWSCHEMA = ?
                and VIEWNAME = ?
            """,
                (schema, view),
            )
            return None, result.scalar()
        except Exception as e:
            logger.warning(
                f"Failed to get qualifier for unqualified names for view {schema}.{view}: {e}",
                exc_info=e,
            )  # TODO add to report
        return super().get_view_default_db_schema(inspector, schema, view)

    def get_inspectors(self) -> Iterable[Inspector]:
        for inspector in super().get_inspectors():
            inspector.dialect = DB2DialectWithoutNormalization()
            inspector.dialect.initialize(inspector.bind)
            yield inspector

    def get_schema_names(self, inspector) -> Iterable[str]:
        for s in inspector.get_schema_names():
            # inspect.get_schema_names() can return schema names with extra space on the end
            yield s.rstrip()
