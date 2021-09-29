import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pydantic

# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401
from snowflake.sqlalchemy import custom_types
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import quoted_name

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.sql.sql_common import (
    RecordTypeClass,
    SQLAlchemyConfig,
    SQLAlchemySource,
    TimeTypeClass,
    make_sqlalchemy_uri,
    register_custom_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)

register_custom_type(custom_types.TIMESTAMP_TZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_LTZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_NTZ, TimeTypeClass)
register_custom_type(custom_types.VARIANT, RecordTypeClass)

logger: logging.Logger = logging.getLogger(__name__)


class BaseSnowflakeConfig(ConfigModel):
    # Note: this config model is also used by the snowflake-usage source.

    scheme = "snowflake"

    username: Optional[str] = None
    password: Optional[str] = None
    host_port: str
    warehouse: Optional[str]
    role: Optional[str]

    def get_sql_alchemy_url(self, database=None):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password,
            self.host_port,
            database,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "warehouse": self.warehouse,
                    "role": self.role,
                }.items()
                if value
            },
        )


class SnowflakeConfig(BaseSnowflakeConfig, SQLAlchemyConfig):
    database_pattern: AllowDenyPattern = AllowDenyPattern(
        deny=[r"^UTIL_DB$", r"^SNOWFLAKE$", r"^SNOWFLAKE_SAMPLE_DATA$"]
    )

    database: Optional[str]  # deprecated

    @pydantic.validator("database")
    def note_database_opt_deprecation(cls, v, values, **kwargs):
        logger.warning(
            "snowflake's `database` option has been deprecated; use database_pattern instead"
        )
        values["database_pattern"].allow = f"^{v}$"
        return None

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url(database=None)


class SnowflakeSource(SQLAlchemySource):
    config: SnowflakeConfig
    current_database: str

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "snowflake")
        self._lineage_map: Optional[Dict[str, List[Tuple[str, str]]]] = None

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        for db_row in engine.execute(text("SHOW DATABASES")):
            with engine.connect() as conn:
                db = db_row.name
                if self.config.database_pattern.allowed(db):
                    self.current_database = db
                    conn.execute((f'USE DATABASE "{quoted_name(db, True)}"'))
                    inspector = inspect(conn)
                    yield inspector
                else:
                    self.report.report_dropped(db)

    def get_identifier(self, *, schema: str, entity: str, **kwargs: Any) -> str:
        regular = super().get_identifier(schema=schema, entity=entity, **kwargs)
        return f"{self.current_database.lower()}.{regular}"

    def _populate_lineage(self) -> None:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        query: str = """
with table_lineage_history as (
select
        r.value:"objectId" as upstream_table_id,
        r.value:"objectName" as upstream_table_name,
        r.value:"objectDomain" as upstream_table_domain,
        r.value:"columns" as upstream_table_columns,
        w.value:"objectId" as downstream_table_id,
        w.value:"objectName" as downstream_table_name,
        w.value:"objectDomain" as downstream_table_domain,
        w.value:"columns" as downstream_table_columns,
        t.query_start_time as query_start_time
    from
        (select * from snowflake.account_usage.access_history) t,
        lateral flatten(input => t.BASE_OBJECTS_ACCESSED) r,
        lateral flatten(input => t.OBJECTS_MODIFIED) w
    where r.value:"objectId" IS NOT NULL AND w.value:"objectId" IS NOT NULL)
select upstream_table_name, downstream_table_name, upstream_table_columns, downstream_table_columns from table_lineage_history
where upstream_table_domain = 'Table' and downstream_table_domain = 'Table'
QUALIFY ROW_NUMBER() OVER (PARTITION BY downstream_table_id, upstream_table_id, query_start_time ORDER BY downstream_table_id, upstream_table_id, query_start_time DESC) = 1
        """
        self._lineage_map = {}
        for db_row in engine.execute(query):
            key: str = db_row[1].lower().replace('"', "")
            if key not in self._lineage_map:
                self._lineage_map[key] = []
            self._lineage_map[key].append(
                (db_row[0].lower().replace('"', ""), db_row[2])
            )
            logger.info(f"Lineage[{key}]:{self._lineage_map[key]}")

    def get_upstream_lineage(
        self, dataset_name: str, custom_properties: Dict[str, str]
    ) -> Optional[UpstreamLineage]:
        if self._lineage_map is None:
            self._populate_lineage()
        assert self._lineage_map is not None
        lineage = self._lineage_map.get(f"{dataset_name}", None)
        if lineage is None:
            logger.info(f"No lineage found for {dataset_name}")
            return None
        upstreams: List[UpstreamClass] = []
        upstream_columns: List[str] = []
        for upstream_entry in lineage:
            upstream = UpstreamClass(
                dataset=builder.make_dataset_urn(
                    self.config.scheme, upstream_entry[0].lower(), self.config.env
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstreams.append(upstream)
            columns_obj = json.loads(upstream_entry[1])
            for e in columns_obj:
                upstream_columns.append(
                    f"{upstream_entry[0].lower()}.{e['columnId']}_{e['columnName'].lower()}"
                )
        custom_properties["upstream_columns"] = "; ".join(upstream_columns)
        logger.info(
            f"upstream_columns[{dataset_name}]:{custom_properties['upstream_columns']}"
        )
        return UpstreamLineage(upstreams=upstreams)
