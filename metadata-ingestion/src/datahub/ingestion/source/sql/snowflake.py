import json
import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pydantic

# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401
from snowflake.sqlalchemy import custom_types, snowdialect
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes, text

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_common import (
    RecordTypeClass,
    SQLAlchemyConfig,
    SQLAlchemySource,
    SqlWorkUnit,
    TimeTypeClass,
    make_sqlalchemy_uri,
    register_custom_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass

register_custom_type(custom_types.TIMESTAMP_TZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_LTZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_NTZ, TimeTypeClass)
register_custom_type(custom_types.VARIANT, RecordTypeClass)

logger: logging.Logger = logging.getLogger(__name__)

APPLICATION_NAME = "acryl_datahub"

snowdialect.ischema_names["GEOGRAPHY"] = sqltypes.NullType


class BaseSnowflakeConfig(BaseTimeWindowConfig):
    # Note: this config model is also used by the snowflake-usage source.

    scheme = "snowflake"

    username: Optional[str] = None
    password: Optional[pydantic.SecretStr] = pydantic.Field(default=None, exclude=True)
    host_port: str
    warehouse: Optional[str]
    role: Optional[str]
    include_table_lineage: Optional[bool] = True

    def get_sql_alchemy_url(self, database=None):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password.get_secret_value() if self.password else None,
            self.host_port,
            f'"{database}"' if database is not None else database,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "warehouse": self.warehouse,
                    "role": self.role,
                    "application": APPLICATION_NAME,
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

    def get_sql_alchemy_url(self, database: str = None) -> str:
        return super().get_sql_alchemy_url(database=database)


class SnowflakeSource(SQLAlchemySource):
    config: SnowflakeConfig
    current_database: str

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "snowflake")
        self._lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        url = self.config.get_sql_alchemy_url(database=None)
        logger.debug(f"sql_alchemy_url={url}")
        db_listing_engine = create_engine(url, **self.config.options)

        for db_row in db_listing_engine.execute(text("SHOW DATABASES")):
            db = db_row.name
            if self.config.database_pattern.allowed(db):
                # We create a separate engine for each database in order to ensure that
                # they are isolated from each other.
                self.current_database = db
                engine = create_engine(
                    self.config.get_sql_alchemy_url(database=db), **self.config.options
                )

                with engine.connect() as conn:
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
WITH table_lineage_history AS (
    SELECT
        r.value:"objectName" AS upstream_table_name,
        r.value:"objectDomain" AS upstream_table_domain,
        r.value:"columns" AS upstream_table_columns,
        w.value:"objectName" AS downstream_table_name,
        w.value:"objectDomain" AS downstream_table_domain,
        w.value:"columns" AS downstream_table_columns,
        t.query_start_time AS query_start_time
    FROM
        (SELECT * from snowflake.account_usage.access_history) t,
        lateral flatten(input => t.BASE_OBJECTS_ACCESSED) r,
        lateral flatten(input => t.OBJECTS_MODIFIED) w
    WHERE r.value:"objectId" IS NOT NULL
    AND w.value:"objectId" IS NOT NULL
    AND w.value:"objectName" NOT LIKE '%.GE_TMP_%'
    AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
    AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3))
SELECT upstream_table_name, downstream_table_name, upstream_table_columns, downstream_table_columns
FROM table_lineage_history
WHERE upstream_table_domain = 'Table' and downstream_table_domain = 'Table'
QUALIFY ROW_NUMBER() OVER (PARTITION BY downstream_table_name, upstream_table_name ORDER BY query_start_time DESC) = 1        """.format(
            start_time_millis=int(self.config.start_time.timestamp() * 1000),
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )
        self._lineage_map = defaultdict(list)
        try:
            for db_row in engine.execute(query):
                # key is the down-stream table name
                key: str = db_row[1].lower().replace('"', "")
                self._lineage_map[key].append(
                    # (<upstream_table_name>, <json_list_of_upstream_columns>, <json_list_of_downstream_columns>)
                    (db_row[0].lower().replace('"', ""), db_row[2], db_row[3])
                )
                logger.debug(f"Lineage[{key}]:{self._lineage_map[key]}")
        except Exception as e:
            logger.warning(
                f"Extracting lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}."
            )

    def _get_upstream_lineage_info(
        self, dataset_urn: str
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:
        dataset_key = builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        if self._lineage_map is None:
            self._populate_lineage()
        assert self._lineage_map is not None
        dataset_name = dataset_key.name
        lineage = self._lineage_map.get(f"{dataset_name}", None)
        if lineage is None:
            logger.debug(f"No lineage found for {dataset_name}")
            return None
        upstream_tables: List[UpstreamClass] = []
        column_lineage: Dict[str, str] = {}
        for lineage_entry in lineage:
            # Update the table-lineage
            upstream_table_name = lineage_entry[0]
            if not self._is_dataset_allowed(upstream_table_name):
                continue
            upstream_table = UpstreamClass(
                dataset=builder.make_dataset_urn(
                    self.platform, upstream_table_name, self.config.env
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstream_tables.append(upstream_table)
            # Update column-lineage for each down-stream column.
            upstream_columns = [
                d["columnName"].lower() for d in json.loads(lineage_entry[1])
            ]
            downstream_columns = [
                d["columnName"].lower() for d in json.loads(lineage_entry[2])
            ]
            upstream_column_str = (
                f"{upstream_table_name}({', '.join(sorted(upstream_columns))})"
            )
            downstream_column_str = (
                f"{dataset_name}({', '.join(sorted(downstream_columns))})"
            )
            column_lineage_key = f"column_lineage[{upstream_table_name}]"
            column_lineage_value = (
                f"{{{upstream_column_str} -> {downstream_column_str}}}"
            )
            column_lineage[column_lineage_key] = column_lineage_value
            logger.debug(f"{column_lineage_key}:{column_lineage_value}")
        if upstream_tables:
            return UpstreamLineage(upstreams=upstream_tables), column_lineage
        return None

    # Override the base class method.
    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        for wu in super().get_workunits():
            if (
                self.config.include_table_lineage
                and isinstance(wu, SqlWorkUnit)
                and isinstance(wu.metadata, MetadataChangeEvent)
                and isinstance(wu.metadata.proposedSnapshot, DatasetSnapshot)
            ):
                dataset_snapshot: DatasetSnapshot = wu.metadata.proposedSnapshot
                assert dataset_snapshot
                # Join the workunit stream from super with the lineage info using the urn.
                lineage_info = self._get_upstream_lineage_info(dataset_snapshot.urn)
                if lineage_info is not None:
                    # Emit the lineage work unit
                    upstream_lineage, upstream_column_props = lineage_info
                    lineage_mcpw = MetadataChangeProposalWrapper(
                        entityType="dataset",
                        changeType=ChangeTypeClass.UPSERT,
                        entityUrn=dataset_snapshot.urn,
                        aspectName="upstreamLineage",
                        aspect=upstream_lineage,
                    )
                    lineage_wu = MetadataWorkUnit(
                        id=f"{self.platform}-{lineage_mcpw.entityUrn}-{lineage_mcpw.aspectName}",
                        mcp=lineage_mcpw,
                    )
                    self.report.report_workunit(lineage_wu)
                    yield lineage_wu

                    # Update the super's workunit to include the column-lineage in the custom properties. We need to follow
                    # the RCU semantics for both the aspects & customProperties in order to preserve the changes made by super.
                    aspects = dataset_snapshot.aspects
                    if aspects is None:
                        aspects = []
                    dataset_properties_aspect: Optional[DatasetPropertiesClass] = None
                    for aspect in aspects:
                        if isinstance(aspect, DatasetPropertiesClass):
                            dataset_properties_aspect = aspect
                    if dataset_properties_aspect is None:
                        dataset_properties_aspect = DatasetPropertiesClass()
                        aspects.append(dataset_properties_aspect)

                    custom_properties = (
                        {
                            **dataset_properties_aspect.customProperties,
                            **upstream_column_props,
                        }
                        if dataset_properties_aspect.customProperties
                        else upstream_column_props
                    )
                    dataset_properties_aspect.customProperties = custom_properties
                    dataset_snapshot.aspects = aspects

            # Emit the work unit from super.
            yield wu

    def _is_dataset_allowed(self, dataset_name: Optional[str]) -> bool:
        # View lineages is not supported. Add the allow/deny pattern for that when it is supported.
        if dataset_name is None:
            return True
        dataset_params = dataset_name.split(".")
        if len(dataset_params) != 3:
            return True
        if (
            not self.config.database_pattern.allowed(dataset_params[0])
            or not self.config.schema_pattern.allowed(dataset_params[1])
            or not self.config.table_pattern.allowed(dataset_params[2])
        ):
            return False
        return True

    # Stateful Ingestion specific overrides
    # NOTE: There is no special state associated with this source yet than what is provided by sql_common.
    def get_platform_instance_id(self) -> str:
        """Overrides the source identifier for stateful ingestion."""
        return self.config.host_port
