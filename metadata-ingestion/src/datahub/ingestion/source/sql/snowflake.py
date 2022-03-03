import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pydantic

# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401
import sqlalchemy.engine
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.network import (
    DEFAULT_AUTHENTICATOR,
    EXTERNAL_BROWSER_AUTHENTICATOR,
    KEY_PAIR_AUTHENTICATOR,
)
from snowflake.sqlalchemy import custom_types, snowdialect
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes, text

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.sql.sql_common import (
    RecordTypeClass,
    SQLAlchemyConfig,
    SQLAlchemySource,
    SQLSourceReport,
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


@dataclass
class SnowflakeReport(SQLSourceReport):
    num_table_to_table_edges_scanned: int = 0
    num_table_to_view_edges_scanned: int = 0
    num_view_to_table_edges_scanned: int = 0
    num_external_table_edges_scanned: int = 0
    upstream_lineage: Dict[str, List[str]] = field(default_factory=dict)


class BaseSnowflakeConfig(BaseTimeWindowConfig):
    # Note: this config model is also used by the snowflake-usage source.

    scheme = "snowflake"

    username: Optional[str] = None
    password: Optional[pydantic.SecretStr] = pydantic.Field(default=None, exclude=True)
    private_key_path: Optional[str]
    private_key_password: Optional[pydantic.SecretStr] = pydantic.Field(
        default=None, exclude=True
    )
    authentication_type: Optional[str] = "DEFAULT_AUTHENTICATOR"
    host_port: str
    warehouse: Optional[str]
    role: Optional[str]
    include_table_lineage: Optional[bool] = True
    include_view_lineage: Optional[bool] = True

    connect_args: Optional[dict]

    @pydantic.validator("authentication_type", always=True)
    def authenticator_type_is_valid(cls, v, values, **kwargs):
        valid_auth_types = {
            "DEFAULT_AUTHENTICATOR": DEFAULT_AUTHENTICATOR,
            "EXTERNAL_BROWSER_AUTHENTICATOR": EXTERNAL_BROWSER_AUTHENTICATOR,
            "KEY_PAIR_AUTHENTICATOR": KEY_PAIR_AUTHENTICATOR,
        }
        if v not in valid_auth_types.keys():
            raise ValueError(
                f"unsupported authenticator type '{v}' was provided,"
                f" use one of {list(valid_auth_types.keys())}"
            )
        else:
            if v == "KEY_PAIR_AUTHENTICATOR":
                # If we are using key pair auth, we need the private key path and password to be set
                if values.get("private_key_path") is None:
                    raise ValueError(
                        f"'private_key_path' was none "
                        f"but should be set when using {v} authentication"
                    )
                if values.get("private_key_password") is None:
                    raise ValueError(
                        f"'private_key_password' was none "
                        f"but should be set when using {v} authentication"
                    )
            logger.info(f"using authenticator type '{v}'")
        return valid_auth_types.get(v)

    @pydantic.validator("include_view_lineage")
    def validate_include_view_lineage(cls, v, values):
        if not values.get("include_table_lineage") and v:
            raise ValueError(
                "include_table_lineage must be True for include_view_lineage to be set."
            )
        return v

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
                    "authenticator": self.authentication_type,
                    "warehouse": self.warehouse,
                    "role": self.role,
                    "application": APPLICATION_NAME,
                }.items()
                if value
            },
        )

    def get_sql_alchemy_connect_args(self) -> dict:
        if self.authentication_type != KEY_PAIR_AUTHENTICATOR:
            return {}
        if self.connect_args is None:
            if self.private_key_path is None:
                raise ValueError("missing required private key path to read key from")
            if self.private_key_password is None:
                raise ValueError("missing required private key password")
            with open(self.private_key_path, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=self.private_key_password.get_secret_value().encode(),
                    backend=default_backend(),
                )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            self.connect_args = {"private_key": pkb}
        return self.connect_args


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

    def get_sql_alchemy_connect_args(self) -> dict:
        return super().get_sql_alchemy_connect_args()


class SnowflakeSource(SQLAlchemySource):
    def __init__(self, config: SnowflakeConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "snowflake")
        self._lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self._external_lineage_map: Optional[Dict[str, Set[str]]] = None
        self.report: SnowflakeReport = SnowflakeReport()
        self.config: SnowflakeConfig = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        url = self.config.get_sql_alchemy_url(database=None)
        logger.debug(f"sql_alchemy_url={url}")

        db_listing_engine = create_engine(
            url,
            connect_args=self.config.get_sql_alchemy_connect_args(),
            **self.config.options,
        )

        for db_row in db_listing_engine.execute(text("SHOW DATABASES")):
            db = db_row.name
            if self.config.database_pattern.allowed(db):
                # We create a separate engine for each database in order to ensure that
                # they are isolated from each other.
                self.current_database = db
                engine = create_engine(
                    self.config.get_sql_alchemy_url(database=db),
                    connect_args=self.config.get_sql_alchemy_connect_args(),
                    **self.config.options,
                )

                with engine.connect() as conn:
                    inspector = inspect(conn)
                    yield inspector
            else:
                self.report.report_dropped(db)

    def get_identifier(self, *, schema: str, entity: str, **kwargs: Any) -> str:
        regular = super().get_identifier(schema=schema, entity=entity, **kwargs)
        return f"{self.current_database.lower()}.{regular}"

    def _populate_view_upstream_lineage(self, engine: sqlalchemy.engine.Engine) -> None:
        # NOTE: This query captures only the upstream lineage of a view (with no column lineage).
        # For more details see: https://docs.snowflake.com/en/user-guide/object-dependencies.html#object-dependencies
        # and also https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html#usage-notes for current limitations on capturing the lineage for views.
        view_upstream_lineage_query: str = """
SELECT
  concat(
    referenced_database, '.', referenced_schema,
    '.', referenced_object_name
  ) AS view_upstream,
  concat(
    referencing_database, '.', referencing_schema,
    '.', referencing_object_name
  ) AS downstream_view
FROM
  snowflake.account_usage.object_dependencies
WHERE
  referencing_object_domain in ('VIEW', 'MATERIALIZED VIEW')
        """

        assert self._lineage_map is not None
        num_edges: int = 0

        try:
            for db_row in engine.execute(view_upstream_lineage_query):
                # Process UpstreamTable/View/ExternalTable/Materialized View->View edge.
                view_upstream: str = db_row["view_upstream"].lower()
                view_name: str = db_row["downstream_view"].lower()
                # key is the downstream view name
                self._lineage_map[view_name].append(
                    # (<upstream_table_name>, <empty_json_list_of_upstream_table_columns>, <empty_json_list_of_downstream_view_columns>)
                    (view_upstream, "[]", "[]")
                )
                num_edges += 1
                logger.debug(
                    f"Upstream->View: Lineage[View(Down)={view_name}]:Upstream={view_upstream}"
                )
        except Exception as e:
            logger.warning(
                f"Extracting the upstream view lineage from Snowflake failed."
                f"Please check your permissions. Continuing...\nError was {e}."
            )
        logger.info(f"A total of {num_edges} View upstream edges found.")
        self.report.num_table_to_view_edges_scanned = num_edges

    def _populate_view_downstream_lineage(
        self, engine: sqlalchemy.engine.Engine
    ) -> None:
        # NOTE: This query captures both the upstream and downstream table lineage for views.
        # We need this query to populate the downstream lineage of a view,
        # as well as to delete the false direct edges between the upstream and downstream tables of a view.
        # See https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html#usage-notes for current limitations on capturing the lineage for views.
        # Eg: For viewA->viewB->ViewC->TableD, snowflake does not yet log intermediate view logs, resulting in only the viewA->TableD edge.
        view_lineage_query: str = """
WITH view_lineage_history AS (
  SELECT
    vu.value : "objectName" AS view_name,
    vu.value : "objectDomain" AS view_domain,
    vu.value : "columns" AS view_columns,
    r.value : "objectName" AS upstream_table_name,
    r.value : "objectDomain" AS upstream_table_domain,
    r.value : "columns" AS upstream_table_columns,
    w.value : "objectName" AS downstream_table_name,
    w.value : "objectDomain" AS downstream_table_domain,
    w.value : "columns" AS downstream_table_columns,
    t.query_start_time AS query_start_time
  FROM
    (
      SELECT
        *
      FROM
        snowflake.account_usage.access_history
    ) t,
    lateral flatten(input => t.DIRECT_OBJECTS_ACCESSED) vu,
    lateral flatten(input => t.BASE_OBJECTS_ACCESSED) r,
    lateral flatten(input => t.OBJECTS_MODIFIED) w
  WHERE
    vu.value : "objectId" IS NOT NULL
    AND r.value : "objectId" IS NOT NULL
    AND w.value : "objectId" IS NOT NULL
    AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
    AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
)
SELECT
  view_name,
  view_columns,
  upstream_table_name,
  upstream_table_domain,
  upstream_table_columns,
  downstream_table_name,
  downstream_table_domain,
  downstream_table_columns
FROM
  view_lineage_history
WHERE
  view_domain in ('View', 'Materialized view')
  AND view_name != upstream_table_name
  AND upstream_table_name != downstream_table_name
  AND view_name != downstream_table_name
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY view_name,
    upstream_table_name,
    downstream_table_name
    ORDER BY
      query_start_time DESC
  ) = 1
        """.format(
            start_time_millis=int(self.config.start_time.timestamp() * 1000),
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )

        assert self._lineage_map is not None
        num_edges: int = 0
        num_false_edges: int = 0

        try:
            for db_row in engine.execute(view_lineage_query):
                # We get two edges here.
                # (1) False UpstreamTable->Downstream table that will be deleted.
                # (2) View->DownstreamTable that will be added.

                view_name: str = db_row[0].lower().replace('"', "")
                upstream_table: str = db_row[2].lower().replace('"', "")
                downstream_table: str = db_row[5].lower().replace('"', "")
                # (1) Delete false direct edge between upstream_table and downstream_table
                prior_edges: List[Tuple[str, str, str]] = self._lineage_map[
                    downstream_table
                ]
                self._lineage_map[downstream_table] = [
                    entry
                    for entry in self._lineage_map[downstream_table]
                    if entry[0] != upstream_table
                ]
                for false_edge in set(prior_edges) - set(
                    self._lineage_map[downstream_table]
                ):
                    logger.debug(
                        f"False Table->Table edge removed: Lineage[Table(Down)={downstream_table}]:Table(Up)={false_edge}."
                    )
                    num_false_edges += 1

                # (2) Add view->downstream table lineage.
                self._lineage_map[downstream_table].append(
                    # (<upstream_view_name>, <json_list_of_upstream_view_columns>, <json_list_of_downstream_columns>)
                    (view_name, db_row[1], db_row[7])
                )
                logger.debug(
                    f"View->Table: Lineage[Table(Down)={downstream_table}]:View(Up)={self._lineage_map[downstream_table]}, downstream_domain={db_row[6]}"
                )
                num_edges += 1

        except Exception as e:
            logger.warning(
                f"Extracting the view lineage from Snowflake failed."
                f"Please check your permissions. Continuing...\nError was {e}."
            )
        logger.info(
            f"Found {num_edges} View->Table edges. Removed {num_false_edges} false Table->Table edges."
        )
        self.report.num_view_to_table_edges_scanned = num_edges

    def _populate_view_lineage(self) -> None:
        if not self.config.include_view_lineage:
            return
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        self._populate_view_upstream_lineage(engine)
        self._populate_view_downstream_lineage(engine)

    def _populate_external_lineage(self) -> None:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        # Handles the case where a table is populated from an external location via copy.
        # Eg: copy into category_english from 's3://acryl-snow-demo-olist/olist_raw_data/category_english'credentials=(aws_key_id='...' aws_secret_key='...')  pattern='.*.csv';
        query: str = """
    WITH external_table_lineage_history AS (
        SELECT
            r.value:"locations" as upstream_locations,
            w.value:"objectName" AS downstream_table_name,
            w.value:"objectDomain" AS downstream_table_domain,
            w.value:"columns" AS downstream_table_columns,
            t.query_start_time AS query_start_time
        FROM
            (SELECT * from snowflake.account_usage.access_history) t,
            lateral flatten(input => t.BASE_OBJECTS_ACCESSED) r,
            lateral flatten(input => t.OBJECTS_MODIFIED) w
        WHERE r.value:"locations" IS NOT NULL
        AND w.value:"objectId" IS NOT NULL
        AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
        AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3))
    SELECT upstream_locations, downstream_table_name, downstream_table_columns
    FROM external_table_lineage_history
    WHERE downstream_table_domain = 'Table'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY downstream_table_name ORDER BY query_start_time DESC) = 1""".format(
            start_time_millis=int(self.config.start_time.timestamp() * 1000),
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )

        num_edges: int = 0
        self._external_lineage_map = defaultdict(set)
        try:
            for db_row in engine.execute(query):
                # key is the down-stream table name
                key: str = db_row[1].lower().replace('"', "")
                self._external_lineage_map[key] |= {*json.loads(db_row[0])}
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]}"
                )
        except Exception as e:
            logger.warning(
                f"Populating table external lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}."
            )
        # Handles the case for explicitly created external tables.
        # NOTE: Snowflake does not log this information to the access_history table.
        external_tables_query: str = "show external tables"
        try:
            for db_row in engine.execute(external_tables_query):
                key = (
                    f"{db_row.database_name}.{db_row.schema_name}.{db_row.name}".lower()
                )
                self._external_lineage_map[key].add(db_row.location)
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]}"
                )
                num_edges += 1
        except Exception as e:
            logger.warning(
                f"Populating external table lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}."
            )
        logger.info(f"Found {num_edges} external lineage edges.")
        self.report.num_external_table_edges_scanned = num_edges

    def _populate_lineage(self) -> None:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(
            url,
            connect_args=self.config.get_sql_alchemy_connect_args(),
            **self.config.options,
        )
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
        lateral flatten(input => t.DIRECT_OBJECTS_ACCESSED) r,
        lateral flatten(input => t.OBJECTS_MODIFIED) w
    WHERE r.value:"objectId" IS NOT NULL
    AND w.value:"objectId" IS NOT NULL
    AND w.value:"objectName" NOT LIKE '%.GE_TMP_%'
    AND w.value:"objectName" NOT LIKE '%.GE_TEMP_%'
    AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
    AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3))
SELECT upstream_table_name, downstream_table_name, upstream_table_columns, downstream_table_columns
FROM table_lineage_history
WHERE upstream_table_domain in ('Table', 'External table') and downstream_table_domain = 'Table'
QUALIFY ROW_NUMBER() OVER (PARTITION BY downstream_table_name, upstream_table_name ORDER BY query_start_time DESC) = 1        """.format(
            start_time_millis=int(self.config.start_time.timestamp() * 1000),
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )
        num_edges: int = 0
        self._lineage_map = defaultdict(list)
        try:
            for db_row in engine.execute(query):
                # key is the down-stream table name
                key: str = db_row[1].lower().replace('"', "")
                self._lineage_map[key].append(
                    # (<upstream_table_name>, <json_list_of_upstream_columns>, <json_list_of_downstream_columns>)
                    (db_row[0].lower().replace('"', ""), db_row[2], db_row[3])
                )
                num_edges += 1
                logger.debug(
                    f"Lineage[Table(Down)={key}]:Table(Up)={self._lineage_map[key]}"
                )
        except Exception as e:
            logger.warning(
                f"Extracting lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}."
            )
        logger.info(
            f"A total of {num_edges} Table->Table edges found"
            f" for {len(self._lineage_map)} downstream tables.",
        )
        self.report.num_table_to_table_edges_scanned = num_edges

    def _get_upstream_lineage_info(
        self, dataset_urn: str
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:
        dataset_key = builder.dataset_urn_to_key(dataset_urn)
        if dataset_key is None:
            logger.warning(f"Invalid dataset urn {dataset_urn}. Could not get key!")
            return None

        if self._lineage_map is None:
            self._populate_lineage()
            self._populate_view_lineage()
        if self._external_lineage_map is None:
            self._populate_external_lineage()

        assert self._lineage_map is not None
        assert self._external_lineage_map is not None
        dataset_name = dataset_key.name
        lineage = self._lineage_map[dataset_name]
        external_lineage = self._external_lineage_map[dataset_name]
        if not (lineage or external_lineage):
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
                dataset=builder.make_dataset_urn_with_platform_instance(
                    self.platform,
                    upstream_table_name,
                    self.config.platform_instance,
                    self.config.env,
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

        for external_lineage_entry in external_lineage:
            # For now, populate only for S3
            if external_lineage_entry.startswith("s3://"):
                external_upstream_table = UpstreamClass(
                    dataset=make_s3_urn(external_lineage_entry, self.config.env),
                    type=DatasetLineageTypeClass.COPY,
                )
                upstream_tables.append(external_upstream_table)

        if upstream_tables:
            logger.debug(
                f"Upstream lineage of '{dataset_name}': {[u.dataset for u in upstream_tables]}"
            )
            self.report.upstream_lineage[dataset_name] = [
                u.dataset for u in upstream_tables
            ]
            return UpstreamLineage(upstreams=upstream_tables), column_lineage
        return None

    # Override the base class method.
    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        for wu in super().get_workunits():
            if (
                self.config.include_table_lineage
                and isinstance(wu, MetadataWorkUnit)
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
            or (
                self.config.include_view_lineage
                and not self.config.view_pattern.allowed(dataset_params[2])
            )
        ):
            return False
        return True

    # Stateful Ingestion specific overrides
    # NOTE: There is no special state associated with this source yet than what is provided by sql_common.
    def get_platform_instance_id(self) -> str:
        """Overrides the source identifier for stateful ingestion."""
        return self.config.host_port
