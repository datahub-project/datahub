import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pydantic

# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401
import sqlalchemy.engine
from snowflake.sqlalchemy import custom_types, snowdialect
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes, text

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.sql.sql_common import (
    RecordTypeClass,
    SQLAlchemySource,
    SqlWorkUnit,
    TimeTypeClass,
    register_custom_type,
)
from datahub.ingestion.source_config.sql.snowflake import (
    APPLICATION_NAME,
    SnowflakeConfig,
)
from datahub.ingestion.source_report.sql.snowflake import SnowflakeReport
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

snowdialect.ischema_names["GEOGRAPHY"] = sqltypes.NullType


@platform_name("Snowflake")
@config_class(SnowflakeConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class SnowflakeSource(SQLAlchemySource, TestableSource):
    def __init__(self, config: SnowflakeConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "snowflake")
        self._lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self._external_lineage_map: Optional[Dict[str, Set[str]]] = None
        self.report: SnowflakeReport = SnowflakeReport()
        self.config: SnowflakeConfig = config
        self.provision_role_in_progress: bool = False
        self.profile_candidates: Dict[str, List[str]] = {}

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            SnowflakeConfig.Config.extra = (
                pydantic.Extra.allow
            )  # we are okay with extra fields during this stage
            connection_conf = SnowflakeConfig.parse_obj(config_dict)
            if connection_conf.authentication_type == "DEFAULT_AUTHENTICATOR":
                connection: snowflake.connector.SnowflakeConnection = (
                    snowflake.connector.connect(
                        user=connection_conf.username,
                        password=connection_conf.password.get_secret_value()
                        if connection_conf.password
                        else None,
                        account=connection_conf.account_id,
                        warehouse=connection_conf.warehouse,
                        role=connection_conf.role,
                        application=APPLICATION_NAME,
                        **connection_conf.connect_args or {},
                    )
                )
                assert connection
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(capable=True)
                )
            else:
                raise NotImplementedError(
                    "Don't support testing connections for non DEFAULT AUTHENTICATED modes"
                )

        except Exception as e:
            # TODO - do we need sensitive error logging ?
            logger.error(f"Failed to test connection due to {e}", exc_info=e)
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=f"{e}"
                )
            )
        finally:
            SnowflakeConfig.Config.extra = (
                pydantic.Extra.forbid
            )  # set config flexibility back to strict

    def get_metadata_engine(
        self, database: Optional[str] = None
    ) -> sqlalchemy.engine.Engine:
        if self.provision_role_in_progress and self.config.provision_role is not None:
            username: Optional[str] = self.config.provision_role.admin_username
            password: Optional[
                pydantic.SecretStr
            ] = self.config.provision_role.admin_password
            role: Optional[str] = self.config.provision_role.admin_role
        else:
            username = self.config.username
            password = self.config.password
            role = self.config.role

        url = self.config.get_sql_alchemy_url(
            database=database, username=username, password=password, role=role
        )
        logger.debug(f"sql_alchemy_url={url}")
        if self.config.authentication_type == "OAUTH_AUTHENTICATOR":
            return create_engine(
                url,
                creator=self.config.get_oauth_connection,
                **self.config.get_options(),
            )
        else:
            return create_engine(
                url,
                **self.config.get_options(),
            )

    def inspect_session_metadata(self) -> Any:
        db_engine = self.get_metadata_engine()
        try:
            logger.info("Checking current version")
            for db_row in db_engine.execute("select CURRENT_VERSION()"):
                self.report.saas_version = db_row[0]
        except Exception as e:
            self.report.report_failure("version", f"Error: {e}")
        try:
            logger.info("Checking current warehouse")
            for db_row in db_engine.execute("select current_warehouse()"):
                self.report.default_warehouse = db_row[0]
        except Exception as e:
            self.report.report_failure("current_warehouse", f"Error: {e}")
        try:
            logger.info("Checking current database")
            for db_row in db_engine.execute("select current_database()"):
                self.report.default_db = db_row[0]
        except Exception as e:
            self.report.report_failure("current_database", f"Error: {e}")
        try:
            logger.info("Checking current schema")
            for db_row in db_engine.execute("select current_schema()"):
                self.report.default_schema = db_row[0]
        except Exception as e:
            self.report.report_failure("current_schema", f"Error: {e}")

    def inspect_role_grants(self) -> Any:
        db_engine = self.get_metadata_engine()
        cur_role = None
        if self.config.role is None:
            for db_row in db_engine.execute("select CURRENT_ROLE()"):
                cur_role = db_row[0]
        else:
            cur_role = self.config.role

        if cur_role is None:
            return

        self.report.role = cur_role
        logger.info(f"Current role is {cur_role}")
        if cur_role.lower() == "accountadmin" or not self.config.check_role_grants:
            return

        logger.info(f"Checking grants for role {cur_role}")
        for db_row in db_engine.execute(text(f"show grants to role {cur_role}")):
            privilege = db_row["privilege"]
            granted_on = db_row["granted_on"]
            name = db_row["name"]
            self.report.role_grants.append(
                f"{privilege} granted on {granted_on} {name}"
            )

    def get_inspectors(self) -> Iterable[Inspector]:
        db_listing_engine = self.get_metadata_engine(database=None)

        for db_row in db_listing_engine.execute(text("SHOW DATABASES")):
            db = db_row.name
            if self.config.database_pattern.allowed(db):
                # We create a separate engine for each database in order to ensure that
                # they are isolated from each other.
                self.current_database = db
                engine = self.get_metadata_engine(database=db)

                with engine.connect() as conn:
                    inspector = inspect(conn)
                    yield inspector
            else:
                self.report.report_dropped(db)

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        regular = super().get_identifier(
            schema=schema, entity=entity, inspector=inspector, **kwargs
        )
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
                if not self._is_dataset_allowed(dataset_name=view_name, is_view=True):
                    continue
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
            self.warn(
                logger,
                "view_upstream_lineage",
                "Extracting the upstream view lineage from Snowflake failed."
                + f"Please check your permissions. Continuing...\nError was {e}.",
            )
        logger.info(f"A total of {num_edges} View upstream edges found.")
        self.report.num_table_to_view_edges_scanned = num_edges

    def _populate_view_downstream_lineage(
        self, engine: sqlalchemy.engine.Engine
    ) -> None:
        # This query captures the downstream table lineage for views.
        # See https://docs.snowflake.com/en/sql-reference/account-usage/access_history.html#usage-notes for current limitations on capturing the lineage for views.
        # Eg: For viewA->viewB->ViewC->TableD, snowflake does not yet log intermediate view logs, resulting in only the viewA->TableD edge.
        view_lineage_query: str = """
WITH view_lineage_history AS (
  SELECT
    vu.value : "objectName" AS view_name,
    vu.value : "objectDomain" AS view_domain,
    vu.value : "columns" AS view_columns,
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
    lateral flatten(input => t.OBJECTS_MODIFIED) w
  WHERE
    vu.value : "objectId" IS NOT NULL
    AND w.value : "objectId" IS NOT NULL
    AND w.value : "objectName" NOT LIKE '%.GE_TMP_%'
    AND w.value : "objectName" NOT LIKE '%.GE_TEMP_%'
    AND t.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
    AND t.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
)
SELECT
  view_name,
  view_columns,
  downstream_table_name,
  downstream_table_columns
FROM
  view_lineage_history
WHERE
  view_domain in ('View', 'Materialized view')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY view_name,
    downstream_table_name
    ORDER BY
      query_start_time DESC
  ) = 1
        """.format(
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )

        assert self._lineage_map is not None
        self.report.num_view_to_table_edges_scanned = 0

        try:
            db_rows = engine.execute(view_lineage_query)
        except Exception as e:
            self.warn(
                logger,
                "view_downstream_lineage",
                f"Extracting the view lineage from Snowflake failed."
                f"Please check your permissions. Continuing...\nError was {e}.",
            )
        else:
            for db_row in db_rows:
                view_name: str = db_row["view_name"].lower().replace('"', "")
                if not self._is_dataset_allowed(dataset_name=view_name, is_view=True):
                    continue
                downstream_table: str = (
                    db_row["downstream_table_name"].lower().replace('"', "")
                )
                # Capture view->downstream table lineage.
                self._lineage_map[downstream_table].append(
                    # (<upstream_view_name>, <json_list_of_upstream_view_columns>, <json_list_of_downstream_columns>)
                    (
                        view_name,
                        db_row["view_columns"],
                        db_row["downstream_table_columns"],
                    )
                )
                self.report.num_view_to_table_edges_scanned += 1

                logger.debug(
                    f"View->Table: Lineage[Table(Down)={downstream_table}]:View(Up)={self._lineage_map[downstream_table]}"
                )

        logger.info(
            f"Found {self.report.num_view_to_table_edges_scanned} View->Table edges."
        )

    def _populate_view_lineage(self) -> None:
        if not self.config.include_view_lineage:
            return
        engine = self.get_metadata_engine(database=None)
        self._populate_view_upstream_lineage(engine)
        self._populate_view_downstream_lineage(engine)

    def _populate_external_lineage(self) -> None:
        engine = self.get_metadata_engine(database=None)
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
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )

        num_edges: int = 0
        self._external_lineage_map = defaultdict(set)
        try:
            for db_row in engine.execute(query):
                # key is the down-stream table name
                key: str = db_row[1].lower().replace('"', "")
                if not self._is_dataset_allowed(key):
                    continue
                self._external_lineage_map[key] |= {*json.loads(db_row[0])}
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]} via access_history"
                )
        except Exception as e:
            logger.warning(
                f"Populating table external lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}."
            )
        # Handles the case for explicitly created external tables.
        # NOTE: Snowflake does not log this information to the access_history table.
        external_tables_query: str = "show external tables in account"
        try:
            for db_row in engine.execute(external_tables_query):
                key = (
                    f"{db_row.database_name}.{db_row.schema_name}.{db_row.name}".lower()
                )
                if not self._is_dataset_allowed(dataset_name=key):
                    continue
                self._external_lineage_map[key].add(db_row.location)
                logger.debug(
                    f"ExternalLineage[Table(Down)={key}]:External(Up)={self._external_lineage_map[key]} via show external tables"
                )
                num_edges += 1
        except Exception as e:
            self.warn(
                logger,
                "external_lineage",
                f"Populating external table lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}.",
            )
        logger.info(f"Found {num_edges} external lineage edges.")
        self.report.num_external_table_edges_scanned = num_edges

    def _populate_lineage(self) -> None:
        engine = self.get_metadata_engine(database=None)
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
            start_time_millis=int(self.config.start_time.timestamp() * 1000)
            if not self.config.ignore_start_time_lineage
            else 0,
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )
        num_edges: int = 0
        self._lineage_map = defaultdict(list)
        try:
            for db_row in engine.execute(query):
                # key is the down-stream table name
                key: str = db_row[1].lower().replace('"', "")
                upstream_table_name = db_row[0].lower().replace('"', "")
                if not (
                    self._is_dataset_allowed(key)
                    or self._is_dataset_allowed(upstream_table_name)
                ):
                    continue
                self._lineage_map[key].append(
                    # (<upstream_table_name>, <json_list_of_upstream_columns>, <json_list_of_downstream_columns>)
                    (upstream_table_name, db_row[2], db_row[3])
                )
                num_edges += 1
                logger.debug(
                    f"Lineage[Table(Down)={key}]:Table(Up)={self._lineage_map[key]}"
                )
        except Exception as e:
            self.warn(
                logger,
                "lineage",
                f"Extracting lineage from Snowflake failed."
                f"Please check your premissions. Continuing...\nError was {e}.",
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
            if self.config.upstream_lineage_in_report:
                self.report.upstream_lineage[dataset_name] = [
                    u.dataset for u in upstream_tables
                ]
            return UpstreamLineage(upstreams=upstream_tables), column_lineage
        return None

    def add_config_to_report(self):
        self.report.cleaned_account_id = self.config.get_account()
        self.report.ignore_start_time_lineage = self.config.ignore_start_time_lineage
        self.report.upstream_lineage_in_report = self.config.upstream_lineage_in_report
        if not self.report.ignore_start_time_lineage:
            self.report.lineage_start_time = self.config.start_time
        self.report.lineage_end_time = self.config.end_time
        self.report.check_role_grants = self.config.check_role_grants
        if self.config.provision_role is not None:
            self.report.run_ingestion = self.config.provision_role.run_ingestion

    def do_provision_role_internal(self):
        provision_role_block = self.config.provision_role
        if provision_role_block is None:
            return
        self.report.provision_role_done = not provision_role_block.dry_run

        role = self.config.role
        if role is None:
            role = "datahub_role"
            self.warn(
                logger,
                "role-grant",
                f"role not specified during provision role using {role} as default",
            )
        self.report.role = role

        warehouse = self.config.warehouse

        logger.info("Creating connection for provision_role")
        engine = self.get_metadata_engine(database=None)

        sqls: List[str] = []
        if provision_role_block.drop_role_if_exists:
            sqls.append(f"DROP ROLE IF EXISTS {role}")

        sqls.append(f"CREATE ROLE IF NOT EXISTS {role}")

        if warehouse is None:
            self.warn(
                logger, "role-grant", "warehouse not specified during provision role"
            )
        else:
            sqls.append(f"grant operate, usage on warehouse {warehouse} to role {role}")

        for inspector in self.get_inspectors():
            db_name = self.get_db_name(inspector)
            sqls.extend(
                [
                    f"grant usage on DATABASE {db_name} to role {role}",
                    f"grant usage on all schemas in database {db_name} to role {role}",
                    f"grant usage on future schemas in database {db_name} to role {role}",
                ]
            )
            if self.config.profiling.enabled:
                sqls.extend(
                    [
                        f"grant select on all tables in database {db_name} to role {role}",
                        f"grant select on future tables in database {db_name} to role {role}",
                        f"grant select on all external tables in database {db_name} to role {role}",
                        f"grant select on future external tables in database {db_name} to role {role}",
                        f"grant select on all views in database {db_name} to role {role}",
                        f"grant select on future views in database {db_name} to role {role}",
                    ]
                )
            else:
                sqls.extend(
                    [
                        f"grant references on all tables in database {db_name} to role {role}",
                        f"grant references on future tables in database {db_name} to role {role}",
                        f"grant references on all external tables in database {db_name} to role {role}",
                        f"grant references on future external tables in database {db_name} to role {role}",
                        f"grant references on all views in database {db_name} to role {role}",
                        f"grant references on future views in database {db_name} to role {role}",
                    ]
                )
        if self.config.username is not None:
            sqls.append(f"grant role {role} to user {self.config.username}")

        if self.config.include_table_lineage or self.config.include_view_lineage:
            sqls.append(
                f"grant imported privileges on database snowflake to role {role}"
            )

        dry_run_str = "[DRY RUN] " if provision_role_block.dry_run else ""
        for sql in sqls:
            logger.info(f"{dry_run_str} Attempting to run sql {sql}")
            if provision_role_block.dry_run:
                continue
            try:
                engine.execute(sql)
            except Exception as e:
                self.error(logger, "role-grant", f"Exception: {e}")

        self.report.provision_role_success = not provision_role_block.dry_run

    def do_provision_role(self):
        if (
            self.config.provision_role is None
            or self.config.provision_role.enabled is False
        ):
            return

        try:
            self.provision_role_in_progress = True
            self.do_provision_role_internal()
        finally:
            self.provision_role_in_progress = False

    def should_run_ingestion(self) -> bool:
        return (
            self.config.provision_role is None
            or self.config.provision_role.enabled is False
            or self.config.provision_role.run_ingestion
        )

    # Override the base class method.
    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        self.add_config_to_report()

        self.do_provision_role()
        if not self.should_run_ingestion():
            return

        self.inspect_session_metadata()

        self.inspect_role_grants()
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

    def _is_dataset_allowed(
        self, dataset_name: Optional[str], is_view: bool = False
    ) -> bool:
        # View lineages is not supported. Add the allow/deny pattern for that when it is supported.
        if dataset_name is None:
            return True
        dataset_params = dataset_name.split(".")
        if len(dataset_params) != 3:
            return True
        if (
            not self.config.database_pattern.allowed(dataset_params[0])
            or not self.config.schema_pattern.allowed(dataset_params[1])
            or (
                not is_view and not self.config.table_pattern.allowed(dataset_params[2])
            )
            or (is_view and not self.config.view_pattern.allowed(dataset_params[2]))
        ):
            return False
        return True

    def generate_profile_candidates(
        self, inspector: Inspector, threshold_time: Optional[datetime], schema: str
    ) -> Optional[List[str]]:
        if threshold_time is None:
            return None
        db_name = self.current_database
        if self.profile_candidates.get(db_name) is not None:
            #  snowflake profile candidates are available at database level,
            #  no need to regenerate for every schema
            return self.profile_candidates[db_name]
        self.report.profile_if_updated_since = threshold_time
        _profile_candidates = []
        logger.debug(f"Generating profiling candidates for db {db_name}")
        db_rows = inspector.engine.execute(
            text(
                """
select table_catalog, table_schema, table_name
from information_schema.tables
where last_altered >= to_timestamp_ltz({timestamp}, 3) and table_type= 'BASE TABLE'
            """.format(
                    timestamp=round(threshold_time.timestamp() * 1000)
                )
            )
        )

        for db_row in db_rows:
            _profile_candidates.append(
                self.get_identifier(
                    schema=db_row.table_schema,
                    entity=db_row.table_name,
                    inspector=inspector,
                ).lower()
            )

        self.report.profile_candidates[db_name] = _profile_candidates
        self.profile_candidates[db_name] = _profile_candidates
        return _profile_candidates

    # Stateful Ingestion specific overrides
    # NOTE: There is no special state associated with this source yet than what is provided by sql_common.
    def get_platform_instance_id(self) -> str:
        """Overrides the source identifier for stateful ingestion."""
        return self.config.get_account()
