import json
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import sqlalchemy
from sqlalchemy import create_engine

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source_config.sql.snowflake import SnowflakeConfig
from datahub.ingestion.source_report.sql.snowflake import SnowflakeReport
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeLineageExtractor:
    def __init__(self, config: SnowflakeConfig, report: SnowflakeReport) -> None:
        self._lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self._external_lineage_map: Optional[Dict[str, Set[str]]] = None
        self.config = config
        self.platform = "snowflake"
        self.report = report

    def _get_upstream_lineage_info(
        self, dataset_name: str
    ) -> Optional[Tuple[UpstreamLineage, Dict[str, str]]]:

        if not self.config.include_table_lineage:
            return None

        if self._lineage_map is None:
            engine = self.get_metadata_engine_for_lineage()
            self._populate_lineage(engine)
            self._populate_view_lineage(engine)
        if self._external_lineage_map is None:
            engine = self.get_metadata_engine_for_lineage()
            self._populate_external_lineage(engine)

        assert self._lineage_map is not None
        assert self._external_lineage_map is not None

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

    def _populate_view_lineage(self, engine: sqlalchemy.engine.Engine) -> None:
        if not self.config.include_view_lineage:
            return
        self._populate_view_upstream_lineage(engine)
        self._populate_view_downstream_lineage(engine)

    def _populate_external_lineage(self, engine: sqlalchemy.engine.Engine) -> None:
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

    def _populate_lineage(self, engine: sqlalchemy.engine.Engine) -> None:
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

    def warn(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_warning(key, reason)
        log.warning(f"{key} => {reason}")

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

    def get_metadata_engine_for_lineage(self) -> sqlalchemy.engine.Engine:

        username = self.config.username
        password = self.config.password
        role = self.config.role

        url = self.config.get_sql_alchemy_url(
            database=None, username=username, password=password, role=role
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
