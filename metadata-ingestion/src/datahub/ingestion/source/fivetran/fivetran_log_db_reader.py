import functools
import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.ingestion.source.fivetran.config import (
    DISABLE_COL_LINEAGE_FOR_CONNECTOR_TYPES,
    MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT,
    MAX_JOBS_PER_CONNECTOR_DEFAULT,
    MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT,
    Constant,
    FivetranLogConfig,
    FivetranSourceReport,
    SnowflakeDestinationConfig,
)
from datahub.ingestion.source.fivetran.data_classes import (
    ColumnLineage,
    Connector,
    Job,
    TableLineage,
)
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery
from datahub.ingestion.source.unity.connection import (
    create_workspace_client,
    get_sql_connection_params,
)

logger: logging.Logger = logging.getLogger(__name__)


class FivetranLogDbReader:
    """Database-based reader of the Fivetran Platform Connector log.

    Implements `FivetranConnectorReader` (and `FivetranJobsReader`).
    Reads the Fivetran log tables directly from the destination
    warehouse the Platform Connector delivers them to (Snowflake,
    BigQuery, or Databricks) via SQLAlchemy.
    """

    # Set in `__init__` from the destination config — the actual log
    # warehouse database/catalog/project_id (Snowflake DB, Databricks
    # catalog, BigQuery project_id resolved via `SELECT @@project_id`).
    # Used by `FivetranSource.resolve_destination_details` as the default
    # database for unmapped destinations in `log_database` mode.
    fivetran_log_database: str

    def __init__(
        self,
        fivetran_log_config: FivetranLogConfig,
        report: FivetranSourceReport,
        max_jobs_per_connector: int = MAX_JOBS_PER_CONNECTOR_DEFAULT,
        max_table_lineage_per_connector: int = MAX_TABLE_LINEAGE_PER_CONNECTOR_DEFAULT,
        max_column_lineage_per_connector: int = MAX_COLUMN_LINEAGE_PER_CONNECTOR_DEFAULT,
    ) -> None:
        self.fivetran_log_config = fivetran_log_config
        self._report = report
        self._max_jobs_per_connector = max_jobs_per_connector
        self._max_table_lineage_per_connector = max_table_lineage_per_connector
        self._max_column_lineage_per_connector = max_column_lineage_per_connector
        (
            self.engine,
            self.fivetran_log_query,
            self.fivetran_log_database,
        ) = self._initialize_fivetran_variables()

    @staticmethod
    def _setup_snowflake_engine(
        cfg: SnowflakeDestinationConfig,
        fivetran_log_query: FivetranLogQuery,
    ) -> Engine:
        """Build a Snowflake SQLAlchemy engine and apply USE DATABASE / SET SCHEMA.

        When `cfg.preserve_case` is True, identifiers are passed through
        verbatim — useful for catalog-linked databases or any case-preserving
        Snowflake schema. Otherwise we follow Snowflake's auto-uppercasing
        convention.
        """
        engine = create_engine(
            cfg.get_sql_alchemy_url(),
            **cfg.get_options(),
        )

        # Snowflake auto-uppercases unquoted identifiers. Conventional Snowflake
        # warehouses created via DataHub have always been queried with the
        # ".upper()" path below for backward compatibility. CLDs (and any
        # case-preserving Snowflake schema) need the verbatim path instead.
        if cfg.preserve_case:
            resolved_database = cfg.database
            resolved_schema = cfg.log_schema
        else:
            resolved_database = (
                cfg.database.upper()
                if FivetranLogQuery._is_valid_unquoted_identifier(cfg.database)
                else cfg.database
            )
            resolved_schema = (
                cfg.log_schema.upper()
                if FivetranLogQuery._is_valid_unquoted_identifier(cfg.log_schema)
                else cfg.log_schema
            )

        logger.info(
            f"Using snowflake database: {resolved_database} (original: {cfg.database})"
        )
        engine.execute(fivetran_log_query.use_database(resolved_database))

        logger.info(
            f"Using snowflake schema: {resolved_schema} (original: {cfg.log_schema})"
        )
        fivetran_log_query.set_schema(resolved_schema)

        return engine

    def _initialize_fivetran_variables(
        self,
    ) -> Tuple[Engine, FivetranLogQuery, str]:
        fivetran_log_query = FivetranLogQuery(
            max_jobs_per_connector=self._max_jobs_per_connector,
            max_table_lineage_per_connector=self._max_table_lineage_per_connector,
            max_column_lineage_per_connector=self._max_column_lineage_per_connector,
        )
        destination_platform = self.fivetran_log_config.destination_platform

        # Each branch fails fast with a ConfigurationError if its destination
        # config block is missing. The model_validator on FivetranLogConfig
        # already enforces this in the normal path, but the early-raise pattern
        # protects programmatic callers (tests, model_construct) and prevents
        # an UnboundLocalError on `engine` if the validator is bypassed.
        if destination_platform == "snowflake":
            snowflake_destination_config = (
                self.fivetran_log_config.snowflake_destination_config
            )
            if snowflake_destination_config is None:
                raise ConfigurationError(
                    "snowflake_destination_config is required when "
                    "destination_platform is 'snowflake'."
                )
            engine = self._setup_snowflake_engine(
                snowflake_destination_config, fivetran_log_query
            )
            fivetran_log_database = snowflake_destination_config.database
        elif destination_platform == "bigquery":
            bigquery_destination_config = (
                self.fivetran_log_config.bigquery_destination_config
            )
            if bigquery_destination_config is None:
                raise ConfigurationError(
                    "bigquery_destination_config is required when "
                    "destination_platform is 'bigquery'."
                )
            bq_url = bigquery_destination_config.get_sql_alchemy_url()
            bq_connect_args: Dict[str, Any] = {}
            if bigquery_destination_config.credential is not None:
                separator = "&" if "?" in bq_url else "?"
                bq_url = f"{bq_url}{separator}user_supplied_client=true"
                bq_connect_args["client"] = (
                    bigquery_destination_config.get_bigquery_client()
                )
            engine = create_engine(bq_url, connect_args=bq_connect_args)
            fivetran_log_query.set_schema(bigquery_destination_config.dataset)

            # The "database" should be the BigQuery project name.
            result = engine.execute("SELECT @@project_id").fetchone()
            if result is None:
                raise ValueError("Failed to retrieve BigQuery project ID")
            fivetran_log_database = result[0]
        elif destination_platform == "databricks":
            databricks_destination_config = (
                self.fivetran_log_config.databricks_destination_config
            )
            if databricks_destination_config is None:
                raise ConfigurationError(
                    "databricks_destination_config is required when "
                    "destination_platform is 'databricks'."
                )
            # Pass connect_args (server_hostname, http_path, credentials_provider)
            # so the databricks-sql-connector has valid authentication settings.
            options = {
                **databricks_destination_config.get_options(),
                "connect_args": get_sql_connection_params(
                    create_workspace_client(databricks_destination_config)
                ),
            }
            engine = create_engine(
                databricks_destination_config.get_sql_alchemy_url(
                    databricks_destination_config.catalog
                ),
                **options,
            )
            fivetran_log_query.set_schema(databricks_destination_config.log_schema)
            fivetran_log_database = databricks_destination_config.catalog
        else:
            raise ConfigurationError(
                f"Destination platform '{destination_platform}' is not yet supported."
            )
        return (
            engine,
            fivetran_log_query,
            fivetran_log_database,
        )

    def _query(self, query: str) -> List[Dict[str, Any]]:
        # Automatically transpile snowflake query syntax to the target dialect.
        if self.fivetran_log_config.destination_platform != "snowflake":
            query = sqlglot.parse_one(query, dialect="snowflake").sql(
                dialect=self.fivetran_log_config.destination_platform, pretty=True
            )
        logger.info(f"Executing query: {query}")
        resp = self.engine.execute(query)
        # Convert SQLAlchemy Row objects to plain dicts at the boundary so
        # downstream consumers can treat results as dict-like uniformly.
        return [dict(row) for row in resp]

    def _get_column_lineage_metadata(
        self, connector_ids: List[str]
    ) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
        """
        Returns dict of column lineage metadata with key as (<SOURCE_TABLE_ID>, <DESTINATION_TABLE_ID>).
        Values are raw query-result rows (each a `Dict[str, Any]` produced by `_query`).
        """
        all_column_lineage: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(
            list
        )

        if not connector_ids:
            return dict(all_column_lineage)

        column_lineage_result = self._query(
            self.fivetran_log_query.get_column_lineage_query(
                connector_ids=connector_ids
            )
        )
        for column_lineage in column_lineage_result:
            key = (
                column_lineage[Constant.SOURCE_TABLE_ID],
                column_lineage[Constant.DESTINATION_TABLE_ID],
            )
            all_column_lineage[key].append(column_lineage)
        return dict(all_column_lineage)

    def _get_table_lineage_metadata(
        self, connector_ids: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Returns dict of table lineage metadata with key as 'CONNECTOR_ID'.
        Values are raw query-result rows (each a `Dict[str, Any]` produced by `_query`).
        """
        connectors_table_lineage_metadata: Dict[str, List[Dict[str, Any]]] = (
            defaultdict(list)
        )

        if not connector_ids:
            return dict(connectors_table_lineage_metadata)

        table_lineage_result = self._query(
            self.fivetran_log_query.get_table_lineage_query(connector_ids=connector_ids)
        )
        for table_lineage in table_lineage_result:
            connectors_table_lineage_metadata[
                table_lineage[Constant.CONNECTOR_ID]
            ].append(table_lineage)
        return dict(connectors_table_lineage_metadata)

    def _extract_connector_lineage(
        self,
        table_lineage_result: Optional[List],
        column_lineage_metadata: Dict[Tuple[str, str], List],
    ) -> List[TableLineage]:
        table_lineage_list: List[TableLineage] = []
        if table_lineage_result is None:
            return table_lineage_list
        for table_lineage in table_lineage_result:
            # Join the column lineage into the table lineage.
            column_lineage_result = column_lineage_metadata.get(
                (
                    table_lineage[Constant.SOURCE_TABLE_ID],
                    table_lineage[Constant.DESTINATION_TABLE_ID],
                )
            )
            column_lineage_list: List[ColumnLineage] = []
            if column_lineage_result:
                column_lineage_list = [
                    ColumnLineage(
                        source_column=column_lineage[Constant.SOURCE_COLUMN_NAME],
                        destination_column=column_lineage[
                            Constant.DESTINATION_COLUMN_NAME
                        ],
                    )
                    for column_lineage in column_lineage_result
                ]

            table_lineage_list.append(
                TableLineage(
                    source_table=f"{table_lineage[Constant.SOURCE_SCHEMA_NAME]}.{table_lineage[Constant.SOURCE_TABLE_NAME]}",
                    destination_table=f"{table_lineage[Constant.DESTINATION_SCHEMA_NAME]}.{table_lineage[Constant.DESTINATION_TABLE_NAME]}",
                    column_lineage=column_lineage_list,
                )
            )

        return table_lineage_list

    def _get_all_connector_sync_logs(
        self, syncs_interval: int, connector_ids: List[str]
    ) -> Dict[str, Dict[str, Dict[str, Tuple[float, Optional[str]]]]]:
        sync_logs: Dict[str, Dict[str, Dict[str, Tuple[float, Optional[str]]]]] = {}

        query = self.fivetran_log_query.get_sync_logs_query(
            syncs_interval=syncs_interval,
            connector_ids=connector_ids,
        )

        for row in self._query(query):
            connector_id = row[Constant.CONNECTOR_ID]
            sync_id = row[Constant.SYNC_ID]

            if connector_id not in sync_logs:
                sync_logs[connector_id] = {}

            sync_logs[connector_id][sync_id] = {
                "sync_start": (row["start_time"].timestamp(), None),
                "sync_end": (row["end_time"].timestamp(), row["end_message_data"]),
            }

        return sync_logs

    def _get_jobs_list(
        self, connector_sync_log: Optional[Dict[str, Dict]]
    ) -> List[Job]:
        jobs: List[Job] = []
        if connector_sync_log is None:
            return jobs
        for sync_id in connector_sync_log:
            if len(connector_sync_log[sync_id]) != 2:
                # If both sync-start and sync-end event log not present for this sync that means sync is still in progress
                continue

            message_data = connector_sync_log[sync_id]["sync_end"][1]
            if message_data is None:
                continue
            message_data = json.loads(message_data)
            if isinstance(message_data, str):
                # Sometimes message_data contains json string inside string
                # Ex: '"{\"status\":\"SUCCESSFUL\"}"'
                # Hence, need to do json loads twice.
                message_data = json.loads(message_data)

            jobs.append(
                Job(
                    job_id=sync_id,
                    start_time=round(connector_sync_log[sync_id]["sync_start"][0]),
                    end_time=round(connector_sync_log[sync_id]["sync_end"][0]),
                    status=message_data[Constant.STATUS],
                )
            )
        return jobs

    @functools.lru_cache()
    def _get_users(self) -> Dict[str, str]:
        users = self._query(self.fivetran_log_query.get_users_query())
        if not users:
            return {}
        return {user[Constant.USER_ID]: user[Constant.EMAIL] for user in users}

    def get_user_email(self, user_id: Optional[str]) -> Optional[str]:
        if not user_id:
            return None
        return self._get_users().get(user_id)

    def _fill_connectors_lineage(self, connectors: List[Connector]) -> None:
        # The DB-mode connector list carries `connector_type`, which is
        # consulted for the per-type column-lineage opt-out (Google
        # Sheets has stale column data — see `DISABLE_COL_LINEAGE_FOR_CONNECTOR_TYPES`).
        # `fetch_lineage_for_connectors` doesn't have that information
        # — it's the public batched fetch used by hybrid mode where the
        # caller knows nothing about types — so the type-based opt-out
        # only applies on this in-mode path.
        cll_connector_ids = [
            c.connector_id
            for c in connectors
            if c.connector_type not in DISABLE_COL_LINEAGE_FOR_CONNECTOR_TYPES
        ]
        lineage_by_id = self._fetch_lineage_internal(
            tll_connector_ids=[c.connector_id for c in connectors],
            cll_connector_ids=cll_connector_ids,
        )
        for connector in connectors:
            connector.lineage = lineage_by_id.get(connector.connector_id, [])

    def fetch_lineage_for_connectors(
        self, connector_ids: List[str]
    ) -> Dict[str, List[TableLineage]]:
        """Standalone batched lineage fetch — used by `FivetranLogRestReader`
        in REST-primary hybrid mode as a fallback when Fivetran's Metadata
        API is plan-restricted. Without this, REST mode would degrade to
        table-only lineage even when the DB log has full column lineage
        readily available.

        Type-based column-lineage opt-outs (e.g. Google Sheets) are not
        applied here — the caller doesn't pass connector types. Callers
        that need that opt-out should filter `connector_ids` themselves.
        """
        if not connector_ids:
            return {}
        return self._fetch_lineage_internal(
            tll_connector_ids=connector_ids,
            cll_connector_ids=connector_ids,
        )

    def _fetch_lineage_internal(
        self,
        tll_connector_ids: List[str],
        cll_connector_ids: List[str],
    ) -> Dict[str, List[TableLineage]]:
        """Run the two warehouse queries and stitch the rows into
        TableLineage objects keyed by connector_id.
        """
        table_lineage_metadata = self._get_table_lineage_metadata(tll_connector_ids)
        column_lineage_metadata = self._get_column_lineage_metadata(cll_connector_ids)
        result: Dict[str, List[TableLineage]] = {}
        for connector_id in tll_connector_ids:
            result[connector_id] = self._extract_connector_lineage(
                table_lineage_result=table_lineage_metadata.get(connector_id),
                column_lineage_metadata=column_lineage_metadata,
            )
        return result

    def _fill_connectors_jobs(
        self, connectors: List[Connector], syncs_interval: int
    ) -> None:
        connector_ids = [connector.connector_id for connector in connectors]
        jobs_by_id = self.fetch_jobs_for_connectors(connector_ids, syncs_interval)
        for connector in connectors:
            connector.jobs = jobs_by_id.get(connector.connector_id, [])

    def fetch_jobs_for_connectors(
        self, connector_ids: List[str], syncs_interval: int
    ) -> Dict[str, List[Job]]:
        """Standalone batched job fetch — useful for hybrid REST+DB modes
        where the connector list is already known via REST and we only
        want the sync history from the DB log.

        Returns a mapping of connector_id → list of recent Job entries
        within `syncs_interval` days. Connectors with no recent runs
        will be absent from the dict; callers should default to `[]`.
        """
        if not connector_ids:
            return {}
        sync_logs = self._get_all_connector_sync_logs(
            syncs_interval, connector_ids=connector_ids
        )
        return {
            connector_id: self._get_jobs_list(rows)
            for connector_id, rows in sync_logs.items()
        }

    def get_allowed_connectors_list(
        self,
        connector_patterns: AllowDenyPattern,
        destination_patterns: AllowDenyPattern,
        syncs_interval: int,
    ) -> List[Connector]:
        connectors: List[Connector] = []
        with self._report.metadata_extraction_perf.connectors_metadata_extraction_sec:
            logger.info("Fetching connector list")
            connector_list = self._query(self.fivetran_log_query.get_connectors_query())
            for connector in connector_list:
                connector_id = connector[Constant.CONNECTOR_ID]
                connector_name = connector[Constant.CONNECTOR_NAME]
                # DB mode has always matched `connector_patterns` against
                # `connector_name` only. Keep that contract — adding
                # connector_id matching would silently change which
                # connectors a `deny` pattern catches for existing recipes
                # (e.g. `deny: ["abc123"]` would now also drop a connector
                # whose ID happened to contain "abc123"). REST mode is new
                # and uses connector_id directly since that's the stable
                # identifier the REST API exposes.
                if not connector_patterns.allowed(connector_name):
                    self._report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, dropped due to filter pattern)"
                    )
                    continue
                if not destination_patterns.allowed(
                    destination_id := connector[Constant.DESTINATION_ID]
                ):
                    self._report.report_connectors_dropped(
                        f"{connector_name} (connector_id: {connector_id}, destination_id: {destination_id})"
                    )
                    continue
                connectors.append(
                    Connector(
                        connector_id=connector_id,
                        connector_name=connector_name,
                        connector_type=connector[Constant.CONNECTOR_TYPE_ID],
                        paused=connector[Constant.PAUSED],
                        sync_frequency=connector[Constant.SYNC_FREQUENCY],
                        destination_id=destination_id,
                        user_id=connector[Constant.CONNECTING_USER_ID],
                        lineage=[],  # filled later
                        jobs=[],  # filled later
                    )
                )

        if not connectors:
            # Some of our queries don't work well when there's no connectors, since
            # we push down connector id filters.
            logger.info("No allowed connectors found")
            return []
        logger.info(f"Found {len(connectors)} allowed connectors")

        with self._report.metadata_extraction_perf.connectors_lineage_extraction_sec:
            logger.info("Fetching connector lineage")
            self._fill_connectors_lineage(connectors)
        with self._report.metadata_extraction_perf.connectors_jobs_extraction_sec:
            logger.info("Fetching connector job run history")
            self._fill_connectors_jobs(connectors, syncs_interval)
        return connectors
