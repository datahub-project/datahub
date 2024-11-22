from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from cassandra import DriverException, OperationTimedOut
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    ProtocolVersion,
    Session,
)

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.cassandra.cassandra_config import CassandraSourceConfig


@dataclass
class CassandraKeyspace:
    keyspace_name: str
    durable_writes: bool
    replication: Dict


@dataclass
class CassandraTable:
    keyspace_name: str
    table_name: str
    bloom_filter_fp_chance: Optional[float]
    caching: Optional[Dict[str, str]]
    comment: Optional[str]
    compaction: Optional[Dict[str, Any]]
    compression: Optional[Dict[str, Any]]
    crc_check_chance: Optional[float]
    dclocal_read_repair_chance: Optional[float]
    default_time_to_live: Optional[int]
    extensions: Optional[Dict[str, Any]]
    gc_grace_seconds: Optional[int]
    max_index_interval: Optional[int]
    memtable_flush_period_in_ms: Optional[int]
    min_index_interval: Optional[int]
    read_repair_chance: Optional[float]
    speculative_retry: Optional[str]


@dataclass
class CassandraColumn:
    keyspace_name: str
    table_name: str
    column_name: str
    type: str
    clustering_order: Optional[str]
    kind: Optional[str]
    position: Optional[int]


@dataclass
class CassandraView(CassandraTable):
    view_name: str
    include_all_columns: Optional[bool]
    where_clause: str = ""


@dataclass
class CassandraEntities:
    keyspaces: List[str] = field(default_factory=list)
    tables: Dict[str, List[str]] = field(
        default_factory=dict
    )  # Maps keyspace -> tables
    columns: Dict[str, List[CassandraColumn]] = field(
        default_factory=dict
    )  # Maps tables -> columns


# - Referencing system_schema: https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useQuerySystem.html#Table3.ColumnsinSystem_SchemaTables-Cassandra3.0 - #
# this keyspace contains details about the cassandra cluster's keyspaces, tables, and columns


class CassandraQueries:
    # get all keyspaces
    GET_KEYSPACES_QUERY = "SELECT * FROM system_schema.keyspaces"
    # get all tables for a keyspace
    GET_TABLES_QUERY = "SELECT * FROM system_schema.tables WHERE keyspace_name = %s"
    # get all columns for a table
    GET_COLUMNS_QUERY = "SELECT * FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s"
    # get all views for a keyspace
    GET_VIEWS_QUERY = "SELECT * FROM system_schema.views WHERE keyspace_name = %s"
    # Row Count
    ROW_COUNT = 'SELECT COUNT(*) AS row_count FROM {}."{}"'
    # Column Count
    COLUMN_COUNT = "SELECT COUNT(*) AS column_count FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}'"


class CassandraAPI:
    def __init__(self, config: CassandraSourceConfig, report: SourceReport):
        self.config = config
        self.report = report
        self._cassandra_session: Optional[Session] = None

    def authenticate(self) -> bool:
        """Establish a connection to Cassandra and return the session."""
        try:
            if self.config.cloud_config:
                cloud_config = self.config.cloud_config
                cluster_cloud_config = {
                    "connect_timeout": cloud_config.connect_timeout,
                    "use_default_tempdir": True,
                    "secure_connect_bundle": cloud_config.secure_connect_bundle,
                }
                profile = ExecutionProfile(request_timeout=cloud_config.request_timeout)
                auth_provider = PlainTextAuthProvider(
                    "token",
                    cloud_config.token,
                )
                cluster = Cluster(
                    cloud=cluster_cloud_config,
                    auth_provider=auth_provider,
                    execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                    protocol_version=ProtocolVersion.V4,
                )

                self._cassandra_session = cluster.connect()
                return True
            if self.config.username and self.config.password:
                auth_provider = PlainTextAuthProvider(
                    username=self.config.username, password=self.config.password
                )
                cluster = Cluster(
                    [self.config.contact_point],
                    port=self.config.port,
                    auth_provider=auth_provider,
                    load_balancing_policy=None,
                )
            else:
                cluster = Cluster(
                    [self.config.contact_point],
                    port=self.config.port,
                    load_balancing_policy=None,
                )

            self._cassandra_session = cluster.connect()
            return True
        except OperationTimedOut as e:
            self.report.failure(
                message="Failed to Authenticate", context=f"{str(e.errors)}", exc=e
            )
            return False
        except DriverException as e:
            self.report.failure(message="Failed to Authenticate", exc=e)
            return False
        except Exception as e:
            self.report.failure(message="Failed to authenticate to Cassandra", exc=e)
            return False

    def get(self, query: str, parameters: Optional[List] = []) -> List:
        if not self._cassandra_session:
            return []

        resp = self._cassandra_session.execute(query, parameters)
        return resp

    def get_keyspaces(self) -> List[CassandraKeyspace]:
        """Fetch all keyspaces."""
        try:
            keyspaces = self.get(CassandraQueries.GET_KEYSPACES_QUERY)
            keyspace_list = [
                CassandraKeyspace(
                    keyspace_name=row.keyspace_name,
                    durable_writes=row.durable_writes,
                    replication=dict(row.replication),
                )
                for row in keyspaces
            ]
            return keyspace_list
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch keyspaces", context=f"{str(e)}", exc=e
            )
            return []
        except Exception as e:
            self.report.warning(message="Failed to fetch keyspaces", exc=e)
            return []

    def get_tables(self, keyspace_name: str) -> List[CassandraTable]:
        """Fetch all tables for a given keyspace."""
        try:
            tables = self.get(CassandraQueries.GET_TABLES_QUERY, [keyspace_name])
            table_list = [
                CassandraTable(
                    keyspace_name=row.keyspace_name,
                    table_name=row.table_name,
                    bloom_filter_fp_chance=row.bloom_filter_fp_chance,
                    caching=dict(row.caching),
                    comment=row.comment,
                    compaction=dict(row.compaction),
                    compression=dict(row.compression),
                    crc_check_chance=row.crc_check_chance,
                    dclocal_read_repair_chance=row.dclocal_read_repair_chance,
                    default_time_to_live=row.default_time_to_live,
                    extensions=dict(row.extensions),
                    gc_grace_seconds=row.gc_grace_seconds,
                    max_index_interval=row.max_index_interval,
                    memtable_flush_period_in_ms=row.memtable_flush_period_in_ms,
                    min_index_interval=row.min_index_interval,
                    read_repair_chance=row.read_repair_chance,
                    speculative_retry=row.speculative_retry,
                )
                for row in tables
            ]
            return table_list
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch tables for keyspace",
                context=f"{str(e)}",
                exc=e,
            )
            return []
        except Exception as e:
            self.report.warning(
                message="Failed to fetch tables for keyspace",
                context=f"{keyspace_name}",
                exc=e,
            )
            return []

    def get_columns(self, keyspace_name: str, table_name: str) -> List[CassandraColumn]:
        """Fetch all columns for a given table."""
        try:
            column_infos = self.get(
                CassandraQueries.GET_COLUMNS_QUERY, [keyspace_name, table_name]
            )
            column_list = [
                CassandraColumn(
                    keyspace_name=row.keyspace_name,
                    table_name=row.table_name,
                    column_name=row.column_name,
                    clustering_order=row.clustering_order,
                    kind=row.kind,
                    position=row.position,
                    type=row.type,
                )
                for row in column_infos
            ]
            return column_list
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch columns for table", context=f"{str(e)}", exc=e
            )
            return []
        except Exception as e:
            self.report.warning(
                message="Failed to fetch columns for table",
                context=f"{keyspace_name}.{table_name}",
                exc=e,
            )
            return []

    def get_views(self, keyspace_name: str) -> List[CassandraView]:
        """Fetch all views for a given keyspace."""
        try:
            views = self.get(CassandraQueries.GET_VIEWS_QUERY, [keyspace_name])
            view_list = [
                CassandraView(
                    table_name=row.base_table_name,
                    keyspace_name=row.keyspace_name,
                    view_name=row.view_name,
                    bloom_filter_fp_chance=row.bloom_filter_fp_chance,
                    caching=dict(row.caching),
                    comment=row.comment,
                    compaction=dict(row.compaction),
                    compression=dict(row.compression),
                    crc_check_chance=row.crc_check_chance,
                    dclocal_read_repair_chance=row.dclocal_read_repair_chance,
                    default_time_to_live=row.default_time_to_live,
                    extensions=dict(row.extensions),
                    gc_grace_seconds=row.gc_grace_seconds,
                    include_all_columns=row.include_all_columns,
                    max_index_interval=row.max_index_interval,
                    memtable_flush_period_in_ms=row.memtable_flush_period_in_ms,
                    min_index_interval=row.min_index_interval,
                    read_repair_chance=row.read_repair_chance,
                    speculative_retry=row.speculative_retry,
                    where_clause=row.where_clause,
                )
                for row in views
            ]
            return view_list
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch views for keyspace", context=f"{str(e)}", exc=e
            )
            return []
        except Exception as e:
            self.report.warning(
                message="Failed to fetch views for keyspace",
                context=f"{keyspace_name}",
                exc=e,
            )
            return []

    def execute(self, query: str, limit: Optional[int] = None) -> List:
        """Fetch stats for cassandra"""
        try:
            if not self._cassandra_session:
                return []
            if limit:
                query = query + f" LIMIT {limit}"
            result_set = self._cassandra_session.execute(query).all()
            return result_set
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch stats for keyspace", context=str(e), exc=e
            )
            return []
        except Exception:
            self.report.warning(
                message="Failed to fetch stats for keyspace",
                context=f"{query}",
            )
            return []

    def close(self):
        """Close the Cassandra session."""
        if self._cassandra_session:
            self._cassandra_session.shutdown()
