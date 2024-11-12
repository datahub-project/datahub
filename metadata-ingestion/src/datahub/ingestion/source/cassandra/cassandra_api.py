from ssl import CERT_NONE, PROTOCOL_TLSv1_2, SSLContext
from typing import List, Optional

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
from datahub.ingestion.source.cassandra.cassandra_utils import (
    CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES,
    CassandraQueries,
)


class CassandraAPIInterface:
    def __init__(self, config: CassandraSourceConfig, report: SourceReport):
        self.config = config
        self.report = report
        self.cassandra_session = self.authenticate()

    def authenticate(self) -> Session:
        """Establish a connection to Cassandra and return the session."""
        try:
            if self.config.cloud:
                cloud_config = self.config.datastax_astra_cloud_config
                assert cloud_config
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

                session: Session = cluster.connect()
                return session

            auth_provider = None
            ssl_context = None
            if self.config.username and self.config.password:
                ssl_context = SSLContext(PROTOCOL_TLSv1_2)
                ssl_context.verify_mode = CERT_NONE
                auth_provider = PlainTextAuthProvider(
                    username=self.config.username, password=self.config.password
                )

            cluster = Cluster(
                [self.config.contact_point],
                port=self.config.port,
                load_balancing_policy=None,
            )

            if auth_provider:
                cluster = Cluster(
                    [self.config.contact_point],
                    port=self.config.port,
                    auth_provider=auth_provider,
                    ssl_context=ssl_context,
                    load_balancing_policy=None,
                )
            session = cluster.connect()
            return session
        except OperationTimedOut as e:
            self.report.warning(
                message="Failed to Autheticate", context=f"{str(e.errors)}", exc=e
            )
            raise
        except DriverException as e:
            self.report.warning(
                message="Failed to Autheticate", context=f"{str(e)}", exc=e
            )
            raise
        except Exception as e:
            self.report.report_failure(
                message="Failed to authenticate to Cassandra", exc=e
            )
            raise

    def get_keyspaces(self) -> List:
        """Fetch all keyspaces."""
        try:
            keyspaces = self.cassandra_session.execute(
                CassandraQueries.GET_KEYSPACES_QUERY
            )
            keyspaces = sorted(
                keyspaces,
                key=lambda k: getattr(
                    k, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"]
                ),
            )
            return keyspaces
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch keyspaces", context=f"{str(e)}", exc=e
            )
            return []
        except Exception:
            self.report.warning(
                message="Failed to fetch keyspaces",
            )
            return []

    def get_tables(self, keyspace_name: str) -> List:
        """Fetch all tables for a given keyspace."""
        try:
            tables = self.cassandra_session.execute(
                CassandraQueries.GET_TABLES_QUERY, [keyspace_name]
            )
            tables = sorted(
                tables,
                key=lambda t: getattr(
                    t, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["table_name"]
                ),
            )
            return tables
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch tables for keyspace",
                context=f"{str(e)}",
                exc=e,
            )
            return []
        except Exception:
            self.report.warning(
                message="Failed to fetch tables for keyspace",
                context=f"{keyspace_name}",
            )
            return []

    def get_columns(self, keyspace_name: str, table_name: str) -> List:
        """Fetch all columns for a given table."""
        try:
            column_infos = self.cassandra_session.execute(
                CassandraQueries.GET_COLUMNS_QUERY, [keyspace_name, table_name]
            )
            column_infos = sorted(column_infos, key=lambda c: c.column_name)
            return column_infos
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch columns for table", context=f"{str(e)}", exc=e
            )
            return []
        except Exception:
            self.report.warning(
                message="Failed to fetch columns for table",
                context=f"{keyspace_name}.{table_name}",
            )
            return []

    def get_views(self, keyspace_name: str) -> List:
        """Fetch all views for a given keyspace."""
        try:
            views = self.cassandra_session.execute(
                CassandraQueries.GET_VIEWS_QUERY, [keyspace_name]
            )
            views = sorted(
                views,
                key=lambda v: getattr(
                    v, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["view_name"]
                ),
            )
            return views
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch views for keyspace", context=f"{str(e)}", exc=e
            )
            return []
        except Exception:
            self.report.warning(
                message="Failed to fetch views for keyspace",
                context=f"{keyspace_name}",
            )
            return []

    def execute(self, query: str, limit: Optional[int]) -> List:
        """Fetch stats for cassandra"""
        try:
            if limit:
                query = query + f" LIMIT {limit}"
            result_set = self.cassandra_session.execute(query).all()
            return result_set
        except DriverException as e:
            self.report.warning(
                message="Failed to fetch stats for keyspace", context=f"{str(e)}", exc=e
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
        if self.cassandra_session:
            self.cassandra_session.shutdown()
