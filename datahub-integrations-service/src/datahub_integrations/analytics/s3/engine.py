import pathlib
from typing import Any, Dict, Optional, Tuple

from duckdb import DuckDBPyConnection
from loguru import logger

from datahub_integrations.analytics.cache.file import CacheManager
from datahub_integrations.analytics.config import AnalyticsConfig
from datahub_integrations.analytics.duckdb.engine import (
    DuckDBAnalyticsEngine,
    DuckDBEngineConfig,
)
from datahub_integrations.analytics.s3.connection import S3Connection


class S3EngineConfig(DuckDBEngineConfig):
    """Configuration for the S3 engine"""

    local_cache_path: pathlib.Path = AnalyticsConfig.get_instance().cache_dir


class S3AnalyticsEngine(DuckDBAnalyticsEngine):
    def load_extensions(self, con: DuckDBPyConnection) -> None:
        """Load required DuckDB extensions with error handling"""
        try:
            con.execute("LOAD httpfs; LOAD aws;")
        except Exception as e:
            logger.error(f"Failed to load extensions: {str(e)}")
            raise RuntimeError("Unable to load required DuckDB extensions") from e

    def __init__(self, config: DuckDBEngineConfig) -> None:
        super().__init__(config=config)
        self.cache_manager = CacheManager(config.local_cache_path / "file_cache")

    def close(self) -> None:
        self.cache_manager.close()
        super().close()

    def __exit__(self) -> None:
        self.close()

    @classmethod
    def create(cls, config: DuckDBEngineConfig) -> "S3AnalyticsEngine":
        return cls(config)

    def _get_location_and_connection(self, params: Dict[str, Any]) -> Tuple[str, Any]:
        logger.info(f"params: {params}")
        location = "s3://" + params["bucket"]
        s3_connection: Optional[S3Connection] = params.get("connection")
        last_modified_millis: Optional[int] = params.get("last_modified_millis")
        local_path = self.cache_manager.get_local_path(location, last_modified_millis)
        logger.debug(f"Resolved s3 location {location} to local path {local_path}")
        return local_path, s3_connection

    def _get_credential_query_fragment(
        self, s3_connection: Optional[S3Connection]
    ) -> str:
        if s3_connection and s3_connection.s3_creds:
            logger.info(f"s3_connection: {s3_connection}")
            return (
                f"SET s3_region='{s3_connection.s3_reqion}'; SET s3_access_key_id='{s3_connection.s3_creds.s3_access_key}'; SET s3_secret_access_key='{s3_connection.s3_creds.s3_access_secret}';"
                + f"SET s3_session_token='{s3_connection.s3_creds.s3_session_token}';"
            )
        else:
            # try to load from environment
            return "CALL load_aws_credentials();"

    def get_location_and_credential_query_fragment(
        self, params: Dict[str, Any]
    ) -> Tuple[str, str]:
        location, s3_connection = self._get_location_and_connection(params)
        if location.startswith("s3://"):
            # we only use the credential query fragment if the location is an s3 location
            credential_query_fragment = self._get_credential_query_fragment(
                s3_connection
            )
        else:
            # we assume this is a local cache file, so no need for a credential query fragment
            credential_query_fragment = ""
        return location, credential_query_fragment
