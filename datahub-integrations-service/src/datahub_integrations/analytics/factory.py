import re
import threading
from typing import Any, Callable, Dict, Optional, Tuple, Type

from datahub.configuration.common import ConnectionModel
from datahub.ingestion.graph.client import DataHubGraph
from fastapi import status
from fastapi.exceptions import HTTPException
from pydantic.main import BaseModel

from datahub_integrations.analytics.bigquery.engine import BigQueryAnalyticsEngine
from datahub_integrations.analytics.engine import AnalyticsEngine
from datahub_integrations.analytics.localfs.engine import LocalFSAnalyticsEngine
from datahub_integrations.analytics.s3.connection import S3Connection
from datahub_integrations.analytics.s3.engine import S3AnalyticsEngine, S3EngineConfig
from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection
from datahub_integrations.analytics.snowflake.engine import SnowflakeAnalyticsEngine
from datahub_integrations.graphql.connection import get_connection_json


class AnalyticsEngineLocator(BaseModel):
    """
    Information needed to locate and initialize an analytics engine.
    """

    physical_location: str
    # We can use the connection_urn or connection_name to look up the connection model from the
    # graph
    connection_urn: Optional[str] = None
    connection_name: Optional[str] = None
    # We can also provide the connection model directly
    connection_model: Optional[ConnectionModel] = None
    last_updated_millis: Optional[int] = None


class AnalyticsEngineFactory:
    """Factory class to create the appropriate AnalyticsEngine based on the
    physical location of the dataset.
    """

    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph
        self._engine_creation_lock = threading.Lock()
        self._engine_cache: dict[str, Any] = {}

    def get_connection_model(
        self,
        graph: DataHubGraph,
        locator: AnalyticsEngineLocator,
        connector_model_class: Type[ConnectionModel],
    ) -> Optional[ConnectionModel]:
        if locator.connection_model:
            assert isinstance(locator.connection_model, connector_model_class)
            return locator.connection_model
        if locator.connection_urn:
            connection_json = get_connection_json(graph, locator.connection_urn)
            if connection_json:
                return connector_model_class.parse_obj(connection_json)
        return None

    def _get_or_create_engine(
        self, engine_name: str, create_engine_fn: Callable[[], AnalyticsEngine]
    ) -> AnalyticsEngine:
        if engine_name not in self._engine_cache:
            with self._engine_creation_lock:
                # If someone else took the lock first and already created the engine,
                # we don't need to do anything.
                if engine_name not in self._engine_cache:
                    self._engine_cache[engine_name] = create_engine_fn()
        return self._engine_cache[engine_name]

    def get_engine_with_params(
        self, locator: AnalyticsEngineLocator
    ) -> Tuple[AnalyticsEngine, dict]:
        """Parse the physical location to determine which AnalyticsEngine to use
        and which parameters to use for the preview."""
        physical_location = locator.physical_location
        # if physical location is a Snowflake URL
        # e.g.
        # https://app.snowflake.com/us-west-2/xaa48144/#/data/databases/LONG_TAIL_COMPANIONS/schemas/ADOPTION/table/PET_PROFILES/
        if physical_location.startswith("https://app.snowflake.com/"):
            # extract the Snowflake account, warehouse, database, and schema
            match = re.match(
                r"https://app.snowflake.com/(?P<account>[^/]+)/(?P<warehouse>[^/]+)/(?P<database>[^/]+)/(?P<schema>[^/]+)",
                physical_location,
            )
            if match is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid Snowflake URL",
                )
            account = match.group("account")
            warehouse = match.group("warehouse")
            database = match.group("database")
            schema = match.group("schema")
            connection_model = self.get_connection_model(
                graph=self.graph,
                locator=locator,
                connector_model_class=SnowflakeConnection,
            )
            engine = self._get_or_create_engine(
                "snowflake",
                lambda: SnowflakeAnalyticsEngine(account, graph=self.graph),
            )
            return engine, {
                "warehouse": warehouse,
                "database": database,
                "schema": schema,
                "connection": connection_model,
            }

        # if physical location is a BigQuery URL
        if physical_location.startswith("bigquery://"):
            # extract the BigQuery project, dataset, and table
            match = re.match(
                r"bigquery://(?P<project>[^/]+)/(?P<dataset>[^/]+)/(?P<table>[^/]+)",
                physical_location,
            )
            if match is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid BigQuery URL",
                )
            project = match.group("project")
            dataset = match.group("dataset")
            table = match.group("table")
            # return the BigQueryAnalyticsEngine and the parameters
            return BigQueryAnalyticsEngine(), {
                "project": project,
                "dataset": dataset,
                "table": table,
            }
        # if physical location is a s3 url, return the S3AnalyticsEngine
        if physical_location.startswith("s3://"):
            connection_model = self.get_connection_model(
                graph=self.graph, locator=locator, connector_model_class=S3Connection
            )
            engine = self._get_or_create_engine(
                "s3",
                lambda: S3AnalyticsEngine.create(S3EngineConfig()),
            )

            params: Dict[str, Any] = {
                "bucket": physical_location[5:],
                "connection": connection_model,
            }
            if locator.last_updated_millis:
                params["last_modified_millis"] = locator.last_updated_millis
            return engine, params

        # if physical location is a local file system, return the
        # LocalFSAnalyticsEngine
        if physical_location.startswith("file://"):
            return LocalFSAnalyticsEngine(), {"path": physical_location[7:]}
        # if physical location is not recognized, return 400
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unrecognized physical location",
        )
