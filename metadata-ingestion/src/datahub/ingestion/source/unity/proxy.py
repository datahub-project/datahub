"""
Manage the communication with DataBricks Server and provide equivalent dataclasses for dependent modules
"""

import dataclasses
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union, cast
from unittest.mock import patch

import cachetools
import yaml
from cachetools import cached
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    GetMetastoreSummaryResponse,
    MetastoreInfo,
    ModelVersionInfo,
    RegisteredModelInfo,
    SchemaInfo,
    TableInfo,
)
from databricks.sdk.service.files import DownloadResponse, FilesAPI
from databricks.sdk.service.iam import ServicePrincipal as DatabricksServicePrincipal
from databricks.sdk.service.ml import (
    ExperimentsAPI,
)
from databricks.sdk.service.sql import (
    QueryFilter,
    QueryInfo,
    QueryStatementType,
    QueryStatus,
)
from databricks.sdk.service.workspace import ObjectType
from databricks.sql import connect
from databricks.sql.types import Row
from typing_extensions import assert_never

from datahub.api.entities.external.unity_catalog_external_entites import UnityCatalogTag
from datahub.emitter.mce_builder import parse_ts_millis
from datahub.ingestion.source.unity.config import (
    LineageDataSource,
    UsageDataSource,
)
from datahub.ingestion.source.unity.connection import get_sql_connection_params
from datahub.ingestion.source.unity.hive_metastore_proxy import HiveMetastoreProxy
from datahub.ingestion.source.unity.proxy_profiling import (
    UnityCatalogProxyProfilingMixin,
)
from datahub.ingestion.source.unity.proxy_types import (
    ALLOWED_STATEMENT_TYPES,
    Catalog,
    Column,
    CustomCatalogType,
    ExternalTableReference,
    Metastore,
    Model,
    ModelRunDetails,
    ModelSignature,
    ModelVersion,
    Notebook,
    NotebookReference,
    Query,
    Schema,
    ServicePrincipal,
    Table,
    TableReference,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.utilities.file_backed_collections import FileBackedDict

logger: logging.Logger = logging.getLogger(__name__)

# It is enough to keep the cache size to 1, since we only process one catalog at a time
# We need to change this if we want to support parallel processing of multiple catalogs
_MAX_CONCURRENT_CATALOGS = 1


# Import and apply the proxy patch from separate module
try:
    from datahub.ingestion.source.unity.proxy_patch import (
        apply_databricks_proxy_fix,
        mask_proxy_credentials,
    )

    # Apply the fix when the module is imported
    apply_databricks_proxy_fix()
except ImportError as e:
    logger.debug(f"Could not import proxy patch module: {e}")

    # Fallback function for masking credentials
    def mask_proxy_credentials(url: Optional[str]) -> str:
        return "***MASKED***" if url else "None"


@dataclasses.dataclass
class TableInfoWithGeneration(TableInfo):
    generation: Optional[int] = None

    def as_dict(self) -> dict:
        return {**super().as_dict(), "generation": self.generation}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TableInfoWithGeneration":
        table_info: TableInfoWithGeneration = cast(
            TableInfoWithGeneration,
            super().from_dict(d),
        )
        table_info.generation = d.get("generation")
        return table_info


@dataclasses.dataclass
class QueryFilterWithStatementTypes(QueryFilter):
    statement_types: List[QueryStatementType] = dataclasses.field(default_factory=list)

    def as_dict(self) -> dict:
        return {**super().as_dict(), "statement_types": self.statement_types}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "QueryFilterWithStatementTypes":
        v: QueryFilterWithStatementTypes = cast(
            QueryFilterWithStatementTypes,
            super().from_dict(d),
        )
        v.statement_types = d["statement_types"]
        return v


@dataclasses.dataclass
class TableUpstream:
    table_name: str
    source_type: str
    last_updated: Optional[datetime] = None


@dataclasses.dataclass
class ExternalUpstream:
    path: str
    source_type: str
    last_updated: Optional[datetime] = None


@dataclasses.dataclass
class TableLineageInfo:
    upstreams: List[TableUpstream] = dataclasses.field(default_factory=list)
    external_upstreams: List[ExternalUpstream] = dataclasses.field(default_factory=list)
    upstream_notebooks: List[NotebookReference] = dataclasses.field(
        default_factory=list
    )
    downstream_notebooks: List[NotebookReference] = dataclasses.field(
        default_factory=list
    )


class UnityCatalogApiProxy(UnityCatalogProxyProfilingMixin):
    _workspace_client: WorkspaceClient
    _workspace_url: str
    report: UnityCatalogReport
    warehouse_id: str
    _experiments_api: ExperimentsAPI
    _files_api: FilesAPI

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        report: UnityCatalogReport,
        hive_metastore_proxy: Optional[HiveMetastoreProxy] = None,
        lineage_data_source: LineageDataSource = LineageDataSource.AUTO,
        usage_data_source: UsageDataSource = UsageDataSource.AUTO,
        databricks_api_page_size: int = 0,
    ):
        self._workspace_client = workspace_client
        self.warehouse_id = self._workspace_client.config.warehouse_id
        self.report = report
        self.hive_metastore_proxy = hive_metastore_proxy
        self.lineage_data_source = lineage_data_source
        self.usage_data_source = usage_data_source
        self.databricks_api_page_size = databricks_api_page_size
        # Initialize MLflow APIs
        self._experiments_api = ExperimentsAPI(self._workspace_client.api_client)
        self._files_api = FilesAPI(self._workspace_client.api_client)

    def get_run_details(self, run_id: str) -> Optional[ModelRunDetails]:
        """
        Get comprehensive details from an MLflow run.

        Args:
            run_id: The MLflow run ID

        Returns:
            ModelRunDetails object with comprehensive run information
        """
        try:
            run_response = self._experiments_api.get_run(run_id)
            run = run_response.run

            if (
                not run
                or not run.info
                or not run.info.run_id
                or not run.info.experiment_id
            ):
                return None

            # Extract metrics
            metrics: Dict[str, Any] = {}
            if run.data and run.data.metrics:
                for metric in run.data.metrics:
                    if metric.key is not None:
                        metrics[metric.key] = metric.value

            # Extract parameters
            parameters: Dict[str, Any] = {}
            if run.data and run.data.params:
                for param in run.data.params:
                    if param.key is not None:
                        parameters[param.key] = param.value

            # Extract tags
            tags: Dict[str, str] = {}
            if run.data and run.data.tags:
                for tag in run.data.tags:
                    if tag.key is not None and tag.value is not None:
                        tags[tag.key] = tag.value

            return ModelRunDetails(
                run_id=run.info.run_id,
                experiment_id=run.info.experiment_id,
                status=run.info.status.value if run.info.status else None,
                start_time=parse_ts_millis(run.info.start_time),
                end_time=parse_ts_millis(run.info.end_time),
                user_id=run.info.user_id,
                metrics=metrics,
                parameters=parameters,
                tags=tags,
            )
        except Exception as e:
            logger.warning(
                f"Unable to get run details for MLflow experiment, run-id: {run_id}",
                exc_info=True,
            )
            self.report.report_warning(
                title="Unable to get run details for MLflow experiment",
                message="Error while getting run details for MLflow experiment",
                context=f"run-id: {run_id}",
                exc=e,
            )
            return None

    def _extract_signature_from_files_api(
        self, model_version: ModelVersionInfo
    ) -> Optional[ModelSignature]:
        """
        Extract signature from MLmodel file using Databricks FilesAPI.
        Uses the API endpoint: /api/2.0/fs/files/Models/{catalog}/{schema}/{model}/{version}/MLmodel

        Args:
            model_version: Unity Catalog ModelVersionInfo object with catalog_name, schema_name, model_name, version

        Returns:
            ModelSignature if found, None otherwise
        """
        try:
            # Construct file path for FilesAPI
            # The correct path format is: /Models/{catalog}/{schema}/{model}/{version}/MLmodel
            file_path = (
                f"/Models/{model_version.catalog_name}/{model_version.schema_name}/"
                f"{model_version.model_name}/{model_version.version}/MLmodel"
            )

            logger.debug(f"Downloading MLmodel from FilesAPI: {file_path}")

            # Download the file using FilesAPI
            download_response: DownloadResponse = self._files_api.download(
                file_path=file_path
            )

            # Read the file content
            # DownloadResponse.contents is a BinaryIO object
            if download_response and download_response.contents:
                content_stream = download_response.contents

                # Read from the binary stream
                if content_stream:
                    mlmodel_content: str = content_stream.read().decode("utf-8")

                    logger.debug(
                        f"MLmodel file contents from FilesAPI ({file_path}):\n{mlmodel_content}"
                    )

                    # Parse YAML content
                    mlmodel_data = yaml.safe_load(mlmodel_content)

                    # Extract signature from MLmodel YAML
                    if mlmodel_data and "signature" in mlmodel_data:
                        signature_raw = mlmodel_data["signature"]

                        # Signature inputs and outputs are stored as JSON strings in the YAML
                        # Parse them into proper dict/list format
                        signature_data = {}
                        if "inputs" in signature_raw:
                            try:
                                signature_data["inputs"] = json.loads(
                                    signature_raw["inputs"]
                                )
                            except (json.JSONDecodeError, TypeError) as e:
                                logger.debug(f"Failed to parse inputs JSON: {e}")

                        if "outputs" in signature_raw:
                            try:
                                signature_data["outputs"] = json.loads(
                                    signature_raw["outputs"]
                                )
                            except (json.JSONDecodeError, TypeError) as e:
                                logger.debug(f"Failed to parse outputs JSON: {e}")

                        if "params" in signature_raw:
                            try:
                                signature_data["params"] = json.loads(
                                    signature_raw["params"]
                                )
                            except (json.JSONDecodeError, TypeError) as e:
                                logger.debug(f"Failed to parse params JSON: {e}")

                        return ModelSignature(
                            inputs=signature_data.get("inputs"),
                            outputs=signature_data.get("outputs"),
                            parameters=signature_data.get("params"),
                        )
                    else:
                        logger.debug(
                            f"No signature found in MLmodel data from {file_path}"
                        )
                        return None

            return None

        except Exception as e:
            model_name = getattr(model_version, "model_name", "unknown")
            version_num = getattr(model_version, "version", "unknown")
            self.report.report_warning(
                title="Unable to extract signature from MLmodel file",
                message="Error while extracting signature from MLmodel file",
                context=f"model-name: {model_name}, model-version: {version_num}",
                exc=e,
            )
            logger.warning(
                f"Unable to extract signature from MLmodel file, model-name: {model_name}, model-version: {version_num}",
                exc_info=True,
            )
            return None

    def check_basic_connectivity(self) -> bool:
        return bool(
            self._workspace_client.catalogs.list(
                include_browse=True, max_results=self.databricks_api_page_size
            )
        )

    def assigned_metastore(self) -> Optional[Metastore]:
        response = self._workspace_client.metastores.summary()
        return self._create_metastore(response)

    def catalogs(self, metastore: Optional[Metastore]) -> Iterable[Catalog]:
        if self.hive_metastore_proxy:
            yield self.hive_metastore_proxy.hive_metastore_catalog(metastore)

        response = self._workspace_client.catalogs.list(
            include_browse=True, max_results=self.databricks_api_page_size
        )
        if not response:
            logger.info("Catalogs not found")
            return
        for catalog in response:
            optional_catalog = self._create_catalog(metastore, catalog)
            if optional_catalog:
                yield optional_catalog

    def catalog(
        self, catalog_name: str, metastore: Optional[Metastore]
    ) -> Optional[Catalog]:
        response = self._workspace_client.catalogs.get(
            catalog_name, include_browse=True
        )
        if not response:
            logger.info(f"Catalog {catalog_name} not found")
            return None
        optional_catalog = self._create_catalog(metastore, response)
        if optional_catalog:
            return optional_catalog

        return None

    def schemas(self, catalog: Catalog) -> Iterable[Schema]:
        if (
            self.hive_metastore_proxy
            and catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG
        ):
            yield from self.hive_metastore_proxy.hive_metastore_schemas(catalog)
            return
        response = self._workspace_client.schemas.list(
            catalog_name=catalog.name,
            include_browse=True,
            max_results=self.databricks_api_page_size,
        )
        if not response:
            logger.info(f"Schemas not found for catalog {catalog.id}")
            return
        for schema in response:
            optional_schema = self._create_schema(catalog, schema)
            if optional_schema:
                yield optional_schema

    def tables(self, schema: Schema) -> Iterable[Table]:
        if (
            self.hive_metastore_proxy
            and schema.catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG
        ):
            yield from self.hive_metastore_proxy.hive_metastore_tables(schema)
            return
        with patch("databricks.sdk.service.catalog.TableInfo", TableInfoWithGeneration):
            response = self._workspace_client.tables.list(
                catalog_name=schema.catalog.name,
                schema_name=schema.name,
                include_browse=True,
                max_results=self.databricks_api_page_size,
            )
            if not response:
                logger.info(f"Tables not found for schema {schema.id}")
                return
            for table in response:
                try:
                    optional_table = self._create_table(
                        schema, cast(TableInfoWithGeneration, table)
                    )
                    if optional_table:
                        yield optional_table
                except Exception as e:
                    logger.warning(f"Error parsing table: {e}")
                    self.report.report_warning("table-parse", str(e))

    def ml_models(
        self, schema: Schema, max_results: Optional[int] = None
    ) -> Iterable[Model]:
        response = self._workspace_client.registered_models.list(
            catalog_name=schema.catalog.name,
            schema_name=schema.name,
            max_results=max_results,
        )
        for ml_model in response:
            optional_ml_model = self._create_ml_model(schema, ml_model)
            if optional_ml_model:
                yield optional_ml_model

    def ml_model_versions(
        self, ml_model: Model, include_aliases: bool = False
    ) -> Iterable[ModelVersion]:
        response = self._workspace_client.model_versions.list(
            full_name=ml_model.id,
            include_browse=True,
            max_results=self.databricks_api_page_size,
        )
        for version in response:
            if version.version is not None:
                if include_aliases:
                    # to get aliases info, use GET
                    version = self._workspace_client.model_versions.get(
                        ml_model.id, version.version, include_aliases=True
                    )
                optional_ml_model_version = self._create_ml_model_version(
                    ml_model, version
                )
                if optional_ml_model_version:
                    yield optional_ml_model_version

    def service_principals(self) -> Iterable[ServicePrincipal]:
        for principal in self._workspace_client.service_principals.list():
            optional_sp = self._create_service_principal(principal)
            if optional_sp:
                yield optional_sp

    def groups(self):
        """
        fetch the list of the groups belongs to the workspace, using the workspace client
        create the list of group's display name, iterating through the list of groups fetched by the workspace client
        """
        group_list: List[Optional[str]] = []
        for group in self._workspace_client.groups.list():
            group_list.append(group.display_name)
        return group_list

    def workspace_notebooks(self) -> Iterable[Notebook]:
        workspace_objects_iter = self._workspace_client.workspace.list(
            "/", recursive=True, max_results=self.databricks_api_page_size
        )
        for obj in workspace_objects_iter:
            if obj.object_type == ObjectType.NOTEBOOK and obj.object_id and obj.path:
                yield Notebook(
                    id=obj.object_id,
                    path=obj.path,
                    language=obj.language,
                    created_at=parse_ts_millis(obj.created_at),
                    modified_at=parse_ts_millis(obj.modified_at),
                )

    def query_history(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Iterable[Query]:
        """Returns all queries that were run between start_time and end_time with relevant statement_type.

        Raises:
            DatabricksError: If the query history API returns an error.
        """
        filter_by = QueryFilterWithStatementTypes.from_dict(
            {
                "query_start_time_range": {
                    "start_time_ms": start_time.timestamp() * 1000,
                    "end_time_ms": end_time.timestamp() * 1000,
                },
                "statuses": [QueryStatus.FINISHED],
                "statement_types": [typ.value for typ in ALLOWED_STATEMENT_TYPES],
            }
        )
        for query_info in self._query_history(filter_by=filter_by):
            try:
                optional_query = self._create_query(query_info)
                if optional_query:
                    yield optional_query
            except Exception as e:
                logger.warning(f"Error parsing query: {e}")
                self.report.report_warning("query-parse", str(e))

    def _query_history(
        self,
        filter_by: QueryFilterWithStatementTypes,
        include_metrics: bool = False,
    ) -> Iterable[QueryInfo]:
        """Manual implementation of the query_history.list() endpoint.

        Needed because:
        - WorkspaceClient incorrectly passes params as query params, not body
        - It does not paginate correctly -- needs to remove filter_by argument
        Remove if these issues are fixed.
        """
        method = "GET"
        path = "/api/2.0/sql/history/queries"

        body: Dict[str, Any] = {
            "include_metrics": include_metrics,
            "max_results": self.databricks_api_page_size,  # Max batch size
        }

        response: dict = self._workspace_client.api_client.do(  # type: ignore
            method, path, body={**body, "filter_by": filter_by.as_dict()}
        )
        # we use default raw=False(default) in above request, therefore will always get dict
        while True:
            if "res" not in response or not response["res"]:
                return
            for v in response["res"]:
                yield QueryInfo.from_dict(v)
            if not response.get("next_page_token"):  # last page
                return
            response = self._workspace_client.api_client.do(  # type: ignore
                method, path, body={**body, "page_token": response["next_page_token"]}
            )

    def get_query_history_via_system_tables(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> Iterable[Query]:
        """Get query history using system.query.history table.

        This method provides an alternative to the REST API for fetching query history,
        offering better performance and richer data for large query volumes.
        """
        logger.info(
            f"Fetching query history from system.query.history for period: {start_time} to {end_time}"
        )

        allowed_types = [typ.value for typ in ALLOWED_STATEMENT_TYPES]
        statement_type_filter = ", ".join(f"'{typ}'" for typ in allowed_types)

        query = f"""
            SELECT
                statement_id,
                statement_text,
                statement_type,
                start_time,
                end_time,
                executed_by,
                executed_as,
                executed_by_user_id,
                executed_as_user_id
            FROM system.query.history
            WHERE
                start_time >= %s
                AND end_time <= %s
                AND execution_status = 'FINISHED'
                AND statement_type IN ({statement_type_filter})
            ORDER BY start_time
        """

        try:
            rows = self._execute_sql_query(query, (start_time, end_time))
            for row in rows:
                try:
                    yield Query(
                        query_id=row.statement_id,
                        query_text=row.statement_text,
                        statement_type=(
                            QueryStatementType(row.statement_type)
                            if row.statement_type
                            else None
                        ),
                        start_time=row.start_time,
                        end_time=row.end_time,
                        user_id=row.executed_by_user_id,
                        user_name=row.executed_by,
                        executed_as_user_id=row.executed_as_user_id,
                        executed_as_user_name=row.executed_as,
                    )
                except Exception as e:
                    logger.warning(f"Error parsing query from system table: {e}")
                    self.report.report_warning("query-parse-system-table", str(e))
        except Exception as e:
            logger.error(
                f"Error fetching query history from system tables: {e}", exc_info=True
            )
            self.report.report_failure(
                title="Failed to fetch query history from system tables",
                message="Error querying system.query.history table",
                context=f"Query period: {start_time} to {end_time}",
            )

    def _build_datetime_where_conditions(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> str:
        """Build datetime filtering conditions for lineage queries."""
        conditions = []
        if start_time:
            conditions.append(f"event_time >= '{start_time.isoformat()}'")
        if end_time:
            conditions.append(f"event_time <= '{end_time.isoformat()}'")
        return " AND " + " AND ".join(conditions) if conditions else ""

    @cached(cachetools.FIFOCache(maxsize=_MAX_CONCURRENT_CATALOGS))
    def get_catalog_table_lineage_via_system_tables(
        self,
        catalog: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> FileBackedDict[TableLineageInfo]:
        """Get table lineage for all tables in a catalog using system tables."""
        logger.info(f"Fetching table lineage for catalog: {catalog}")
        try:
            additional_where = self._build_datetime_where_conditions(
                start_time, end_time
            )

            query = f"""
                SELECT
                    entity_type, entity_id,
                    source_table_full_name, source_type, source_path,
                    target_table_full_name, target_type,
                    max(event_time) as last_updated
                FROM system.access.table_lineage
                WHERE
                    (target_table_catalog = %s or source_table_catalog = %s)
                    {additional_where}
                GROUP BY
                    entity_type, entity_id,
                    source_table_full_name, source_type, source_path,
                    target_table_full_name, target_type
                """
            rows = self._execute_sql_query(query, [catalog, catalog])

            result_dict: FileBackedDict[TableLineageInfo] = FileBackedDict()
            for row in rows:
                entity_type = row["entity_type"]
                entity_id = row["entity_id"]
                source_full_name = row["source_table_full_name"]
                target_full_name = row["target_table_full_name"]
                source_type = row["source_type"]
                source_path = row["source_path"]
                last_updated = row["last_updated"]

                # Initialize TableLineageInfo for both source and target tables if they're in our catalog
                for table_name in [source_full_name, target_full_name]:
                    if (
                        table_name
                        and table_name.startswith(f"{catalog}.")
                        and table_name not in result_dict
                    ):
                        result_dict[table_name] = TableLineageInfo()

                # Process upstream relationships (target table gets upstreams)
                if target_full_name and target_full_name.startswith(f"{catalog}."):
                    # Handle table upstreams
                    if (
                        source_type in ["TABLE", "VIEW"]
                        and source_full_name != target_full_name
                    ):
                        upstream = TableUpstream(
                            table_name=source_full_name,
                            source_type=source_type,
                            last_updated=last_updated,
                        )
                        result_dict[target_full_name].upstreams.append(upstream)

                    # Handle external upstreams (PATH type)
                    elif source_type == "PATH":
                        external_upstream = ExternalUpstream(
                            path=source_path,
                            source_type=source_type,
                            last_updated=last_updated,
                        )
                        result_dict[target_full_name].external_upstreams.append(
                            external_upstream
                        )

                    # Handle upstream notebooks (notebook -> table)
                    elif entity_type == "NOTEBOOK":
                        notebook_ref = NotebookReference(
                            id=entity_id,
                            last_updated=last_updated,
                        )
                        result_dict[target_full_name].upstream_notebooks.append(
                            notebook_ref
                        )

                # Process downstream relationships (source table gets downstream notebooks)
                if (
                    entity_type == "NOTEBOOK"
                    and source_full_name
                    and source_full_name.startswith(f"{catalog}.")
                ):
                    notebook_ref = NotebookReference(
                        id=entity_id,
                        last_updated=last_updated,
                    )
                    result_dict[source_full_name].downstream_notebooks.append(
                        notebook_ref
                    )

            return result_dict
        except Exception as e:
            logger.warning(
                f"Error getting table lineage for catalog {catalog}: {e}",
                exc_info=True,
            )
            return FileBackedDict()

    @cached(cachetools.FIFOCache(maxsize=_MAX_CONCURRENT_CATALOGS))
    def get_catalog_column_lineage_via_system_tables(
        self,
        catalog: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> FileBackedDict[Dict[str, dict]]:
        """Get column lineage for all tables in a catalog using system tables."""
        logger.info(f"Fetching column lineage for catalog: {catalog}")
        try:
            additional_where = self._build_datetime_where_conditions(
                start_time, end_time
            )

            query = f"""
                SELECT
                    source_table_catalog, source_table_schema, source_table_name, source_column_name, source_type,
                    target_table_schema, target_table_name, target_column_name,
                    max(event_time) as last_updated
                FROM system.access.column_lineage
                WHERE
                    target_table_catalog = %s
                    AND target_table_schema IS NOT NULL
                    AND target_table_name IS NOT NULL
                    AND target_column_name IS NOT NULL
                    AND source_table_catalog IS NOT NULL
                    AND source_table_schema IS NOT NULL
                    AND source_table_name IS NOT NULL
                    AND source_column_name IS NOT NULL
                    {additional_where}
                GROUP BY
                    source_table_catalog, source_table_schema, source_table_name, source_column_name, source_type,
                    target_table_schema, target_table_name, target_column_name
                """
            rows = self._execute_sql_query(query, [catalog])

            result_dict: FileBackedDict[Dict[str, dict]] = FileBackedDict()
            for row in rows:
                result_dict.setdefault(row["target_table_schema"], {}).setdefault(
                    row["target_table_name"], {}
                ).setdefault(row["target_column_name"], []).append(
                    # make fields look like the response from the older HTTP API
                    {
                        "catalog_name": row["source_table_catalog"],
                        "schema_name": row["source_table_schema"],
                        "table_name": row["source_table_name"],
                        "name": row["source_column_name"],
                        "last_updated": row["last_updated"],
                    }
                )

            return result_dict
        except Exception as e:
            logger.warning(
                f"Error getting column lineage for catalog {catalog}: {e}",
                exc_info=True,
            )
            return FileBackedDict()

    def list_lineages_by_table_via_http_api(
        self, table_name: str, include_entity_lineage: bool
    ) -> dict:
        """List table lineage by table name."""
        logger.debug(f"Getting table lineage for {table_name}")
        return self._workspace_client.api_client.do(  # type: ignore
            method="GET",
            path="/api/2.0/lineage-tracking/table-lineage",
            body={
                "table_name": table_name,
                "include_entity_lineage": include_entity_lineage,
            },
        )

    def list_lineages_by_column_via_http_api(
        self, table_name: str, column_name: str
    ) -> list:
        """List column lineage by table name and column name."""
        logger.debug(f"Getting column lineage for {table_name}.{column_name}")
        try:
            return (
                self._workspace_client.api_client.do(  # type: ignore
                    "GET",
                    "/api/2.0/lineage-tracking/column-lineage",
                    body={"table_name": table_name, "column_name": column_name},
                ).get("upstream_cols")
                or []
            )
        except Exception as e:
            logger.warning(
                f"Error getting column lineage on table {table_name}, column {column_name}: {e}",
                exc_info=True,
            )
            return []

    def table_lineage(
        self,
        table: Table,
        include_entity_lineage: bool,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> None:
        if table.schema.catalog.type == CustomCatalogType.HIVE_METASTORE_CATALOG:
            # Lineage is not available for Hive Metastore Tables.
            return None

        try:
            # Determine lineage data source based on config
            use_system_tables = False
            if self.lineage_data_source == LineageDataSource.SYSTEM_TABLES:
                use_system_tables = True
            elif self.lineage_data_source == LineageDataSource.API:
                use_system_tables = False
            elif self.lineage_data_source == LineageDataSource.AUTO:
                # Use the newer system tables if we have a SQL warehouse, otherwise fall back
                # to the older (and slower) HTTP API.
                use_system_tables = bool(self.warehouse_id)
            else:
                assert_never(self.lineage_data_source)

            if use_system_tables:
                self._process_system_table_lineage(table, start_time, end_time)
            else:
                self._process_table_lineage_via_http_api(table, include_entity_lineage)
        except Exception as e:
            logger.warning(
                f"Error getting lineage on table {table.ref}: {e}", exc_info=True
            )

    def _process_system_table_lineage(
        self,
        table: Table,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> None:
        """Process table lineage using system.access.table_lineage table."""
        catalog_lineage = self.get_catalog_table_lineage_via_system_tables(
            table.ref.catalog, start_time, end_time
        )
        table_full_name = table.ref.qualified_table_name

        lineage_info = catalog_lineage.get(table_full_name, TableLineageInfo())

        # Process table upstreams
        for upstream in lineage_info.upstreams:
            upstream_table_name = upstream.table_name
            # Parse catalog.schema.table format
            parts = upstream_table_name.split(".")
            if len(parts) == 3:
                catalog_name, schema_name, table_name = parts[0], parts[1], parts[2]
                table_ref = TableReference(
                    metastore=table.schema.catalog.metastore.id
                    if table.schema.catalog.metastore
                    else None,
                    catalog=catalog_name,
                    schema=schema_name,
                    table=table_name,
                    last_updated=upstream.last_updated,
                )
                table.upstreams[table_ref] = {}
            else:
                logger.warning(
                    f"Unexpected upstream table format: {upstream_table_name} for table {table_full_name}"
                )
                continue

        # Process external upstreams
        for external_upstream in lineage_info.external_upstreams:
            external_ref = ExternalTableReference(
                path=external_upstream.path,
                has_permission=True,
                name=None,
                type=None,
                storage_location=external_upstream.path,
                last_updated=external_upstream.last_updated,
            )
            table.external_upstreams.add(external_ref)

        # Process upstream notebook lineage
        for notebook_ref in lineage_info.upstream_notebooks:
            existing_ref = table.upstream_notebooks.get(notebook_ref.id)
            if existing_ref is None or (
                notebook_ref.last_updated
                and existing_ref.last_updated
                and notebook_ref.last_updated > existing_ref.last_updated
            ):
                table.upstream_notebooks[notebook_ref.id] = notebook_ref

        # Process downstream notebook lineage
        for notebook_ref in lineage_info.downstream_notebooks:
            existing_ref = table.downstream_notebooks.get(notebook_ref.id)
            if existing_ref is None or (
                notebook_ref.last_updated
                and existing_ref.last_updated
                and notebook_ref.last_updated > existing_ref.last_updated
            ):
                table.downstream_notebooks[notebook_ref.id] = notebook_ref

    def _process_table_lineage_via_http_api(
        self, table: Table, include_entity_lineage: bool
    ) -> None:
        """Process table lineage using the HTTP API (legacy fallback)."""
        response: dict = self.list_lineages_by_table_via_http_api(
            table_name=table.ref.qualified_table_name,
            include_entity_lineage=include_entity_lineage,
        )

        for item in response.get("upstreams") or []:
            if "tableInfo" in item:
                table_ref = TableReference.create_from_lineage(
                    item["tableInfo"], table.schema.catalog.metastore
                )
                if table_ref:
                    table.upstreams[table_ref] = {}
            elif "fileInfo" in item:
                external_ref = ExternalTableReference.create_from_lineage(
                    item["fileInfo"]
                )
                if external_ref:
                    table.external_upstreams.add(external_ref)

            for notebook in item.get("notebookInfos") or []:
                notebook_ref = NotebookReference(
                    id=notebook["notebook_id"],
                )
                table.upstream_notebooks[notebook_ref.id] = notebook_ref

        for item in response.get("downstreams") or []:
            for notebook in item.get("notebookInfos") or []:
                notebook_ref = NotebookReference(
                    id=notebook["notebook_id"],
                )
                table.downstream_notebooks[notebook_ref.id] = notebook_ref

    def get_column_lineage(
        self,
        table: Table,
        column_names: List[str],
        *,
        max_workers: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> None:
        try:
            # Determine lineage data source based on config
            use_system_tables = False
            if self.lineage_data_source == LineageDataSource.SYSTEM_TABLES:
                use_system_tables = True
            elif self.lineage_data_source == LineageDataSource.API:
                use_system_tables = False
            elif self.lineage_data_source == LineageDataSource.AUTO:
                # Use the newer system tables if we have a SQL warehouse, otherwise fall back
                # to the older (and slower) HTTP API.
                use_system_tables = bool(self.warehouse_id)
            else:
                assert_never(self.lineage_data_source)

            if use_system_tables:
                lineage = (
                    self.get_catalog_column_lineage_via_system_tables(
                        table.ref.catalog, start_time, end_time
                    )
                    .get(table.ref.schema, {})
                    .get(table.ref.table, {})
                )
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.list_lineages_by_column_via_http_api,
                            table.ref.qualified_table_name,
                            column_name,
                        )
                        for column_name in column_names
                    ]
                lineage = {
                    column_name: future.result()
                    for column_name, future in zip(column_names, futures, strict=False)
                }

            for column_name in column_names:
                for item in lineage.get(column_name) or []:
                    table_ref = TableReference.create_from_lineage(
                        item,
                        table.schema.catalog.metastore,
                    )
                    if table_ref:
                        table.upstreams.setdefault(table_ref, {}).setdefault(
                            column_name, []
                        ).append(item["name"])

        except Exception as e:
            logger.warning(
                f"Error getting column lineage on table {table.ref}: {e}",
                exc_info=True,
            )

    @staticmethod
    def _escape_sequence(value: str) -> str:
        return value.replace(" ", "_")

    @staticmethod
    def _create_metastore(
        obj: Union[GetMetastoreSummaryResponse, MetastoreInfo],
    ) -> Optional[Metastore]:
        if not obj.name:
            return None
        return Metastore(
            name=obj.name,
            id=UnityCatalogApiProxy._escape_sequence(obj.name),
            global_metastore_id=obj.global_metastore_id,
            metastore_id=obj.metastore_id,
            owner=obj.owner,
            region=obj.region,
            cloud=obj.cloud,
            comment=None,
        )

    def _create_catalog(
        self, metastore: Optional[Metastore], obj: CatalogInfo
    ) -> Optional[Catalog]:
        if not obj.name:
            self.report.num_catalogs_missing_name += 1
            return None
        catalog_name = self._escape_sequence(obj.name)
        return Catalog(
            name=obj.name,
            id=f"{metastore.id}.{catalog_name}" if metastore else catalog_name,
            metastore=metastore,
            comment=obj.comment,
            owner=obj.owner,
            type=obj.catalog_type,
        )

    def _create_schema(self, catalog: Catalog, obj: SchemaInfo) -> Optional[Schema]:
        if not obj.name:
            self.report.num_schemas_missing_name += 1
            return None
        return Schema(
            name=obj.name,
            id=f"{catalog.id}.{self._escape_sequence(obj.name)}",
            catalog=catalog,
            comment=obj.comment,
            owner=obj.owner,
        )

    def _create_column(self, table_id: str, obj: ColumnInfo) -> Optional[Column]:
        if not obj.name:
            self.report.num_columns_missing_name += 1
            return None
        return Column(
            name=obj.name,
            id=f"{table_id}.{self._escape_sequence(obj.name)}",
            type_text=obj.type_text or "",
            type_name=obj.type_name,
            type_scale=obj.type_scale,
            type_precision=obj.type_precision,
            position=obj.position,
            nullable=obj.nullable,
            comment=obj.comment,
        )

    def _create_table(
        self, schema: Schema, obj: TableInfoWithGeneration
    ) -> Optional[Table]:
        if not obj.name:
            self.report.num_tables_missing_name += 1
            return None
        table_id = f"{schema.id}.{self._escape_sequence(obj.name)}"
        return Table(
            name=obj.name,
            id=table_id,
            table_type=obj.table_type,
            schema=schema,
            storage_location=obj.storage_location,
            data_source_format=obj.data_source_format,
            columns=(
                list(self._extract_columns(obj.columns, table_id))
                if obj.columns
                else []
            ),
            view_definition=obj.view_definition or None,
            properties=obj.properties or {},
            owner=obj.owner,
            generation=obj.generation,
            created_at=(parse_ts_millis(obj.created_at) if obj.created_at else None),
            created_by=obj.created_by,
            updated_at=(parse_ts_millis(obj.updated_at) if obj.updated_at else None),
            updated_by=obj.updated_by,
            table_id=obj.table_id,
            comment=obj.comment,
        )

    def _extract_columns(
        self, columns: List[ColumnInfo], table_id: str
    ) -> Iterable[Column]:
        for column in columns:
            optional_column = self._create_column(table_id, column)
            if optional_column:
                yield optional_column

    def _create_ml_model(
        self, schema: Schema, obj: RegisteredModelInfo
    ) -> Optional[Model]:
        if not obj.name or not obj.full_name:
            self.report.num_ml_models_missing_name += 1
            return None
        return Model(
            id=obj.full_name,
            name=obj.name,
            description=obj.comment,
            schema_name=schema.name,
            catalog_name=schema.catalog.name,
            created_at=parse_ts_millis(obj.created_at),
            updated_at=parse_ts_millis(obj.updated_at),
        )

    def _create_ml_model_version(
        self, model: Model, obj: ModelVersionInfo
    ) -> Optional[ModelVersion]:
        if obj.version is None:
            return None

        aliases = []
        if obj.aliases:
            for alias in obj.aliases:
                if alias.alias_name:
                    aliases.append(alias.alias_name)

        run_details: Optional[ModelRunDetails] = None
        # Fetch run details if run_id exists
        if obj.run_id:
            run_details = self.get_run_details(obj.run_id)

        # Extract signature separately from Files API
        signature: Optional[ModelSignature] = self._extract_signature_from_files_api(
            obj
        )

        return ModelVersion(
            id=f"{model.id}_{obj.version}",
            name=f"{model.name}_{obj.version}",
            model=model,
            version=str(obj.version),
            aliases=aliases,
            description=obj.comment,
            created_at=parse_ts_millis(obj.created_at),
            updated_at=parse_ts_millis(obj.updated_at),
            created_by=obj.created_by,
            run_details=run_details,
            signature=signature,
        )

    def _create_service_principal(
        self, obj: DatabricksServicePrincipal
    ) -> Optional[ServicePrincipal]:
        if not obj.display_name or not obj.application_id:
            return None
        return ServicePrincipal(
            id=f"{obj.id}.{self._escape_sequence(obj.display_name)}",
            display_name=obj.display_name,
            application_id=obj.application_id,
            active=obj.active,
        )

    def _create_query(self, info: QueryInfo) -> Optional[Query]:
        if (
            not info.query_text
            or not info.query_start_time_ms
            or not info.query_end_time_ms
        ):
            self.report.num_queries_missing_info += 1
            return None
        return Query(
            query_id=info.query_id,
            query_text=info.query_text,
            statement_type=info.statement_type,
            start_time=parse_ts_millis(info.query_start_time_ms),
            end_time=parse_ts_millis(info.query_end_time_ms),
            user_id=info.user_id,
            user_name=info.user_name,
            executed_as_user_id=info.executed_as_user_id,
            executed_as_user_name=info.executed_as_user_name,
        )

    def _execute_sql_query(self, query: str, params: Sequence[Any] = ()) -> List[Row]:
        """Execute SQL query using databricks-sql connector for better performance"""
        logger.debug(f"Executing SQL query with {len(params)} parameters")
        if logger.isEnabledFor(logging.DEBUG):
            # Only log full query in debug mode to avoid performance overhead
            logger.debug(f"Full SQL query: {query}")
            if params:
                logger.debug(f"Query parameters: {params}")

        # Check if warehouse_id is available for SQL operations
        if not self.warehouse_id:
            self.report.report_warning(
                "Cannot execute SQL query",
                "warehouse_id is not configured. SQL operations require a valid warehouse_id to be set in the Unity Catalog configuration",
            )
            logger.warning(
                "Cannot execute SQL query: warehouse_id is not configured. "
                "SQL operations require a valid warehouse_id to be set in the Unity Catalog configuration."
            )
            return []

        # Log connection parameters (with masked token)
        sql_connection_params = get_sql_connection_params(self._workspace_client)
        logger.debug(f"Using connection parameters: {sql_connection_params}")

        # Log proxy environment variables that affect SQL connections
        proxy_env_debug = {}
        for var in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
            value = os.environ.get(var)
            if value:
                proxy_env_debug[var] = mask_proxy_credentials(value)

        if proxy_env_debug:
            logger.debug(
                f"SQL connection will use proxy environment variables: {proxy_env_debug}"
            )
        else:
            logger.debug("No proxy environment variables detected for SQL connection")

        try:
            with (
                connect(**sql_connection_params) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(query, list(params))
                rows = cursor.fetchall()
                logger.debug(
                    f"SQL query executed successfully, returned {len(rows)} rows"
                )
                return rows

        except Exception as e:
            logger.warning(f"Failed to execute SQL query: {e}", exc_info=True)
            if logger.isEnabledFor(logging.DEBUG):
                # Only log failed query details in debug mode for security
                logger.debug(f"SQL query that failed: {query}")
                logger.debug(f"SQL query parameters: {params}")

            # Check if this might be a proxy-related error
            error_str = str(e).lower()
            if any(
                proxy_keyword in error_str
                for proxy_keyword in [
                    "proxy",
                    "407",
                    "authentication required",
                    "tunnel",
                    "connect",
                ]
            ):
                logger.error(
                    "SQL query failure appears to be proxy-related. "
                    "Please check proxy configuration and authentication. "
                    f"Proxy environment variables detected: {list(proxy_env_debug.keys())}"
                )

            return []

    @cached(cachetools.FIFOCache(maxsize=_MAX_CONCURRENT_CATALOGS))
    def get_schema_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching schema tags for catalog: `{catalog}`")

        query = f"SELECT * FROM `{catalog}`.information_schema.schema_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, schema_name, tag_name, tag_value = row
            schema_key = f"{catalog_name}.{schema_name}"

            if schema_key not in result_dict:
                result_dict[schema_key] = []

            result_dict[schema_key].append(
                UnityCatalogTag(key=tag_name, value=tag_value)
            )

        return result_dict

    @cached(cachetools.FIFOCache(maxsize=_MAX_CONCURRENT_CATALOGS))
    def get_catalog_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching table tags for catalog: `{catalog}`")

        query = f"SELECT * FROM `{catalog}`.information_schema.catalog_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, tag_name, tag_value = row

            if catalog_name not in result_dict:
                result_dict[catalog_name] = []

            result_dict[catalog_name].append(
                UnityCatalogTag(key=tag_name, value=tag_value)
            )

        return result_dict

    @cached(cachetools.FIFOCache(maxsize=_MAX_CONCURRENT_CATALOGS))
    def get_table_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching table tags for catalog: `{catalog}`")

        query = f"SELECT * FROM `{catalog}`.information_schema.table_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, schema_name, table_name, tag_name, tag_value = row
            table_key = f"{catalog_name}.{schema_name}.{table_name}"

            if table_key not in result_dict:
                result_dict[table_key] = []

            result_dict[table_key].append(
                UnityCatalogTag(key=tag_name, value=tag_value if tag_value else None)
            )

        return result_dict

    @cached(cachetools.FIFOCache(maxsize=_MAX_CONCURRENT_CATALOGS))
    def get_column_tags(self, catalog: str) -> Dict[str, List[UnityCatalogTag]]:
        """Optimized version using databricks-sql"""
        logger.info(f"Fetching column tags for catalog: `{catalog}`")

        query = f"SELECT * FROM `{catalog}`.information_schema.column_tags"
        rows = self._execute_sql_query(query)

        result_dict: Dict[str, List[UnityCatalogTag]] = {}

        for row in rows:
            catalog_name, schema_name, table_name, column_name, tag_name, tag_value = (
                row
            )
            column_key = f"{catalog_name}.{schema_name}.{table_name}.{column_name}"

            if column_key not in result_dict:
                result_dict[column_key] = []

            result_dict[column_key].append(
                UnityCatalogTag(key=tag_name, value=tag_value if tag_value else None)
            )

        return result_dict
