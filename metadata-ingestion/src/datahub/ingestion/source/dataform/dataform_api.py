import json
import logging
import os
import subprocess
from typing import Any, Dict, Optional

from google.cloud.dataform_v1beta1 import DataformClient, ListCompilationResultsRequest

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.dataform.dataform_config import DataformSourceConfig
from datahub.ingestion.source.dataform.dataform_models import (
    DataformAssertion,
    DataformColumn,
    DataformDeclaration,
    DataformEntities,
    DataformOperation,
    DataformTable,
)

# Type aliases for compilation result structure
# Using Any for complex nested structures to avoid overly restrictive unions
EntityInfo = Dict[str, Any]
CompiledGraph = Dict[str, Any]
CompilationResult = Dict[str, Any]

logger = logging.getLogger(__name__)


class DataformAPI:
    """API client for interacting with Dataform (both Cloud and Core)."""

    def __init__(self, config: DataformSourceConfig, report: SourceReport):
        self.config = config
        self.report = report
        self._client: Optional[DataformClient] = None

    def get_client(self) -> Optional[DataformClient]:
        """Get the Dataform client for Cloud mode."""
        if not self.config.is_cloud_mode():
            return None

        if self._client is not None:
            return self._client

        try:
            cloud_config = self.config.cloud_config
            assert cloud_config is not None

            # Set up authentication using GCPCredential pattern
            credentials = None
            if cloud_config.credential:
                # Use the modern GCPCredential approach
                from google.oauth2 import service_account

                credentials = service_account.Credentials.from_service_account_info(
                    cloud_config.credential.to_dict(cloud_config.project_id)
                )
            elif cloud_config.service_account_key_file:
                # Backward compatibility: deprecated approach
                from google.oauth2 import service_account

                credentials = service_account.Credentials.from_service_account_file(
                    cloud_config.service_account_key_file
                )
            elif cloud_config.credentials_json:
                # Backward compatibility: deprecated approach
                import json

                from google.oauth2 import service_account

                credentials_info = json.loads(cloud_config.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info
                )

            self._client = DataformClient(credentials=credentials)
            return self._client

        except Exception as e:
            logger.error(f"Failed to create Dataform client: {e}")
            self.report.failure(
                title="Failed to create Dataform client",
                message=str(e),
            )
            return None

    def get_compilation_result(self) -> Optional[CompilationResult]:
        """Get the compilation result from Dataform."""
        if self.config.is_cloud_mode():
            return self._get_cloud_compilation_result()
        else:
            return self._get_core_compilation_result()

    def _get_cloud_compilation_result(self) -> Optional[CompilationResult]:
        """Get compilation result from Google Cloud Dataform."""
        client = self.get_client()
        if not client:
            return None

        try:
            cloud_config = self.config.cloud_config
            assert cloud_config is not None

            # Build the repository path
            repository_path = client.repository_path(
                cloud_config.project_id,
                cloud_config.region,
                cloud_config.repository_id,
            )

            # Get workspace
            workspace_id = cloud_config.workspace_id or "default"
            client.workspace_path(
                cloud_config.project_id,
                cloud_config.region,
                cloud_config.repository_id,
                workspace_id,
            )

            # Get compilation result
            if cloud_config.compilation_result_id:
                compilation_result_path = client.compilation_result_path(
                    cloud_config.project_id,
                    cloud_config.region,
                    cloud_config.repository_id,
                    cloud_config.compilation_result_id,
                )
                compilation_result = client.get_compilation_result(
                    name=compilation_result_path
                )
            else:
                # Get the latest compilation result
                request = ListCompilationResultsRequest(parent=repository_path)
                compilation_results = client.list_compilation_results(request=request)
                compilation_results_list = list(compilation_results)

                if not compilation_results_list:
                    logger.warning("No compilation results found")
                    return None

                # Get the most recent one
                compilation_result = compilation_results_list[0]

            # Convert to dictionary for easier processing
            return self._compilation_result_to_dict(compilation_result)

        except Exception as e:
            logger.error(f"Failed to get cloud compilation result: {e}")
            self.report.failure(
                title="Failed to get cloud compilation result",
                message=str(e),
            )
            return None

    def _get_core_compilation_result(self) -> Optional[CompilationResult]:
        """Get compilation result from Dataform Core."""
        try:
            core_config = self.config.core_config
            assert core_config is not None

            if core_config.compilation_results_path:
                # Load from existing compilation results file
                with open(core_config.compilation_results_path, "r") as f:
                    return json.load(f)
            else:
                # Compile the project
                return self._compile_dataform_project()

        except Exception as e:
            logger.error(f"Failed to get core compilation result: {e}")
            self.report.failure(
                title="Failed to get core compilation result",
                message=str(e),
            )
            return None

    def _compile_dataform_project(self) -> Optional[CompilationResult]:
        """Compile a Dataform project using the CLI."""
        try:
            core_config = self.config.core_config
            assert core_config is not None

            # Change to project directory
            original_cwd = os.getcwd()
            os.chdir(core_config.project_path)

            try:
                # Build the compile command
                cmd = ["dataform", "compile"]

                if core_config.target_name:
                    cmd.extend(["--target", core_config.target_name])

                # Run the compilation
                subprocess.run(cmd, capture_output=True, text=True, check=True)

                # Parse the output
                # Note: This is a simplified approach - in practice, you might need
                # to parse the actual compiled output files
                compilation_result = {
                    "compiledGraph": {},
                    "dataformCoreVersion": "unknown",
                    "targets": [],
                }

                # Try to load compiled files from the .dataform directory
                compiled_dir = os.path.join(core_config.project_path, ".dataform")
                if os.path.exists(compiled_dir):
                    # Load compiled definitions
                    compilation_result = self._load_compiled_definitions(compiled_dir)

                return compilation_result

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            logger.error(f"Dataform compilation failed: {e.stderr}")
            self.report.failure(
                title="Dataform compilation failed",
                message=e.stderr,
            )
            return None
        except Exception as e:
            logger.error(f"Failed to compile Dataform project: {e}")
            self.report.failure(
                title="Failed to compile Dataform project",
                message=str(e),
            )
            return None

    def _load_compiled_definitions(self, compiled_dir: str) -> CompilationResult:
        """Load compiled definitions from .dataform directory."""
        compilation_result: CompilationResult = {
            "compiledGraph": {
                "tables": {},
                "assertions": {},
                "operations": {},
                "declarations": {},
            }
        }

        # This is a simplified implementation
        # In practice, you would parse the actual compiled files
        # which contain the SQL and metadata for each action

        return compilation_result

    def _compilation_result_to_dict(
        self, compilation_result: object
    ) -> CompilationResult:
        """Convert a Dataform compilation result to a dictionary."""
        # This would convert the protobuf message to a dictionary
        # Implementation depends on the specific structure of the compilation result
        return {
            "compiledGraph": {
                "tables": {},
                "assertions": {},
                "operations": {},
                "declarations": {},
            }
        }

    def extract_entities(
        self, compilation_result: CompilationResult
    ) -> DataformEntities:
        """Extract Dataform entities from compilation result."""
        entities = DataformEntities()

        compiled_graph = compilation_result.get("compiledGraph", {})

        # Extract tables
        if self.config.entities_enabled.tables:
            tables_data = compiled_graph.get("tables", {})
            for table_name, table_info in tables_data.items():
                table = self._parse_table(table_name, table_info)
                if table and self._should_include_table(table):
                    entities.tables.append(table)

        # Extract assertions
        if self.config.entities_enabled.assertions:
            assertions_data = compiled_graph.get("assertions", {})
            for assertion_name, assertion_info in assertions_data.items():
                assertion = self._parse_assertion(assertion_name, assertion_info)
                if assertion:
                    entities.assertions.append(assertion)

        # Extract operations
        if self.config.entities_enabled.operations:
            operations_data = compiled_graph.get("operations", {})
            for operation_name, operation_info in operations_data.items():
                operation = self._parse_operation(operation_name, operation_info)
                if operation:
                    entities.operations.append(operation)

        # Extract declarations
        if self.config.entities_enabled.declarations:
            declarations_data = compiled_graph.get("declarations", {})
            for declaration_name, declaration_info in declarations_data.items():
                declaration = self._parse_declaration(
                    declaration_name, declaration_info
                )
                if declaration:
                    entities.declarations.append(declaration)

        return entities

    def _parse_table(
        self, table_name: str, table_info: EntityInfo
    ) -> Optional[DataformTable]:
        """Parse a table from compilation result."""
        try:
            # Extract basic information
            target: Dict[str, Any] = table_info.get("target", {})

            table = DataformTable(
                name=table_name,
                database=target.get("database"),
                schema_name=target.get("schema", ""),
                type=table_info.get("type", "table"),
                description=table_info.get("description"),
                sql_query=table_info.get("query"),
                materialization_type=table_info.get("type"),
                file_path=table_info.get("fileName"),
                compiled_code=table_info.get("compiledQuery"),  # Add compiled code
                owner=None,  # Will be populated later if available
            )

            # Extract columns
            columns_data = table_info.get("columns", [])
            for column_info in columns_data:
                column = DataformColumn(
                    name=column_info.get("name", ""),
                    type=column_info.get("type", ""),
                    description=column_info.get("description"),
                )
                table.columns.append(column)

            # Extract dependencies
            dependencies = table_info.get("dependencyTargets", [])
            for dep in dependencies:
                if isinstance(dep, dict):
                    dep_name = f"{dep.get('schema', '')}.{dep.get('name', '')}"
                    table.dependencies.append(dep_name)
                else:
                    table.dependencies.append(str(dep))

            # Extract tags
            tags = table_info.get("tags", [])
            table.tags = [tag for tag in tags if self.config.tag_pattern.allowed(tag)]

            return table

        except Exception as e:
            logger.warning(f"Failed to parse table {table_name}: {e}")
            return None

    def _parse_assertion(
        self, assertion_name: str, assertion_info: EntityInfo
    ) -> Optional[DataformAssertion]:
        """Parse an assertion from compilation result."""
        try:
            # Extract target information
            target_info = assertion_info.get("target", {})

            assertion = DataformAssertion(
                name=assertion_name,
                database=target_info.get("database"),
                schema_name=target_info.get("schema", ""),
                table=target_info.get(
                    "name", assertion_name
                ),  # Use assertion name as fallback
                description=assertion_info.get("description"),
                sql_query=assertion_info.get("query", ""),
                file_path=assertion_info.get("fileName"),
            )

            # Extract dependencies
            dependencies = assertion_info.get("dependencyTargets", [])
            for dep in dependencies:
                if isinstance(dep, dict):
                    dep_name = f"{dep.get('schema', '')}.{dep.get('name', '')}"
                    assertion.dependencies.append(dep_name)
                else:
                    assertion.dependencies.append(str(dep))

            # Extract tags
            tags = assertion_info.get("tags", [])
            assertion.tags = [
                tag for tag in tags if self.config.tag_pattern.allowed(tag)
            ]

            return assertion

        except Exception as e:
            logger.warning(f"Failed to parse assertion {assertion_name}: {e}")
            return None

    def _parse_operation(
        self, operation_name: str, operation_info: EntityInfo
    ) -> Optional[DataformOperation]:
        """Parse an operation from compilation result."""
        try:
            operation = DataformOperation(
                name=operation_name,
                description=operation_info.get("description"),
                sql_query=operation_info.get("queries", [""])[0]
                if operation_info.get("queries")
                else "",
                file_path=operation_info.get("fileName"),
            )

            # Extract dependencies
            dependencies = operation_info.get("dependencyTargets", [])
            for dep in dependencies:
                if isinstance(dep, dict):
                    dep_name = f"{dep.get('schema', '')}.{dep.get('name', '')}"
                    operation.dependencies.append(dep_name)
                else:
                    operation.dependencies.append(str(dep))

            # Extract tags
            tags = operation_info.get("tags", [])
            operation.tags = [
                tag for tag in tags if self.config.tag_pattern.allowed(tag)
            ]

            return operation

        except Exception as e:
            logger.warning(f"Failed to parse operation {operation_name}: {e}")
            return None

    def _parse_declaration(
        self, declaration_name: str, declaration_info: EntityInfo
    ) -> Optional[DataformDeclaration]:
        """Parse a declaration from compilation result."""
        try:
            target: Dict[str, Any] = declaration_info.get("target", {})

            declaration = DataformDeclaration(
                name=declaration_name,
                database=target.get("database"),
                schema_name=target.get("schema", ""),
                description=declaration_info.get("description"),
            )

            # Extract columns
            columns_data = declaration_info.get("columns", [])
            for column_info in columns_data:
                column = DataformColumn(
                    name=column_info.get("name", ""),
                    type=column_info.get("type", ""),
                    description=column_info.get("description"),
                )
                declaration.columns.append(column)

            # Extract tags
            tags = declaration_info.get("tags", [])
            declaration.tags = [
                tag for tag in tags if self.config.tag_pattern.allowed(tag)
            ]

            return declaration

        except Exception as e:
            logger.warning(f"Failed to parse declaration {declaration_name}: {e}")
            return None

    def _should_include_table(self, table: DataformTable) -> bool:
        """Check if a table should be included based on patterns."""
        full_name = f"{table.schema_name}.{table.name}"

        if not self.config.table_pattern.allowed(full_name):
            return False

        if not self.config.schema_pattern.allowed(table.schema_name):
            return False

        return True
