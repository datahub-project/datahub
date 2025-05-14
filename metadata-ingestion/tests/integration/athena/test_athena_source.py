from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from sqlalchemy import ARRAY, BIGINT, INTEGER, String
from sqlalchemy_bigquery import STRUCT

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.aws.s3_util import make_s3_urn
from datahub.ingestion.source.sql.athena import AthenaSource
from datahub.utilities.sqlalchemy_type_converter import MapType
from tests.test_helpers import (  # Ensure mce_helpers is available for validation.
    mce_helpers,
)

FROZEN_TIME = "2022-12-15 10:00:00"


@freeze_time(FROZEN_TIME)
def test_athena_source_ingestion(pytestconfig, tmp_path):
    """Test Athena source ingestion and generate MCP JSON file for validation."""
    output_file_name = "athena_mce_output.json"
    golden_file_name = "athena_mce_golden.json"
    test_resources_dir = pytestconfig.rootpath / "tests/integration/athena"

    # Mock dependencies
    with patch.object(
        AthenaSource, "get_inspectors"
    ) as mock_get_inspectors, patch.object(
        AthenaSource, "get_table_properties"
    ) as mock_get_table_properties:
        # Mock engine and inspectors
        mock_inspector = MagicMock()
        mock_get_inspectors.return_value = [mock_inspector]
        mock_engine_instance = MagicMock()
        mock_engine_instance.url.database = ""
        mock_inspector.engine = mock_engine_instance

        # Mock schema and table names
        mock_inspector.get_schema_names.return_value = ["test_schema"]
        mock_inspector.get_table_names.return_value = ["test_table"]
        mock_inspector.get_view_names.return_value = ["test_view_1", "test_view_2"]

        # Mock view definitions
        def mock_get_view_definition(view_name, schema):
            if view_name == "test_view_1":
                return (
                    'CREATE VIEW "test_schema".test_view_1 AS\n'
                    "SELECT *\n"
                    "FROM\n"
                    '  "test_schema"."test_table"'
                )
            elif view_name == "test_view_2":
                return (
                    'CREATE VIEW "test_schema".test_view_2 AS\n'
                    "SELECT employee_id, employee_name, skills\n"
                    "FROM\n"
                    '  "test_schema"."test_view_1"'
                )
            return ""

        mock_inspector.get_view_definition.side_effect = mock_get_view_definition

        mock_inspector.get_columns.return_value = [
            {
                "name": "employee_id",
                "type": String(),
                "nullable": False,
                "default": None,
                "autoincrement": False,
                "comment": "Unique identifier for the employee",
                "dialect_options": {"awsathena_partition": None},
            },
            {
                "name": "annual_salary",
                "type": BIGINT(),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": "Annual salary of the employee in USD",
                "dialect_options": {"awsathena_partition": None},
            },
            {
                "name": "employee_name",
                "type": String(),
                "nullable": False,
                "default": None,
                "autoincrement": False,
                "comment": "Full name of the employee",
                "dialect_options": {"awsathena_partition": None},
            },
            {
                "name": "job_history",
                "type": MapType(
                    String(), STRUCT(year=INTEGER(), company=String(), role=String())
                ),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": "Job history map: year to details (company, role)",
                "dialect_options": {"awsathena_partition": None},
            },
            {
                "name": "department_budgets",
                "type": MapType(String(), BIGINT()),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": "Map of department names to their respective budgets",
                "dialect_options": {"awsathena_partition": None},
            },
            {
                "name": "skills",
                "type": ARRAY(String()),
                "nullable": True,
                "default": None,
                "autoincrement": False,
                "comment": "List of skills possessed by the employee",
                "dialect_options": {"awsathena_partition": None},
            },
        ]
        # Mock table properties
        mock_get_table_properties.return_value = (
            "Test table description",
            {"key": "value", "table_type": "EXTERNAL_TABLE"},
            make_s3_urn("s3://test-bucket/test_table", "PROD"),
        )

        # Define the pipeline configuration
        config_dict = {
            "run_id": "athena-test",
            "source": {
                "type": "athena",
                "config": {
                    "aws_region": "us-east-1",
                    "work_group": "primary",
                    "query_result_location": "s3://athena-query-results/",
                    "catalog_name": "awsdatacatalog",
                    "include_views": True,
                    "include_tables": True,
                    "profiling": {
                        "enabled": False,
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{output_file_name}",
                },
            },
        }

        # Create and run the pipeline
        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()

        # Validate the output with the golden file
        mce_helpers.check_golden_file(
            pytestconfig=pytestconfig,
            output_path=f"{tmp_path}/{output_file_name}",
            golden_path=f"{test_resources_dir}/{golden_file_name}",
        )
