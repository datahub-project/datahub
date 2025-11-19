import datetime
from functools import partial
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.fivetran.config import (
    BigQueryDestinationConfig,
    DatabricksDestinationConfig,
    FivetranLogConfig,
    FivetranSourceConfig,
    PlatformDetail,
    SnowflakeDestinationConfig,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.fivetran.fivetran_log_api import FivetranLogAPI
from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery
from datahub.testing import mce_helpers

FROZEN_TIME = "2022-06-07 17:00:00"

default_connector_query_results = [
    {
        "connection_id": "calendar_elected",
        "connecting_user_id": "reapply_phone",
        "connector_type_id": "postgres",
        "connection_name": "postgres",
        "paused": False,
        "sync_frequency": 1440,
        "destination_id": "interval_unconstitutional",
    },
    {
        "connection_id": "my_confluent_cloud_connector_id",
        "connecting_user_id": "reapply_phone",
        "connector_type_id": "confluent_cloud",
        "connection_name": "confluent_cloud",
        "paused": False,
        "sync_frequency": 1440,
        "destination_id": "my_confluent_cloud_connector_id",
    },
]


def default_query_results(
    query, connector_query_results=default_connector_query_results
):
    fivetran_log_query = FivetranLogQuery()
    # For Snowflake, valid unquoted identifiers are uppercased
    # "test_database" -> "TEST_DATABASE", "test" -> "TEST"
    fivetran_log_query.set_schema("TEST")
    if query == fivetran_log_query.use_database("TEST_DATABASE"):
        return []
    if query == fivetran_log_query.get_connectors_query():
        return connector_query_results
    elif query == fivetran_log_query.get_table_lineage_query(
        connector_ids=["calendar_elected", "my_confluent_cloud_connector_id"]
    ):
        return [
            {
                "connection_id": "calendar_elected",
                "source_table_id": "10040",
                "source_table_name": "employee",
                "source_schema_name": "public",
                "destination_table_id": "7779",
                "destination_table_name": "employee",
                "destination_schema_name": "postgres_public",
            },
            {
                "connection_id": "calendar_elected",
                "source_table_id": "10041",
                "source_table_name": "company",
                "source_schema_name": "public",
                "destination_table_id": "7780",
                "destination_table_name": "company",
                "destination_schema_name": "postgres_public",
            },
            {
                "connection_id": "my_confluent_cloud_connector_id",
                "source_table_id": "10042",
                "source_table_name": "my-source-topic",
                "source_schema_name": "confluent_cloud",
                "destination_table_id": "7781",
                "destination_table_name": "my-destination-topic",
                "destination_schema_name": "confluent_cloud",
            },
        ]
    elif query == fivetran_log_query.get_column_lineage_query(
        connector_ids=["calendar_elected", "my_confluent_cloud_connector_id"]
    ):
        return [
            {
                "source_table_id": "10040",
                "destination_table_id": "7779",
                "source_column_name": "id",
                "destination_column_name": "id",
            },
            {
                "source_table_id": "10040",
                "destination_table_id": "7779",
                "source_column_name": "name",
                "destination_column_name": "name",
            },
            {
                "source_table_id": "10041",
                "destination_table_id": "7780",
                "source_column_name": "id",
                "destination_column_name": "id",
            },
            {
                "source_table_id": "10041",
                "destination_table_id": "7780",
                "source_column_name": "name",
                "destination_column_name": "name",
            },
        ]
    elif query == fivetran_log_query.get_users_query():
        return [
            {
                "user_id": "reapply_phone",
                "given_name": "Shubham",
                "family_name": "Jagtap",
                "email": "abc.xyz@email.com",
            }
        ]
    elif query == fivetran_log_query.get_sync_logs_query(
        syncs_interval=7,
        connector_ids=["calendar_elected", "my_confluent_cloud_connector_id"],
    ):
        return [
            {
                "connection_id": "calendar_elected",
                "sync_id": "4c9a03d6-eded-4422-a46a-163266e58243",
                "start_time": datetime.datetime(2023, 9, 20, 6, 37, 32, 606000),
                "end_time": datetime.datetime(2023, 9, 20, 6, 38, 5, 56000),
                "end_message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
            },
            {
                "connection_id": "calendar_elected",
                "sync_id": "f773d1e9-c791-48f4-894f-8cf9b3dfc834",
                "start_time": datetime.datetime(2023, 10, 3, 14, 35, 30, 345000),
                "end_time": datetime.datetime(2023, 10, 3, 14, 35, 31, 512000),
                "end_message_data": '"{\\"reason\\":\\"Sync has been cancelled because of a user action in the dashboard.Standard Config updated.\\",\\"status\\":\\"CANCELED\\"}"',
            },
            {
                "connection_id": "calendar_elected",
                "sync_id": "63c2fc85-600b-455f-9ba0-f576522465be",
                "start_time": datetime.datetime(2023, 10, 3, 14, 35, 55, 401000),
                "end_time": datetime.datetime(2023, 10, 3, 14, 36, 29, 678000),
                "end_message_data": '"{\\"reason\\":\\"java.lang.RuntimeException: FATAL: too many connections for role \\\\\\"hxwraqld\\\\\\"\\",\\"taskType\\":\\"reconnect\\",\\"status\\":\\"FAILURE_WITH_TASK\\"}"',
            },
            {
                "connection_id": "my_confluent_cloud_connector_id",
                "sync_id": "d9a03d6-eded-4422-a46a-163266e58244",
                "start_time": datetime.datetime(2023, 9, 20, 6, 37, 32, 606000),
                "end_time": datetime.datetime(2023, 9, 20, 6, 38, 5, 56000),
                "end_message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
            },
        ]
    # Unreachable code
    raise Exception(f"Unknown query {query}")


# Test cases with different schema names that might cause issues
SCHEMA_NAME_TEST_CASES = [
    "fivetran_logs",  # Normal case
    "fivetran-logs",  # Hyphen
    "fivetran_logs_123",  # Underscore and numbers
    "fivetran.logs",  # Dot
    "fivetran logs",  # Space
    "fivetran'logs",  # Single quote
    'fivetran"logs',  # Double quote
    "fivetran`logs",  # Backtick
    "fivetran-logs-123",  # Multiple hyphens
    "fivetran_logs-123",  # Mixed underscore and hyphen
    "fivetran-logs_123",  # Mixed hyphen and underscore
    "fivetran.logs-123",  # Mixed dot and hyphen
    "fivetran logs 123",  # Multiple spaces
    "fivetran'logs'123",  # Multiple quotes
    'fivetran"logs"123',  # Multiple double quotes
    "fivetran`logs`123",  # Multiple backticks
]


@pytest.mark.parametrize("schema", SCHEMA_NAME_TEST_CASES)
def test_quoted_query_transpilation(schema):
    """Test different schema strings and their transpilation to Snowflake, BigQuery, and Databricks"""
    # Ref: https://github.com/datahub-project/datahub/issues/14210

    # Test with Snowflake destination platform to verify unquoted identifier support
    fivetran_log_query_snowflake = FivetranLogQuery()
    # Test without platform (default behavior - always quote)
    fivetran_log_query = FivetranLogQuery()
    fivetran_log_query.use_database("test_database")

    with mock.patch(
        "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"
    ) as mock_create_engine:
        connection_magic_mock = MagicMock()
        connection_magic_mock.execute.fetchone.side_effect = ["test-project-id"]

        mock_create_engine.return_value = connection_magic_mock

        snowflake_dest_config = FivetranLogConfig(
            destination_platform="snowflake",
            snowflake_destination_config=SnowflakeDestinationConfig(
                account_id="TESTID",
                warehouse="TEST_WH",
                username="test",
                password="test@123",
                database="TEST_DATABASE",
                role="TESTROLE",
                log_schema="TEST_SCHEMA",
            ),
        )

        bigquery_dest_config = FivetranLogConfig(
            destination_platform="bigquery",
            bigquery_destination_config=BigQueryDestinationConfig(
                credential=GCPCredential(
                    private_key_id="testprivatekey",
                    project_id="test-project",
                    client_email="fivetran-connector@test-project.iam.gserviceaccount.com",
                    client_id="1234567",
                    private_key="private-key",
                ),
                dataset="test_dataset",
            ),
        )

        databricks_dest_config = FivetranLogConfig(
            destination_platform="databricks",
            databricks_destination_config=DatabricksDestinationConfig(
                token="test-token",
                workspace_url="https://test-workspace.cloud.databricks.com",
                warehouse_id="test-warehouse-id",
                catalog="test_catalog",
                log_schema="test_schema",
            ),
        )

        # Create FivetranLogAPI instance
        snowflake_fivetran_log_api = FivetranLogAPI(snowflake_dest_config)
        bigquery_fivetran_log_api = FivetranLogAPI(bigquery_dest_config)
        databricks_fivetran_log_api = FivetranLogAPI(databricks_dest_config)

        # Test with default (always quote)
        fivetran_log_query.set_schema(schema)

        # Make sure the schema_clause is wrapped in double quotes and ends with "."
        # Example: "fivetran".
        assert fivetran_log_query.schema_clause[0] == '"', (
            "Missing double quote at the beginning of schema_clause"
        )
        assert fivetran_log_query.schema_clause[-2] == '"', (
            "Missing double quote at the end of schema_clause"
        )
        assert fivetran_log_query.schema_clause[-1] == ".", (
            "Missing dot at the end of schema_clause"
        )

        # If the schema has quotes in the string, then schema_clause should have double the number of quotes + 2
        num_quotes_in_schema = schema.count('"')
        num_quotes_in_clause = fivetran_log_query.schema_clause.count('"')
        if num_quotes_in_schema > 0:
            # Each quote in schema is escaped as two quotes in clause, plus two for the wrapping quotes
            assert num_quotes_in_clause == num_quotes_in_schema * 2 + 2, (
                f"For schema {schema!r}, expected {num_quotes_in_schema * 2 + 2} quotes in schema_clause, "
                f"but got {num_quotes_in_clause}: {fivetran_log_query.schema_clause!r}"
            )

        # Test with Snowflake platform - valid unquoted identifiers get uppercased before quoting
        # Simulate the preprocessing that happens in fivetran_log_api.py
        is_valid_unquoted = FivetranLogQuery._is_valid_unquoted_identifier(schema)
        processed_schema = schema.upper() if is_valid_unquoted else schema
        fivetran_log_query_snowflake.set_schema(processed_schema)

        # All identifiers should be quoted (after preprocessing)
        assert fivetran_log_query_snowflake.schema_clause[0] == '"', (
            f"Expected quoted identifier for Snowflake schema {schema!r}, but got: {fivetran_log_query_snowflake.schema_clause!r}"
        )
        assert fivetran_log_query_snowflake.schema_clause[-2] == '"', (
            f"Missing double quote at the end for Snowflake schema {schema!r}"
        )
        assert fivetran_log_query_snowflake.schema_clause[-1] == ".", (
            f"Missing dot at the end for Snowflake schema {schema!r}"
        )

        # Verify the processed schema is in the clause
        # Extract the schema name from the clause (remove quotes and dot)
        schema_in_clause = fivetran_log_query_snowflake.schema_clause[1:-2]
        # The schema in the clause will have quotes escaped (doubled), so we need to escape the processed schema for comparison
        expected_escaped_schema = processed_schema.replace('"', '""')
        assert schema_in_clause == expected_escaped_schema, (
            f"For schema {schema!r}, expected processed schema {processed_schema!r} (escaped: {expected_escaped_schema!r}) in clause, "
            f"but got {schema_in_clause!r}"
        )

        # Make sure transpilation works for snowflake, bigquery, and databricks
        snowflake_fivetran_log_api._query(fivetran_log_query.get_connectors_query())
        bigquery_fivetran_log_api._query(fivetran_log_query.get_connectors_query())
        databricks_fivetran_log_api._query(fivetran_log_query.get_connectors_query())


# Test cases with different database names that might cause issues
DATABASE_NAME_TEST_CASES = [
    "test_database",  # Normal case
    "test-database",  # Hyphen
    "test_database_123",  # Underscore and numbers
    "test.database",  # Dot
    "test database",  # Space
    "test'database",  # Single quote
    'test"database',  # Double quote
    "test`database",  # Backtick
    "test-database-123",  # Multiple hyphens
    "test_database-123",  # Mixed underscore and hyphen
    "test-database_123",  # Mixed hyphen and underscore
    "test.database-123",  # Mixed dot and hyphen
    "test database 123",  # Multiple spaces
    "test'database'123",  # Multiple quotes
    'test"database"123',  # Multiple double quotes
    "test`database`123",  # Multiple backticks
]


@pytest.mark.parametrize("db_name", DATABASE_NAME_TEST_CASES)
def test_quoted_database_identifiers(db_name):
    """Test different database names and their quoted identifier handling"""
    # Ref: https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers

    # Test with Snowflake destination platform to verify unquoted identifier support
    fivetran_log_query_snowflake = FivetranLogQuery()
    # Test without platform (default behavior - always quote)
    fivetran_log_query = FivetranLogQuery()

    with mock.patch(
        "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"
    ) as mock_create_engine:
        connection_magic_mock = MagicMock()
        connection_magic_mock.execute.fetchone.side_effect = ["test-project-id"]

        mock_create_engine.return_value = connection_magic_mock

        snowflake_dest_config = FivetranLogConfig(
            destination_platform="snowflake",
            snowflake_destination_config=SnowflakeDestinationConfig(
                account_id="TESTID",
                warehouse="TEST_WH",
                username="test",
                password="test@123",
                database="TEST_DATABASE",
                role="TESTROLE",
                log_schema="TEST_SCHEMA",
            ),
        )

        bigquery_dest_config = FivetranLogConfig(
            destination_platform="bigquery",
            bigquery_destination_config=BigQueryDestinationConfig(
                credential=GCPCredential(
                    private_key_id="testprivatekey",
                    project_id="test-project",
                    client_email="fivetran-connector@test-project.iam.gserviceaccount.com",
                    client_id="1234567",
                    private_key="private-key",
                ),
                dataset="test_dataset",
            ),
        )

        databricks_dest_config = FivetranLogConfig(
            destination_platform="databricks",
            databricks_destination_config=DatabricksDestinationConfig(
                token="test-token",
                workspace_url="https://test-workspace.cloud.databricks.com",
                warehouse_id="test-warehouse-id",
                catalog="test_catalog",
                log_schema="test_schema",
            ),
        )

        # Create FivetranLogAPI instance
        snowflake_fivetran_log_api = FivetranLogAPI(snowflake_dest_config)
        bigquery_fivetran_log_api = FivetranLogAPI(bigquery_dest_config)
        databricks_fivetran_log_api = FivetranLogAPI(databricks_dest_config)

        # Test with default (always quote)
        use_db_query = fivetran_log_query.use_database(db_name)

        # Make sure the database name is wrapped in double quotes
        # Example: use database "test_database"
        assert use_db_query.startswith('use database "'), (
            f"Missing 'use database \"' at the beginning for database {db_name!r}"
        )
        assert use_db_query.endswith('"'), (
            f"Missing double quote at the end for database {db_name!r}"
        )

        # Extract the database name from the query
        # Format: use database "db_name"
        db_name_in_query = use_db_query[14:-1]  # Remove 'use database "' and '"'

        # If the database name has quotes in the string, then the query should have double the number of quotes
        # Each quote in database name is escaped as two quotes in query, plus two for the wrapping quotes
        num_quotes_in_db_name = db_name.count('"')
        num_quotes_in_query = db_name_in_query.count('"')
        if num_quotes_in_db_name > 0:
            # Each quote in database name is escaped as two quotes in query
            assert num_quotes_in_query == num_quotes_in_db_name * 2, (
                f"For database {db_name!r}, expected {num_quotes_in_db_name * 2} quotes in query (escaped), "
                f"but got {num_quotes_in_query}: {use_db_query!r}"
            )

        # Test with Snowflake platform - valid unquoted identifiers get uppercased before quoting
        # Simulate the preprocessing that happens in fivetran_log_api.py
        is_valid_unquoted = FivetranLogQuery._is_valid_unquoted_identifier(db_name)
        processed_db_name = db_name.upper() if is_valid_unquoted else db_name
        use_db_query_snowflake = fivetran_log_query_snowflake.use_database(
            processed_db_name
        )

        # All identifiers should be quoted (after preprocessing)
        assert use_db_query_snowflake.startswith('use database "'), (
            f"Expected quoted identifier for Snowflake database {db_name!r}, but got: {use_db_query_snowflake!r}"
        )
        assert use_db_query_snowflake.endswith('"'), (
            f"Missing double quote at the end for Snowflake database {db_name!r}"
        )

        # Verify the processed database name is in the query
        # Extract the database name from the query (remove 'use database "' and '"')
        db_name_in_query = use_db_query_snowflake[14:-1]
        # The database name in the query will have quotes escaped (doubled), so we need to escape the processed name for comparison
        expected_escaped_db_name = processed_db_name.replace('"', '""')
        assert db_name_in_query == expected_escaped_db_name, (
            f"For database {db_name!r}, expected processed name {processed_db_name!r} (escaped: {expected_escaped_db_name!r}) in query, "
            f"but got {db_name_in_query!r}"
        )

        # Set a test schema and verify queries work with the database name
        fivetran_log_query.set_schema("test_schema")
        # Make sure transpilation works for snowflake, bigquery, and databricks
        # Note: use_database returns a query string, it doesn't modify the query object
        # So we test that the schema queries still work correctly
        snowflake_fivetran_log_api._query(fivetran_log_query.get_connectors_query())
        bigquery_fivetran_log_api._query(fivetran_log_query.get_connectors_query())
        databricks_fivetran_log_api._query(fivetran_log_query.get_connectors_query())


def test_snowflake_unquoted_identifier_uppercase_conversion():
    """Test that valid unquoted identifiers are uppercased for Snowflake backward compatibility"""
    from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery

    fivetran_log_query = FivetranLogQuery()

    # Test cases: valid unquoted identifiers (should be uppercased)
    valid_unquoted_cases = [
        "test_database",  # lowercase, valid
        "my_schema",  # lowercase, valid
        "TEST_DB",  # already uppercase, valid
        "schema_123",  # lowercase with numbers, valid
    ]

    # Test cases: invalid unquoted identifiers (should stay as-is)
    invalid_unquoted_cases = [
        "test-database",  # has hyphen
        "test.database",  # has dot
        "test database",  # has space
        '"quoted_db"',  # already quoted
        "test'db",  # has single quote
    ]

    for identifier in valid_unquoted_cases:
        # Simulate preprocessing in fivetran_log_api.py
        is_valid = FivetranLogQuery._is_valid_unquoted_identifier(identifier)
        assert is_valid, f"Expected {identifier!r} to be a valid unquoted identifier"

        processed = identifier.upper() if is_valid else identifier
        assert processed == identifier.upper(), (
            f"Expected {identifier!r} to be uppercased to {identifier.upper()!r}, "
            f"but got {processed!r}"
        )

        # Verify it gets quoted
        use_db_query = fivetran_log_query.use_database(processed)
        assert use_db_query == f'use database "{processed}"', (
            f"Expected quoted uppercase identifier, got: {use_db_query!r}"
        )

        for identifier in invalid_unquoted_cases:
            # Simulate preprocessing in fivetran_log_api.py
            is_valid = FivetranLogQuery._is_valid_unquoted_identifier(identifier)
            assert not is_valid, (
                f"Expected {identifier!r} to be an invalid unquoted identifier"
            )

            processed = identifier.upper() if is_valid else identifier
            assert processed == identifier, (
                f"Expected {identifier!r} to stay as-is, but got {processed!r}"
            )

            # Verify it gets quoted as-is
            # Note: If the identifier is already quoted (starts and ends with quotes),
            # the quotes inside will be escaped when we quote it again
            use_db_query = fivetran_log_query.use_database(processed)
            # Escape quotes in the processed identifier for comparison
            expected_escaped = processed.replace('"', '""')
            assert use_db_query == f'use database "{expected_escaped}"', (
                f"Expected quoted identifier as-is (escaped: {expected_escaped!r}), got: {use_db_query!r}"
            )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fivetran_with_snowflake_dest(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/fivetran"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "fivetran_test_events.json"
    golden_file = test_resources_dir / "fivetran_snowflake_golden.json"

    with mock.patch(
        "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"
    ) as mock_create_engine:
        connection_magic_mock = MagicMock()
        connection_magic_mock.execute.side_effect = default_query_results

        mock_create_engine.return_value = connection_magic_mock

        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "fivetran",
                    "config": {
                        "fivetran_log_config": {
                            "destination_platform": "snowflake",
                            "snowflake_destination_config": {
                                "account_id": "testid",
                                "warehouse": "test_wh",
                                "username": "test",
                                "password": "test@123",
                                "database": "test_database",
                                "role": "testrole",
                                "log_schema": "test",
                            },
                        },
                        "connector_patterns": {
                            "allow": ["postgres", "confluent_cloud"]
                        },
                        "destination_patterns": {
                            "allow": [
                                "interval_unconstitutional",
                                "my_confluent_cloud_connector_id",
                            ]
                        },
                        "sources_to_platform_instance": {
                            "calendar_elected": {
                                "database": "postgres_db",
                                "env": "DEV",
                            },
                            "my_confluent_cloud_connector_id": {
                                "platform": "kafka",
                                "include_schema_in_urn": False,
                                "database": "kafka_prod",
                            },
                        },
                        "destination_to_platform_instance": {
                            "my_confluent_cloud_connector_id": {
                                "platform": "kafka",
                                "include_schema_in_urn": False,
                                "database": "kafka_prod",
                            }
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{output_file}",
                    },
                },
            }
        )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{output_file}",
        golden_path=f"{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fivetran_with_snowflake_dest_and_null_connector_user(pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/fivetran"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "fivetran_test_events.json"
    golden_file = (
        test_resources_dir / "fivetran_snowflake_empty_connection_user_golden.json"
    )

    with mock.patch(
        "datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"
    ) as mock_create_engine:
        connection_magic_mock = MagicMock()

        connector_query_results = [
            {
                "connection_id": "calendar_elected",
                "connecting_user_id": None,
                "connector_type_id": "postgres",
                "connection_name": "postgres",
                "paused": False,
                "sync_frequency": 1440,
                "destination_id": "interval_unconstitutional",
            },
            {
                "connection_id": "my_confluent_cloud_connector_id",
                "connecting_user_id": None,
                "connector_type_id": "confluent_cloud",
                "connection_name": "confluent_cloud",
                "paused": False,
                "sync_frequency": 1440,
                "destination_id": "interval_unconstitutional",
            },
        ]

        connection_magic_mock.execute.side_effect = partial(
            default_query_results, connector_query_results=connector_query_results
        )

        mock_create_engine.return_value = connection_magic_mock

        pipeline = Pipeline.create(
            {
                "run_id": "powerbi-test",
                "source": {
                    "type": "fivetran",
                    "config": {
                        "platform_instance": "my-fivetran",
                        "fivetran_log_config": {
                            "destination_platform": "snowflake",
                            "snowflake_destination_config": {
                                "account_id": "testid",
                                "warehouse": "test_wh",
                                "username": "test",
                                "password": "test@123",
                                "database": "test_database",
                                "role": "testrole",
                                "log_schema": "test",
                            },
                        },
                        "connector_patterns": {
                            "allow": ["postgres", "confluent_cloud"]
                        },
                        "destination_patterns": {
                            "allow": [
                                "interval_unconstitutional",
                            ]
                        },
                        "sources_to_platform_instance": {
                            "calendar_elected": {
                                "platform": "postgres",
                                "env": "DEV",
                                "database": "postgres_db",
                            },
                            "my_confluent_cloud_connector_id": {
                                "platform": "kafka",
                                "database": "kafka_prod",
                                "include_schema_in_urn": False,
                            },
                        },
                        "destination_to_platform_instance": {
                            "my_confluent_cloud_connector_id": {
                                "platform": "kafka",
                                "database": "kafka_prod",
                                "include_schema_in_urn": False,
                            }
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{output_file}",
                    },
                },
            }
        )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{output_file}",
        golden_path=f"{golden_file}",
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_fivetran_bigquery_config():
    with mock.patch("datahub.ingestion.source.fivetran.fivetran_log_api.create_engine"):
        # Simply test that the config is parsed and the source is initialized without an error.
        assert FivetranSource.create(
            {
                "fivetran_log_config": {
                    "destination_platform": "bigquery",
                    "bigquery_destination_config": {
                        "credential": {
                            "private_key_id": "testprivatekey",
                            "project_id": "test-project",
                            "client_email": "fivetran-connector@test-project.iam.gserviceaccount.com",
                            "client_id": "1234567",
                            "private_key": "private-key",
                        },
                        "dataset": "test",
                    },
                },
            },
            ctx=PipelineContext(run_id="fivetran-bq-dummy"),
        )


@freeze_time(FROZEN_TIME)
def test_fivetran_snowflake_destination_config():
    snowflake_dest = SnowflakeDestinationConfig(
        account_id="TESTID",
        warehouse="TEST_WH",
        username="test",
        password="test@123",
        database="TEST_DATABASE",
        role="TESTROLE",
        log_schema="TEST_SCHEMA",
    )
    assert (
        snowflake_dest.get_sql_alchemy_url()
        == "snowflake://test:test%40123@TESTID?application=acryl_datahub&authenticator=SNOWFLAKE&role=TESTROLE&warehouse=TEST_WH"
    )


@freeze_time(FROZEN_TIME)
def test_fivetran_bigquery_destination_config():
    bigquery_dest = BigQueryDestinationConfig(
        credential=GCPCredential(
            private_key_id="testprivatekey",
            project_id="test-project",
            client_email="fivetran-connector@test-project.iam.gserviceaccount.com",
            client_id="1234567",
            private_key="private-key",
        ),
        dataset="test_dataset",
    )
    assert bigquery_dest.get_sql_alchemy_url() == "bigquery://"


@freeze_time(FROZEN_TIME)
def test_rename_destination_config():
    config_dict = {
        "fivetran_log_config": {
            "destination_platform": "snowflake",
            "destination_config": {
                "account_id": "testid",
                "database": "test_database",
                "log_schema": "test",
            },
        },
    }
    with pytest.warns(
        ConfigurationWarning,
        match="destination_config is deprecated, please use snowflake_destination_config instead.",
    ):
        config = FivetranSourceConfig.model_validate(config_dict)
        assert config.fivetran_log_config.snowflake_destination_config is not None
        assert (
            config.fivetran_log_config.snowflake_destination_config.account_id
            == "testid"
        )


def test_compat_sources_to_database() -> None:
    config_dict = {
        # We just need a valid fivetran_log_config to test the compat transformation.
        "fivetran_log_config": {
            "destination_platform": "snowflake",
            "snowflake_destination_config": {
                "account_id": "testid",
                "warehouse": "test_wh",
                "username": "test",
                "password": "test@123",
                "database": "test_database",
                "role": "testrole",
                "log_schema": "test",
            },
        },
        "sources_to_database": {"calendar_elected": "my_db", "connector_2": "my_db_2"},
        "sources_to_platform_instance": {"calendar_elected": {"env": "DEV"}},
    }

    with pytest.warns(
        ConfigurationWarning,
        match=r"sources_to_database.*deprecated",
    ):
        config = FivetranSourceConfig.model_validate(config_dict)

    assert config.sources_to_platform_instance == {
        "calendar_elected": PlatformDetail(env="DEV", database="my_db"),
        "connector_2": PlatformDetail(database="my_db_2"),
    }
