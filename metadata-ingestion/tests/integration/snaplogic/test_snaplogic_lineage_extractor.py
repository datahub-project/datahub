import json
from pathlib import Path

import pytest

from datahub.ingestion.source.snaplogic.snaplogic_parser import (
    ColumnMapping,
    Dataset,
    SnapLogicParser,
)


@pytest.fixture
def lineage_data():
    # Path to your local test_data folder
    test_resources_dir = Path(__file__).parent / "test_data"

    # Load the mock response from snaplogic_base_response.json
    with open(test_resources_dir / "snaplogic_simple_response.json", "r") as f:
        snaplogic_response = json.load(f)

    # Return it so tests can use it
    return snaplogic_response


def test_extract_columns_mapping_from_lineage(lineage_data):
    instance = SnapLogicParser([], {})

    expected_result = [
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Id",
            output_field="Id",
        ),
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Name",
            output_field="Name",
        ),
    ]

    result = instance.extract_columns_mapping_from_lineage(
        lineage_data.get("content")[0]
    )

    # Check that the result is a list
    assert isinstance(result, list)
    assert result == expected_result


def test_extract_columns_mapping_from_lineage_case_insensitive_namespace(lineage_data):
    instance = SnapLogicParser(
        ["sqlserver://snaplogic-test.database.windows.net:1433"], {}
    )

    expected_result = [
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="id",
            output_field="Id",
        ),
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="name",
            output_field="Name",
        ),
    ]

    result = instance.extract_columns_mapping_from_lineage(
        lineage_data.get("content")[0]
    )

    # Check that the result is a list
    assert isinstance(result, list)
    assert result == expected_result


def test_extract_columns_mapping_from_lineage_case_namespace_mapping(lineage_data):
    instance = SnapLogicParser(
        [], {"sqlserver://snaplogic-test.database.windows.net:1433": "prod-sqlserver"}
    )

    expected_result = [
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform_instance="prod-sqlserver",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Id",
            output_field="Id",
        ),
        ColumnMapping(
            input_dataset=Dataset(
                name="snaplogic-test.tonyschema.accounts",
                display_name="snaplogic-test.tonyschema.accounts",
                platform_instance="prod-sqlserver",
                platform="mssql",
                type="INPUT",
            ),
            output_dataset=Dataset(
                name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                display_name="Virtual_DB.Virtual_Schema.Azure Synapse SQL - Select:2faf1",
                type="OUTPUT",
            ),
            input_field="Name",
            output_field="Name",
        ),
    ]

    result = instance.extract_columns_mapping_from_lineage(
        lineage_data.get("content")[0]
    )

    # Check that the result is a list
    assert isinstance(result, list)
    assert result == expected_result
