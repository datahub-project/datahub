import time
from enum import Enum

import pytest
from tenacity import retry, stop_after_delay, wait_fixed

import datahub.metadata.schema_classes as models
from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from tests.utils import ingest_file_via_rest, wait_for_writes_to_sync

_MAX_DELAY_UNTIL_WRITES_VISIBLE_SECS = 30
_ATTEMPT_RETRY_INTERVAL_SECS = 1
large_dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:looker,test-large-schema,PROD)"


class FieldPathStyle(Enum):
    FLAT = "FLAT"
    NESTED = "NESTED"


def _create_schema_with_fields(
    entity_urn: str,
    num_fields: int = 1,
    field_path_style: FieldPathStyle = FieldPathStyle.FLAT,
) -> models.SchemaMetadataClass:
    """
    A simple helper function to create a schema with a given number of fields.
    The fields are created with the following naming convention:
    - FLAT: field_0, field_1, field_2, ...
    - NESTED: [version=2.0].[type=Address].parent_field.[type=string].field_0,
    [version=2.0].[type=Address].parent_field.[type=string].field_1, ...

    TODO: Add support for more complex field types and descriptions beyond
    strings.
    """

    schema = models.SchemaMetadataClass(
        schemaName="my_schema",
        platformSchema=models.OtherSchemaClass(rawSchema=""),
        platform="urn:li:dataPlatform:bigquery",
        version=0,
        hash="",
        fields=[
            models.SchemaFieldClass(
                fieldPath=(
                    f"field_{i}"
                    if field_path_style == FieldPathStyle.FLAT
                    else f"[version=2.0].[type=Address].parent_field.[type=string].field_{i}"
                ),
                nativeDataType="STRING",
                type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                description="",
                nullable=True,
            )
            for i in range(num_fields)
        ],
    )
    assert schema.validate()
    return schema


@pytest.fixture()
def test_setup(auth_session, graph_client):
    """Fixture data"""
    session = graph_client._session
    gms_host = graph_client.config.server

    ingest_file_via_rest(
        auth_session, "tests/schema_fields/schema_field_side_effect_data.json"
    )

    assert "schemaMetadata" in get_aspects_for_entity(
        session,
        gms_host,
        entity_urn=large_dataset_urn,
        aspects=["schemaMetadata"],
        typed=False,
    )

    yield
    # Deleting takes way too long for CI
    #
    # rollback_url = f"{gms_host}/runs?action=rollback"
    # session.post(
    #     rollback_url,
    #     data=json.dumps(
    #         {"runId": ingested_dataset_run_id, "dryRun": False, "hardDelete": True}
    #     ),
    # )
    #
    # wait_for_writes_to_sync()
    #
    # assert "schemaMetadata" not in get_aspects_for_entity(
    #     entity_urn=large_dataset_urn, aspects=["schemaMetadata"], typed=False
    # )


@retry(
    stop=stop_after_delay(_MAX_DELAY_UNTIL_WRITES_VISIBLE_SECS),
    wait=wait_fixed(_ATTEMPT_RETRY_INTERVAL_SECS),
    reraise=True,
)
def assert_schema_field_exists(graph: DataHubGraph, urn: str, field_path: str):
    schema_field_urn = make_schema_field_urn(parent_urn=urn, field_path=field_path)
    assert graph.exists(schema_field_urn)
    status = graph.get_aspect(schema_field_urn, models.StatusClass)
    assert status is None or status.removed is False


@retry(
    stop=stop_after_delay(_MAX_DELAY_UNTIL_WRITES_VISIBLE_SECS),
    wait=wait_fixed(_ATTEMPT_RETRY_INTERVAL_SECS),
    reraise=True,
)
def assert_schema_field_soft_deleted(graph: DataHubGraph, urn: str, field_path: str):
    schema_field_urn = make_schema_field_urn(parent_urn=urn, field_path=field_path)
    status = graph.get_aspect(schema_field_urn, models.StatusClass)
    assert status and status.removed is True


@pytest.mark.parametrize(
    "field_path_style",
    [
        FieldPathStyle.NESTED,
        FieldPathStyle.FLAT,
    ],
)
def test_schema_evolution_field_dropped(
    graph_client: DataHubGraph, field_path_style: FieldPathStyle
):
    """
    Test that schema evolution works as expected
    1. Create a schema with 2 fields
    2. Sleep for 10 seconds
    3. Update the schema to have 1 field
    4. Sleep for 10 seconds
    5. Assert that the field_1 is removed
    """

    now = int(time.time())

    urn = make_dataset_urn("bigquery", f"my_dataset.my_table.{now}")
    print(urn)

    schema_with_2_fields = _create_schema_with_fields(
        urn, 2, field_path_style=field_path_style
    )
    field_names = [field.fieldPath for field in schema_with_2_fields.fields]
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=schema_with_2_fields,
        )
    )

    for field_name in field_names:
        print("Checking field: ", field_name)
        assert_schema_field_exists(graph_client, urn, field_name)

    # Evolve the schema
    schema_with_1_field = _create_schema_with_fields(
        urn, 1, field_path_style=field_path_style
    )
    new_field_name = schema_with_1_field.fields[0].fieldPath

    field_names.remove(new_field_name)
    removed_field_name = field_names[0]

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=schema_with_1_field,
        )
    )

    assert_schema_field_exists(graph_client, urn, new_field_name)
    assert_schema_field_soft_deleted(graph_client, urn, removed_field_name)


def test_soft_deleted_entity(graph_client: DataHubGraph):
    """
    Test that we if there is a soft deleted dataset, its schema fields are
    initialized with soft deleted status
    1. Create a schema with 2 fields
    """

    now = int(time.time())

    urn = make_dataset_urn("bigquery", f"my_dataset.my_table.{now}")
    print(urn)

    schema_with_2_fields = _create_schema_with_fields(urn, 2)
    field_names = [field.fieldPath for field in schema_with_2_fields.fields]
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=schema_with_2_fields,
        )
    )

    for field_name in field_names:
        print("Checking field: ", field_name)
        assert_schema_field_exists(graph_client, urn, field_name)

    # Soft delete the dataset
    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=models.StatusClass(removed=True),
        )
    )

    # Check that the fields are soft deleted
    for field_name in field_names:
        assert_schema_field_soft_deleted(graph_client, urn, field_name)


# Note: Does not execute deletes, too slow for CI
@pytest.mark.dependency()
def test_large_schema(graph_client: DataHubGraph, test_setup):
    wait_for_writes_to_sync()
    assert_schema_field_exists(graph_client, large_dataset_urn, "last_of.6800_cols")
