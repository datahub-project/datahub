import logging
import os
import re
import urllib
from typing import Any, List, Optional

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    FormsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)
from datahub.utilities.urns.urn import Urn
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns, get_sleep_info

logger = logging.getLogger(__name__)

dataset_urns = [
    make_dataset_urn("snowflake", f"database.schema.bulk_form_table_{i}")
    for i in range(0, 2000)
]

generated_urns = dataset_urns.copy()


def parse_dataset_urn(urn):
    """
    Parse a DataHub dataset URN of format:
    urn:li:dataset:(urn:li:dataPlatform:platform,database.schema.table,env)
    """
    match = re.match(
        r"urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,]+),([^)]+)\)", urn
    )
    if not match:
        raise ValueError(f"Invalid dataset URN format: {urn}")

    platform = match.group(1)
    full_name = match.group(2)  # database.schema.table
    env = match.group(3)

    name_parts = full_name.split(".")
    if len(name_parts) != 3:
        raise ValueError(
            f"Expected database.schema.table format in URN, got: {full_name}"
        )

    database, schema, table = name_parts

    return {
        "platform": platform,
        "database": database,
        "schema": schema,
        "table": table,
        "env": env,
    }


def create_dataset_proposal(dataset_urn):
    # Parse the URN to extract components
    urn_parts = parse_dataset_urn(dataset_urn)

    # Add dataset properties using extracted name
    dataset_properties = DatasetPropertiesClass(
        description=f"Dataset {urn_parts['table']} in {urn_parts['database']}.{urn_parts['schema']}",
        name=urn_parts["table"],
        customProperties={
            "database": urn_parts["database"],
            "schema": urn_parts["schema"],
            "platform": urn_parts["platform"],
            "environment": urn_parts["env"],
        },
    )

    # Add ownership information using the built-in OwnershipType
    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner="urn:li:corpuser:jdoe",
                type=OwnershipTypeClass.DATA_STEWARD,  # Using built-in ownership type
            )
        ]
    )

    # Create the MetadataChangeProposalWrapper
    mcp_wrapper = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType="UPSERT",
        entityUrn=dataset_urn,
        aspectName="datasetProperties",
        aspect=dataset_properties,
    )

    ownership_mcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType="UPSERT",
        entityUrn=dataset_urn,
        aspectName="ownership",
        aspect=ownership,
    )

    return [mcp_wrapper, ownership_mcp]


def create_property_definition(
    property_name: str,
    namespace: str = "io.acryl",
    value_type: str = "number",
    cardinality: str = "SINGLE",
    allowed_values: Optional[List[PropertyValueClass]] = None,
    entity_types: Optional[List[str]] = None,
):
    structured_property_definition = StructuredPropertyDefinitionClass(
        qualifiedName=f"{namespace}.{property_name}",
        valueType=Urn.make_data_type_urn(value_type),
        description="The retention policy for the dataset",
        entityTypes=(
            [Urn.make_entity_type_urn(e) for e in entity_types]
            if entity_types
            else [Urn.make_entity_type_urn("dataset")]
        ),
        cardinality=cardinality,
        allowedValues=allowed_values,
    )

    property_urn = f"urn:li:structuredProperty:{namespace}.{property_name}"
    generated_urns.append(property_urn)

    return [
        MetadataChangeProposalWrapper(
            entityUrn=property_urn,
            aspect=structured_property_definition,
        )
    ]


def create_form(auth_session: Any, property_name: str, namespace: str = "io.acryl"):
    json = {
        "query": """mutation createForm($input: CreateFormInput!) {
        createForm(input: $input) {
            urn
        }}""",
        "variables": {
            "input": {
                "id": "bulkFormSubmissionTest",
                "name": "bulkFormSubmissionTest 2024",
                "description": "How we want to ensure the most important data assets in our organization have all of the most important and expected pieces of metadata filled out",
                "type": "VERIFICATION",
                "prompts": [
                    {
                        "id": "123",
                        "title": "Retention Time",
                        "description": "Apply Retention Time structured property to form",
                        "type": "STRUCTURED_PROPERTY",
                        "structuredPropertyParams": {
                            "urn": f"urn:li:structuredProperty:{namespace}.{property_name}"
                        },
                    }
                ],
                "actors": {
                    "users": [
                        "urn:li:corpuser:jane@email.com",
                        "urn:li:corpuser:john@email.com",
                    ],
                    "groups": ["urn:li:corpGroup:team@email.com"],
                },
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()
    form_urn = res_data["data"]["createForm"]["urn"]
    generated_urns.append(form_urn)
    return form_urn


def assign_form(auth_session: Any, form_urn: str):
    json = {
        "query": """mutation batchAssignForm($input: BatchAssignFormInput!) {
        batchAssignForm(input: $input)}""",
        "variables": {"input": {"formUrn": form_urn, "entityUrns": dataset_urns}},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    generated_urns.append(f"urn:li:test:{urllib.parse.quote(form_urn)}")


def create_test_data(auth_session, graph_client):
    mcpws = []
    mcpws.extend(
        create_property_definition(
            "retentionTimeBulkSubmit",
            allowed_values=[
                PropertyValueClass(value="30"),
                PropertyValueClass(value="90"),
                PropertyValueClass(value="365"),
            ],
            entity_types=["urn:li:entityType:dataset"],
        )
    )
    mcpws.extend([mcp for urn in dataset_urns for mcp in create_dataset_proposal(urn)])
    graph_client.emit_mcps(mcpws)
    wait_for_writes_to_sync()


sleep_sec, sleep_times = get_sleep_info()


@pytest.fixture(scope="module")
def ingest_cleanup_data(auth_session, graph_client, request):
    print("generating test datasets")
    create_test_data(auth_session, graph_client)
    yield
    delete_urns(graph_client, generated_urns)
    wait_for_writes_to_sync()


@pytest.mark.skipif(
    os.environ.get("PYTEST_FULL") != "true",
    reason="long running test: duration 3 minutes",
)
def test_bulk_form_submission(ingest_cleanup_data, auth_session, graph_client):
    form_urn = create_form(auth_session, "retentionTimeBulkSubmit")
    assign_form(auth_session, form_urn)
    wait_for_writes_to_sync()
    for dataset_urn in dataset_urns:
        forms = graph_client.get_aspect(dataset_urn, FormsClass)
        assert form_urn == forms.completedForms[0].urn
