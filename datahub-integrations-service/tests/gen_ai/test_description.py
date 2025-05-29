from typing import List
from unittest.mock import MagicMock, patch

import datahub.metadata.schema_classes as models
import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.gen_ai.description_context import (
    EntityDescriptionResult,
    ExtractedTableInfo,
)
from datahub_integrations.gen_ai.description_v3 import (
    generate_entity_descriptions_for_urn,
)

# Sample URN for testing
TEST_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table_name,PROD)"
)


@pytest.fixture
def mock_graph_client() -> DataHubGraph:
    # Create a mock DataHubGraph
    mock_client = MagicMock(spec=DataHubGraph)

    # Mock entity response
    mock_entity = {
        "schemaMetadata": models.SchemaMetadataClass(
            schemaName="database.schema",
            platform="snowflake",
            platformSchema=models.OtherSchemaClass(rawSchema=""),
            version=1,
            hash="1234567890",
            fields=[
                models.SchemaFieldClass(
                    fieldPath="id",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR",
                    description="Primary key for the table",
                    isPartOfKey=True,
                ),
                models.SchemaFieldClass(
                    fieldPath="name",
                    type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
                    nativeDataType="VARCHAR",
                    description="Name of the entity",
                ),
                models.SchemaFieldClass(
                    fieldPath="created_at",
                    type=models.SchemaFieldDataTypeClass(type=models.DateTypeClass()),
                    nativeDataType="DATE",
                    description="Creation timestamp",
                ),
            ],
        ),
        "datasetProperties": models.DatasetPropertiesClass(
            name="table_name",
            description="A test table for unit testing",
        ),
        "editableDatasetProperties": models.EditableDatasetPropertiesClass(
            description="An edited description for the test table",
        ),
        "upstreamLineage": models.UpstreamLineageClass(
            upstreams=[
                models.UpstreamClass(
                    dataset="urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.source_table,PROD)",
                    type=models.DatasetLineageTypeClass.TRANSFORMED,
                )
            ],
            fineGrainedLineages=[
                models.FineGrainedLineageClass(
                    upstreamType=models.FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=models.FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                    upstreams=[
                        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.source_table,PROD),source_id)"
                    ],
                    downstreams=[
                        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table_name,PROD),id)"
                    ],
                    transformOperation="COPY",
                )
            ],
        ),
        "globalTags": models.GlobalTagsClass(
            tags=[
                models.TagAssociationClass(
                    tag="urn:li:tag:test_tag",
                )
            ]
        ),
        "glossaryTerms": models.GlossaryTermsClass(
            terms=[
                models.GlossaryTermAssociationClass(
                    urn="urn:li:glossaryTerm:test_term",
                )
            ],
            auditStamp=models.AuditStampClass(
                time=1717958400000,
                actor="urn:li:corpuser:test_user",
            ),
        ),
        "domains": models.DomainsClass(
            domains=["urn:li:domain:test_domain"],
        ),
        "ownership": models.OwnershipClass(
            owners=[
                models.OwnerClass(
                    owner="urn:li:corpuser:test_user",
                    type=models.OwnershipTypeClass.DATAOWNER,
                )
            ]
        ),
    }

    def mock_get_entity_semityped(urn: str) -> dict:
        if urn == TEST_URN:
            return mock_entity
        elif "tag" in urn:
            return {
                "tagProperties": models.TagPropertiesClass(
                    name="test_tag", description="A test tag"
                )
            }
        elif "glossaryTerm" in urn:
            return {
                "glossaryTermInfo": models.GlossaryTermInfoClass(
                    name="test_term",
                    definition="A test term",
                    termSource="test_source",
                )
            }
        elif "domain" in urn:
            return {
                "domainProperties": models.DomainPropertiesClass(
                    name="test_domain", description="A test domain"
                )
            }
        elif "downstream_table" in urn:
            return {
                "datasetProperties": models.DatasetPropertiesClass(
                    name="downstream_table", description="A downstream table"
                )
            }
        elif "source_table" in urn:
            return {
                "datasetProperties": models.DatasetPropertiesClass(
                    name="source_table", description="A source table"
                ),
                "schemaMetadata": models.SchemaMetadataClass(
                    schemaName="database.schema",
                    platform="snowflake",
                    platformSchema=models.OtherSchemaClass(rawSchema=""),
                    version=1,
                    hash="1234567890",
                    fields=[
                        models.SchemaFieldClass(
                            fieldPath="source_id",
                            type=models.SchemaFieldDataTypeClass(
                                type=models.StringTypeClass()
                            ),
                            nativeDataType="VARCHAR",
                            description="Source ID",
                        ),
                    ],
                ),
            }
        return {}

    mock_client.get_entity_semityped.side_effect = mock_get_entity_semityped

    # Mock get_downstream_urns_for_table
    mock_client.execute_graphql.return_value = {
        "dataset": {
            "lineage": {
                "relationships": [
                    {
                        "entity": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.downstream_table,PROD)"
                        }
                    }
                ]
            }
        }
    }

    # Mock get_sample_values
    mock_client.get_timeseries_values.return_value = [
        models.DatasetProfileClass(
            timestampMillis=1717958400000,
            fieldProfiles=[
                models.DatasetFieldProfileClass(
                    fieldPath="id",
                    sampleValues=["1", "2", "3"],
                ),
                models.DatasetFieldProfileClass(
                    fieldPath="name",
                    sampleValues=["John", "Jane", "Bob"],
                ),
            ],
        )
    ]

    return mock_client


@pytest.fixture
def mock_bedrock_responses() -> List[str]:
    # Sequence is important here.
    return [
        """
    {
        "table_description": "This is a test table description",
    }
    """,
        """
    {
        "id": "Primary key for the table",
        "name": "Name of the entity",
        "created_at": "Creation timestamp"
    }
    """,
    ]


def test_generate_entity_descriptions_for_urn(
    mock_graph_client: DataHubGraph, mock_bedrock_responses: List[str]
) -> None:
    mock_client = mock_graph_client

    with patch(
        "datahub_integrations.gen_ai.description_v3.call_bedrock_llm"
    ) as mock_call_bedrock_llm:
        # Set up the mock for call_bedrock_llm
        mock_call_bedrock_llm.side_effect = mock_bedrock_responses

        # Call the function
        result = generate_entity_descriptions_for_urn(mock_client, TEST_URN)

        # Verify the result
        assert isinstance(result, EntityDescriptionResult)
        assert result.table_description == "This is a test table description"
        assert result.column_descriptions == {
            "id": "Primary key for the table",
            "name": "Name of the entity",
            "created_at": "Creation timestamp",
        }

        # Verify that call_bedrock_llm was called with the correct parameters
        mock_call_bedrock_llm.assert_called()
        assert mock_call_bedrock_llm.call_count == 2
        call1 = mock_call_bedrock_llm.call_args_list[0]
        call2 = mock_call_bedrock_llm.call_args_list[1]

        assert call1[1]["max_tokens"] == 4096  # max_tokens

        # Verify that the extracted_entity_info is correct
        check_graph_to_info_class_mapping(result)

        table_prompt = """You are tasked with generating concise description for a DataHub table based on provided metadata. Here is the information you will be working with:

<table_info>
{'tags': [{'tag_name': 'test_tag', 'tag_description': 'A test tag'}], 'glossary_terms': [{'term_name': 'test_term', 'term_definition': 'A test term'}], 'name': 'table_name', 'description': 'An edited description for the test table', 'domains_info': [{'domain_name': 'test_domain', 'domain_description': 'A test domain'}], 'owners_info': [{'owner_name': 'test_user', 'owner_type': 'DATAOWNER'}], 'upstream_lineage_info': [{'upstream_table_urn': 'urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.source_table,PROD)', 'upstream_table_name': 'source_table', 'upstream_table_description': 'A source table', 'lineage_type': 'TRANSFORMED'}], 'downstream_lineage_info': [{'downstream_table_urn': 'urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.downstream_table,PROD)', 'downstream_table_name': 'downstream_table', 'downstream_table_description': 'A downstream table'}]}
</table_info>

<column_info>
{'id': {'column_name': 'id', 'metadata': {'description': 'Primary key for the table', 'nativeDataType': 'VARCHAR', 'isPartOfKey': True}, 'descriptions': 'Primary key for the table', 'upstream_lineages': [{'lineage': [{'upstream_column_name': 'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.source_table,PROD),source_id)', 'upstream_column_description': 'Source ID', 'upstream_column_native_type': 'VARCHAR'}], 'transform_operation': 'COPY'}], 'sample_values': ['1', '2', '3']}, 'name': {'column_name': 'name', 'metadata': {'description': 'Name of the entity', 'nativeDataType': 'VARCHAR'}, 'descriptions': 'Name of the entity', 'upstream_lineages': [], 'sample_values': ['John', 'Jane', 'Bob']}, 'created_at': {'column_name': 'created_at', 'metadata': {'description': 'Creation timestamp', 'nativeDataType': 'DATE'}, 'descriptions': 'Creation timestamp', 'upstream_lineages': []}}
</column_info>

Generate the description as follows:

   Create a few paragraphs of Markdown-formatted text that includes:
   a) A summary of the primary purpose and business importance of the table.
   b) If metadata is available, a summary of the upstream tables and transformations applied.
   c) A summary of the downstream tables (consumers) and general use cases for the table. Only include information that can be substantiated by the provided table_info.
   d) Technical notes and usage tips, including the table type (fact or dimension) and grain if available.
   e) A note on whether the table directly contains any PII data, like names, emails, and addresses. Do not provide recommendations related to access control, monitoring, or governance.

   Format any references to other entities as markdown links, using the entity URN as the link. For example: [table_name](urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table_name,PROD))
   Use Markdown sections like H2 and H3 with appropriate section titles. The first line should be \"# <table name>\", followed by a blank line.

When writing the descriptions:
- Aim for a technical yet informative tone, suitable for a data catalog.
- Avoid weak phrases like \"suggests\", \"could be\", \"likely\", or \"is considered\". Only include information you are confident about based on the provided metadata.
- Be concise and to the point.

Provide your output in the following dictionary format:

{
    \"table_description\": \"\"\"
[Your multi-line table description here]
\"\"\"
}

Ensure that the dictionary is properly formatted and parsable. Include the table description with the key \"table_description\"."""

        column_prompt = """You are tasked with generating concise description for a columns of a DataHub table based on provided metadata. Here is the information you will be working with:

<table_info>
{\'tags\': [{\'tag_name\': \'test_tag\', \'tag_description\': \'A test tag\'}], \'glossary_terms\': [{\'term_name\': \'test_term\', \'term_definition\': \'A test term\'}], \'name\': \'table_name\', \'description\': \'An edited description for the test table\', \'domains_info\': [{\'domain_name\': \'test_domain\', \'domain_description\': \'A test domain\'}], \'owners_info\': [{\'owner_name\': \'test_user\', \'owner_type\': \'DATAOWNER\'}], \'upstream_lineage_info\': [{\'upstream_table_urn\': \'urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.source_table,PROD)\', \'upstream_table_name\': \'source_table\', \'upstream_table_description\': \'A source table\', \'lineage_type\': \'TRANSFORMED\'}], \'downstream_lineage_info\': [{\'downstream_table_urn\': \'urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.downstream_table,PROD)\', \'downstream_table_name\': \'downstream_table\', \'downstream_table_description\': \'A downstream table\'}]}
</table_info>

<column_info>
{\'id\': {\'column_name\': \'id\', \'metadata\': {\'description\': \'Primary key for the table\', \'nativeDataType\': \'VARCHAR\', \'isPartOfKey\': True}, \'descriptions\': \'Primary key for the table\', \'upstream_lineages\': [{\'lineage\': [{\'upstream_column_name\': \'urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.source_table,PROD),source_id)\', \'upstream_column_description\': \'Source ID\', \'upstream_column_native_type\': \'VARCHAR\'}], \'transform_operation\': \'COPY\'}], \'sample_values\': [\'1\', \'2\', \'3\']}, \'name\': {\'column_name\': \'name\', \'metadata\': {\'description\': \'Name of the entity\', \'nativeDataType\': \'VARCHAR\'}, \'descriptions\': \'Name of the entity\', \'upstream_lineages\': [], \'sample_values\': [\'John\', \'Jane\', \'Bob\']}, \'created_at\': {\'column_name\': \'created_at\', \'metadata\': {\'description\': \'Creation timestamp\', \'nativeDataType\': \'DATE\'}, \'descriptions\': \'Creation timestamp\', \'upstream_lineages\': []}}
</column_info>

<table_description>
This is a test table description
</table_description>

Generate the description as follows:

  For each column, create a concise description of one or two sentences. Prefer elliptical sentences that are direct and to the point. If available, include details about how the column was generated or calculated.



When writing the descriptions:
- Aim for a technical yet informative tone, suitable for a data catalog.
- Avoid weak phrases like "suggests", "could be", "likely", or "is considered". Only include information you are confident about based on the provided metadata.
- Be concise and to the point.

Provide your output in the following dictionary format:

{
    "column_name1": "Column description",
    "column_name2": "Column description",
    ...
}

Ensure that the dictionary is properly formatted and parsable. Use the column display names as keys for the column descriptions.\
"""
        # Verify that the prompt contains the expected information
        assert call1[0][0] == table_prompt
        assert call2[0][0] == column_prompt


def check_graph_to_info_class_mapping(result: EntityDescriptionResult) -> None:
    assert isinstance(result.extracted_entity_info, ExtractedTableInfo)
    assert result.extracted_entity_info.table_name == "table_name"
    assert (
        result.extracted_entity_info.table_description
        == "An edited description for the test table"
    )
    assert len(result.extracted_entity_info.column_names) == 3
    assert len(result.extracted_entity_info.column_descriptions) == 3
    assert len(result.extracted_entity_info.column_upstream_lineages) == 3

    assert len(result.extracted_entity_info.table_tags) == 1
    assert len(result.extracted_entity_info.table_glossary_terms) == 1
    assert result.extracted_entity_info.table_domains_info is not None
    assert len(result.extracted_entity_info.table_domains_info) == 1
    assert result.extracted_entity_info.table_owners_info is not None
    assert len(result.extracted_entity_info.table_owners_info) == 1
    assert result.extracted_entity_info.table_upstream_lineage_info is not None
    assert len(result.extracted_entity_info.table_upstream_lineage_info) == 1
    assert len(result.extracted_entity_info.table_downstream_lineage_info) == 1
