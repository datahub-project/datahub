from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import bson
import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mongodb import MongoDBConfig, MongoDBSource
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
)
from datahub.utilities.urns.urn import guess_entity_type


@pytest.fixture
def mock_mongo_client():
    with patch("datahub.ingestion.source.mongodb.MongoClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        mock_instance.admin.command.return_value = {"ok": 1}
        yield mock_instance


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test-mongodb-run")


def test_mongodb_schema_inference_respects_max_schema_size(
    mock_mongo_client, pipeline_context
):
    """
    Test that maxSchemaSize limits the number of fields in the output schema.

    This ensures the schema downsampling works correctly when there are too
    many fields.
    """
    mock_mongo_client.list_database_names.return_value = ["test_db"]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = ["wide_table"]

    mock_collection = MagicMock()
    mock_database.__getitem__.return_value = mock_collection

    mock_document: Dict["str", Any] = {
        "_id": bson.ObjectId("507f1f77bcf86cd799439011"),
    }
    for i in range(50):
        mock_document[f"field_{i}"] = f"value_{i}"

    mock_collection.aggregate.return_value = [mock_document]

    config = MongoDBConfig(
        connect_uri="mongodb://localhost:27017",
        enableSchemaInference=True,
        maxSchemaSize=10,
    )

    source = MongoDBSource(ctx=pipeline_context, config=config)

    workunits = list(source.get_workunits_internal())

    schema_metadata_aspects = [
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(SchemaMetadataClass))
    ]

    assert len(schema_metadata_aspects) == 1
    schema_metadata = schema_metadata_aspects[0]
    assert len(schema_metadata.fields) <= 10

    dataset_properties_aspects: List[DatasetPropertiesClass] = [
        properties_aspect
        for wu in workunits
        if (properties_aspect := wu.get_aspect_of_type(DatasetPropertiesClass))
    ]

    assert len(dataset_properties_aspects) == 1
    custom_props = dataset_properties_aspects[0].customProperties
    assert "schema.downsampled" in custom_props
    assert custom_props["schema.downsampled"] == "True"


def test_mongodb_complex_schema_trimming(mock_mongo_client, pipeline_context):
    """
    Test that maxSchemaSize limits the number of fields in the output schema.

    This ensures the schema downsampling works correctly when there are too
    many fields.
    """
    mock_mongo_client.list_database_names.return_value = ["test_db"]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = ["wide_table"]

    mock_collection = MagicMock()
    mock_database.__getitem__.return_value = mock_collection

    mock_mongo_client.list_database_names.return_value = ["test_db"]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = ["events"]

    mock_collection = MagicMock()
    mock_database.__getitem__.return_value = mock_collection

    mock_documents = [
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439011"),
            "metadata": {
                "source": "web",
                "properties": [
                    {"name": "user_agent", "value": "Mozilla/5.0"},
                    {"name": "ip_address2"},
                    {"name": "ip_address3"},
                    {"name": "ip_address4"},
                ],
                "tags": {"foo": "bar"},
            },
        },
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439012"),
        },
    ]

    mock_collection.aggregate.return_value = mock_documents

    config = MongoDBConfig(
        connect_uri="mongodb://localhost:27017",
        enableSchemaInference=True,
        maxSchemaSize=3,
    )

    source = MongoDBSource(ctx=pipeline_context, config=config)

    workunits = list(source.get_workunits_internal())

    schema_metadata_aspects = [
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(SchemaMetadataClass))
    ]

    assert len(schema_metadata_aspects) == 1
    schema_metadata = schema_metadata_aspects[0]

    field_paths = [field.fieldPath for field in schema_metadata.fields]
    assert field_paths == [
        "metadata",
        "metadata.properties",
        "metadata.properties.name",
    ]

    dataset_properties_aspects = [
        properties_aspect
        for wu in workunits
        if (properties_aspect := wu.get_aspect_of_type(DatasetPropertiesClass))
    ]

    assert len(dataset_properties_aspects) == 1
    custom_props = dataset_properties_aspects[0].customProperties
    assert "schema.downsampled" in custom_props
    assert custom_props["schema.downsampled"] == "True"


def test_mongodb_schema_inference_with_deeply_nested_structures(
    mock_mongo_client, pipeline_context
):
    """
    Test schema inference with deeply nested objects and arrays.

    Ensures parent counts bubble up correctly through multiple levels.
    """
    mock_mongo_client.list_database_names.return_value = ["test_db"]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = ["events"]

    mock_collection = MagicMock()
    mock_database.__getitem__.return_value = mock_collection

    mock_documents = [
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439011"),
            "metadata": {
                "source": "web",
                "properties": [
                    {"name": "user_agent", "value": "Mozilla/5.0"},
                    {"name": "ip_address", "value": "192.168.1.1"},
                ],
                "tags": {"foo": "bar"},
            },
        },
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439012"),
            "metadata": {
                "source": "mobile",
                "properties": [
                    {"name": "device_id", "value": "abc123"},
                ],
            },
        },
    ]

    mock_collection.aggregate.return_value = mock_documents

    config = MongoDBConfig(
        connect_uri="mongodb://localhost:27017", enableSchemaInference=True
    )
    source = MongoDBSource(ctx=pipeline_context, config=config)

    workunits = list(source.get_workunits_internal())

    schema_metadata_aspects = [
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(SchemaMetadataClass))
    ]

    assert len(schema_metadata_aspects) == 1

    schema_metadata = schema_metadata_aspects[0]
    field_paths = {field.fieldPath for field in schema_metadata.fields}
    expected_paths = {
        "metadata",
        "metadata.source",
        "metadata.properties",
        "metadata.properties.name",
        "metadata.properties.value",
        "metadata.tags",
        "metadata.tags.foo",
        "_id",
    }
    assert field_paths == expected_paths


def test_mongodb_schema_inference_disabled(mock_mongo_client, pipeline_context):
    mock_mongo_client.list_database_names.return_value = ["test_db"]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = ["collection1"]

    mock_collection = MagicMock()
    mock_database.__getitem__.return_value = mock_collection

    mock_documents = [
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439011"),
            "metadata": {
                "source": "web",
                "properties": [
                    {"name": "user_agent", "value": "Mozilla/5.0"},
                    {"name": "ip_address", "value": "192.168.1.1"},
                ],
                "tags": {"foo": "bar"},
            },
        },
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439012"),
            "metadata": {
                "source": "mobile",
                "properties": [
                    {"name": "device_id", "value": "abc123"},
                ],
            },
        },
    ]

    mock_collection.aggregate.return_value = mock_documents

    config = MongoDBConfig(
        connect_uri="mongodb://localhost:27017", enableSchemaInference=False
    )
    source = MongoDBSource(ctx=pipeline_context, config=config)

    workunits = list(source.get_workunits_internal())

    schema_metadata_aspects = [
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(SchemaMetadataClass))
    ]

    assert len(schema_metadata_aspects) == 0


def test_mongodb_schema_field_ordering_with_arrays(mock_mongo_client, pipeline_context):
    """
    Test that schema fields are ordered correctly with parent fields before children.

    This is critical for the MongoDB fix that ensures parent containers appear
    before their nested children in the UI, especially when schemas are trimmed.
    """
    mock_mongo_client.list_database_names.return_value = ["test_db"]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = ["orders"]

    mock_collection = MagicMock()
    mock_database.__getitem__.return_value = mock_collection

    mock_documents = [
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439011"),
            "order_id": "ORD-001",
            "items": [
                {"sku": "ITEM-A", "quantity": 2, "price": 10.50},
                {"sku": "ITEM-B", "quantity": 1, "price": 25.00},
            ],
        },
        {
            "_id": bson.ObjectId("507f1f77bcf86cd799439012"),
            "order_id": "ORD-002",
            "items": [
                {"sku": "ITEM-C", "quantity": 3, "price": 15.75},
            ],
        },
    ]

    mock_collection.aggregate.return_value = mock_documents

    config = MongoDBConfig(
        connect_uri="mongodb://localhost:27017", enableSchemaInference=True
    )
    source = MongoDBSource(ctx=pipeline_context, config=config)

    workunits = list(source.get_workunits_internal())

    schema_metadata_aspects = [
        aspect
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(SchemaMetadataClass))
    ]

    assert len(schema_metadata_aspects) == 1

    schema_metadata = schema_metadata_aspects[0]
    field_paths = [field.fieldPath for field in schema_metadata.fields]

    items_index = field_paths.index("items")
    items_sku_index = field_paths.index("items.sku")
    items_quantity_index = field_paths.index("items.quantity")
    items_price_index = field_paths.index("items.price")

    assert items_index < items_sku_index
    assert items_index < items_quantity_index
    assert items_index < items_price_index


def test_mongodb_database_filtering(mock_mongo_client, pipeline_context):
    mock_mongo_client.list_database_names.return_value = [
        "prod_db",
        "prod_instances",
        "test_db",
        "dev_db",
        "admin",
        "config",
        "local",
    ]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = []

    config = MongoDBConfig(
        connect_uri="mongodb://localhost:27017",
        enableSchemaInference=True,
        database_pattern={"allow": ["^prod_.*"]},
    )
    source = MongoDBSource(ctx=pipeline_context, config=config)

    workunits = list(source.get_workunits_internal())
    databases_ingested: List[str] = [
        aspect.name
        for wu in workunits
        if (aspect := wu.get_aspect_of_type(ContainerPropertiesClass))
    ]
    assert databases_ingested == ["prod_db", "prod_instances"]

    filtered_list = list(source.report.filtered)
    assert "test_db" in filtered_list
    assert "dev_db" in filtered_list

    # internal databases, skipped by default, not reported
    assert "admin" not in filtered_list
    assert "config" not in filtered_list
    assert "local" not in filtered_list

    # allowed databases
    assert "prod_db" not in filtered_list
    assert "prod_instances" not in filtered_list


def test_mongodb_collection_filtering(mock_mongo_client, pipeline_context):
    mock_mongo_client.list_database_names.return_value = ["test_db"]

    mock_database = MagicMock()
    mock_mongo_client.__getitem__.return_value = mock_database
    mock_database.list_collection_names.return_value = [
        "users",
        "orders",
        "temp_data",
        "cache_entries",
    ]

    mock_collection = MagicMock()
    mock_database.__getitem__.return_value = mock_collection
    mock_collection.aggregate.return_value = []

    config = MongoDBConfig(
        connect_uri="mongodb://localhost:27017",
        enableSchemaInference=True,
        collection_pattern={"deny": ["test_db\\.(temp_.*|cache_.*)"]},
    )
    source = MongoDBSource(ctx=pipeline_context, config=config)

    workunits = list(source.get_workunits_internal())

    dataset_urns = {
        wu.metadata.entityUrn
        for wu in workunits
        if isinstance(
            wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
        )
        and wu.metadata.entityUrn
        and guess_entity_type(wu.metadata.entityUrn) == "dataset"
    }

    assert dataset_urns == {
        "urn:li:dataset:(urn:li:dataPlatform:mongodb,test_db.orders,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:mongodb,test_db.users,PROD)",
    }

    filtered_list = list(source.report.filtered)
    assert "test_db.temp_data" in filtered_list
    assert "test_db.cache_entries" in filtered_list
