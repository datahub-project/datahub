"""Tests for DocumentEntityBuilder import mode handling."""

from datahub.ingestion.source.unstructured.config import DocumentMappingConfig
from datahub.ingestion.source.unstructured.document_builder import DocumentEntityBuilder
from datahub.metadata.schema_classes import DocumentSourceTypeClass


def test_build_document_entity_native_source_type() -> None:
    config = DocumentMappingConfig()
    config.source.type = "NATIVE"
    builder = DocumentEntityBuilder(config=config, source_type="notion")

    doc = builder.build_document_entity(
        elements=[{"text": "Hello world", "type": "Title"}],
        metadata={"filename": "page.html", "file_directory": ""},
        custom_properties={"ingestion_source": "notion"},
    )

    document_info = doc._get_document_info()
    assert document_info is not None
    assert document_info.source is not None
    assert document_info.source.sourceType == DocumentSourceTypeClass.NATIVE
    assert document_info.source.externalUrl is None


def test_build_document_entity_external_source_type() -> None:
    config = DocumentMappingConfig()
    config.source.type = "EXTERNAL"
    builder = DocumentEntityBuilder(config=config, source_type="notion")

    doc = builder.build_document_entity(
        elements=[{"text": "Hello world", "type": "Title"}],
        metadata={
            "filename": "page.html",
            "file_directory": "",
            "data_source": {"url": "https://notion.so/page-123"},
        },
        custom_properties={"ingestion_source": "notion"},
    )

    document_info = doc._get_document_info()
    assert document_info is not None
    assert document_info.source is not None
    assert document_info.source.sourceType == DocumentSourceTypeClass.EXTERNAL
    assert document_info.source.externalUrl == "https://notion.so/page-123"
