"""Tests for DocumentEntityBuilder import mode handling."""

from datahub.ingestion.source.unstructured.config import (
    DocumentMappingConfig,
    TitleExtractionConfig,
)
from datahub.ingestion.source.unstructured.document_builder import (
    DocumentEntityBuilder,
    TitleExtractor,
)
from datahub.metadata.schema_classes import DocumentSourceTypeClass

# A representative Notion page ID: the unstructured connector hardcodes the
# per-page filename to "{page_id}.html", so this is what the filename fallback
# would otherwise surface as the title.
_NOTION_PAGE_ID = "2aafc6a6427780d1b553d4afb741bf22"


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


def _title_extractor() -> TitleExtractor:
    return TitleExtractor(TitleExtractionConfig())


def test_extract_title_prefers_notion_url_slug() -> None:
    title = _title_extractor().extract_title(
        elements=[{"type": "NarrativeText", "text": "some body text"}],
        filename=f"{_NOTION_PAGE_ID}.html",
        metadata={
            "data_source": {
                "url": f"https://www.notion.so/Patching-upsert-in-SDK-v2-{_NOTION_PAGE_ID}"
            }
        },
    )
    assert title == "Patching upsert in SDK v2"


def test_extract_title_falls_back_to_heading_when_url_has_no_slug() -> None:
    # Bare-ID Notion URL carries no human-readable slug; a Header in the body
    # should be used instead of the raw page ID filename.
    title = _title_extractor().extract_title(
        elements=[
            {"type": "Header", "text": "Quarterly Planning"},
            {"type": "NarrativeText", "text": "details"},
        ],
        filename=f"{_NOTION_PAGE_ID}.html",
        metadata={"data_source": {"url": f"https://www.notion.so/{_NOTION_PAGE_ID}"}},
    )
    assert title == "Quarterly Planning"


def test_extract_title_never_returns_raw_page_id() -> None:
    # No URL slug and no heading/text content: the filename is a raw Notion page
    # ID, which must not be surfaced as the title.
    title = _title_extractor().extract_title(
        elements=[],
        filename=f"{_NOTION_PAGE_ID}.html",
        metadata={"data_source": {"url": f"https://www.notion.so/{_NOTION_PAGE_ID}"}},
    )
    assert title == "Untitled Document"
    assert _NOTION_PAGE_ID not in title


def test_extract_title_uses_leading_text_over_page_id() -> None:
    title = _title_extractor().extract_title(
        elements=[{"type": "NarrativeText", "text": "Meeting notes from Monday"}],
        filename=f"{_NOTION_PAGE_ID}.html",
        metadata=None,
    )
    assert title == "Meeting notes from Monday"


def test_extract_title_keeps_human_readable_filename() -> None:
    # Non-ID filenames (e.g. GitHub-style paths) are still valid title fallbacks.
    title = _title_extractor().extract_title(
        elements=[],
        filename="getting-started.md",
        metadata=None,
    )
    assert title == "getting-started"
