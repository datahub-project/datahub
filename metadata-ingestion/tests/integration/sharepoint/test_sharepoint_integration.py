from datetime import datetime, timezone
from typing import Dict, Iterable, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sharepoint.sharepoint_client import (
    SharePointDrive,
    SharePointItem,
    SharePointPage,
    SharePointSite,
)
from datahub.ingestion.source.sharepoint.sharepoint_config import SharePointSourceConfig
from datahub.ingestion.source.sharepoint.sharepoint_source import SharePointSource

pytestmark = pytest.mark.integration


def _make_config(**overrides: object) -> SharePointSourceConfig:
    base: Dict[str, object] = {
        "auth": {
            "tenant_id": "tenant-integration",
            "client_id": "app-integration",
            "client_secret": "secret-integration",
        },
        "site": {
            "hostname": "testorg.sharepoint.com",
        },
    }
    base.update(overrides)
    return SharePointSourceConfig.model_validate(base)


def _make_site() -> SharePointSite:
    return SharePointSite(
        id="site-integration",
        name="DataEngineering",
        display_name="Data Engineering",
        web_url="https://testorg.sharepoint.com/sites/DataEngineering",
        server_relative_url="/sites/DataEngineering",
    )


def _make_drive() -> SharePointDrive:
    return SharePointDrive(
        id="drive-integration",
        name="RawData",
        drive_type="documentLibrary",
        web_url="https://testorg.sharepoint.com/sites/DataEngineering/RawData",
    )


def _make_csv_item() -> SharePointItem:
    return SharePointItem(
        id="item-csv",
        name="customers.csv",
        web_url="https://testorg.sharepoint.com/sites/DataEngineering/RawData/customers.csv",
        size=512,
        last_modified=datetime(2024, 4, 1, tzinfo=timezone.utc),
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        mime_type="text/csv",
        download_url="https://download.example.com/customers.csv",
        parent_path="",
        is_folder=False,
    )


def _make_xlsx_item() -> SharePointItem:
    return SharePointItem(
        id="item-xlsx",
        name="sales_report.xlsx",
        web_url="https://testorg.sharepoint.com/sites/DataEngineering/RawData/sales_report.xlsx",
        size=2048,
        last_modified=datetime(2024, 4, 15, tzinfo=timezone.utc),
        created=datetime(2024, 2, 1, tzinfo=timezone.utc),
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        download_url="https://download.example.com/sales_report.xlsx",
        parent_path="",
        is_folder=False,
    )


def _make_page() -> SharePointPage:
    return SharePointPage(
        id="page-integration",
        title="Data Engineering Overview",
        web_url="https://testorg.sharepoint.com/sites/DataEngineering/SitePages/Overview.aspx",
        last_modified=datetime(2024, 5, 1, tzinfo=timezone.utc),
        created=datetime(2024, 3, 1, tzinfo=timezone.utc),
        content_html="",
    )


def _collect_workunits(source: SharePointSource) -> List[MetadataWorkUnit]:
    return list(source.get_workunits())


def _get_aspect_types(workunits: List[MetadataWorkUnit]) -> List[str]:
    types = []
    for wu in workunits:
        mcp = wu.metadata
        if hasattr(mcp, "aspect"):
            aspect = mcp.aspect
            if aspect is not None:
                types.append(type(aspect).__name__)
    return types


def test_data_lake_mode_csv_pipeline() -> None:
    config = _make_config(mode="data_lake", file_types=["csv", "xlsx"])
    ctx = PipelineContext(run_id="integration-data-lake")

    site = _make_site()
    drive = _make_drive()
    csv_item = _make_csv_item()
    csv_content = (
        b"customer_id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n"
    )

    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter([csv_item])
        mock_client.download_file_bytes.return_value = csv_content

        source = SharePointSource(config, ctx)
        source.client = mock_client
        workunits = _collect_workunits(source)

    aspect_types = _get_aspect_types(workunits)
    assert "DataPlatformInfoClass" in aspect_types
    assert "DatasetPropertiesClass" in aspect_types

    container_wus = [wu for wu in workunits if "urn:li:container:" in wu.id]
    assert len(container_wus) >= 1

    dataset_wus = [wu for wu in workunits if "urn:li:dataset:" in wu.id]
    assert len(dataset_wus) >= 1
    assert any("customers.csv" in wu.id for wu in dataset_wus)


def test_data_lake_mode_xlsx_pipeline() -> None:
    config = _make_config(mode="data_lake", file_types=["xlsx"])
    ctx = PipelineContext(run_id="integration-xlsx")

    site = _make_site()
    drive = _make_drive()
    xlsx_item = _make_xlsx_item()

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.infer_schema",
            return_value=None,
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter([xlsx_item])
        mock_client.download_file_bytes.return_value = b""

        source = SharePointSource(config, ctx)
        source.client = mock_client
        workunits = _collect_workunits(source)

    aspect_types = _get_aspect_types(workunits)
    assert "DatasetPropertiesClass" in aspect_types


def test_data_lake_mode_container_hierarchy() -> None:
    config = _make_config(mode="data_lake", file_types=["csv"])
    ctx = PipelineContext(run_id="integration-containers")

    site = _make_site()
    drive = _make_drive()
    nested_item = SharePointItem(
        id="item-nested",
        name="archive.csv",
        web_url="https://testorg.sharepoint.com/sites/DataEngineering/RawData/2024/Q1/archive.csv",
        size=256,
        last_modified=datetime(2024, 4, 1, tzinfo=timezone.utc),
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        mime_type="text/csv",
        download_url="https://download.example.com/archive.csv",
        parent_path="2024/Q1",
        is_folder=False,
    )

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.infer_schema",
            return_value=None,
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter([nested_item])
        mock_client.download_file_bytes.return_value = b"a,b\n"

        source = SharePointSource(config, ctx)
        source.client = mock_client
        workunits = _collect_workunits(source)

    container_wus = [wu for wu in workunits if "urn:li:container:" in wu.id]
    # site container, library container, folder "2024", folder "Q1"
    assert len(container_wus) >= 4


def test_data_lake_mode_library_pattern_filter() -> None:
    config = _make_config(
        mode="data_lake",
        file_types=["csv"],
        site={
            "hostname": "testorg.sharepoint.com",
            "library_pattern": {"deny": ["^Style Library$", "^SiteAssets$"]},
        },
    )
    ctx = PipelineContext(run_id="integration-library-filter")

    site = _make_site()
    allowed_drive = _make_drive()
    excluded_drive = SharePointDrive(
        id="drive-styles",
        name="Style Library",
        drive_type="documentLibrary",
        web_url="https://testorg.sharepoint.com/sites/DataEngineering/Style%20Library",
    )

    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_drives.return_value = iter([allowed_drive, excluded_drive])
        mock_client.list_items_recursive.return_value = iter([])

        source = SharePointSource(config, ctx)
        source.client = mock_client
        _collect_workunits(source)

    assert source.report.libraries_processed == 1
    assert source.report.libraries_scanned == 2


def test_data_lake_mode_file_download_failure_continues() -> None:
    config = _make_config(
        mode="data_lake",
        file_types=["csv"],
        advanced={"continue_on_failure": True},
    )
    ctx = PipelineContext(run_id="integration-download-fail")

    site = _make_site()
    drive = _make_drive()
    items = [
        _make_csv_item(),
        SharePointItem(
            id="item-bad",
            name="broken.csv",
            web_url="https://example.com/broken.csv",
            size=100,
            last_modified=datetime(2024, 4, 1, tzinfo=timezone.utc),
            created=datetime(2024, 1, 1, tzinfo=timezone.utc),
            mime_type="text/csv",
            download_url="https://download.example.com/broken.csv",
            parent_path="",
            is_folder=False,
        ),
    ]

    def fake_download(url: str) -> bytes:
        if "broken" in url:
            raise ConnectionError("Simulated download failure")
        return b"id,name\n1,Alice\n"

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.infer_schema",
            return_value=None,
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter(items)
        mock_client.download_file_bytes.side_effect = fake_download

        source = SharePointSource(config, ctx)
        source.client = mock_client
        _collect_workunits(source)

    assert source.report.files_scanned == 2
    assert source.report.files_processed >= 1


def test_document_mode_page_pipeline() -> None:
    config = _make_config(mode="document", ingest_pages=True, ingest_files=False)
    ctx = PipelineContext(run_id="integration-document-page")

    site = _make_site()
    page = _make_page()
    page_html = "<h1>Data Engineering Overview</h1><p>This team builds pipelines.</p>"

    mock_chunking = MagicMock()
    mock_chunking.report = MagicMock(
        num_documents_with_embeddings=0,
        num_embedding_failures=0,
        embedding_failures=[],
        num_documents_limit_reached=False,
    )
    mock_chunking.process_elements_inline.return_value = iter([])

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_pages.return_value = iter([page])
        mock_client.get_page_html.return_value = page_html

        source = SharePointSource(config, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking
        workunits = _collect_workunits(source)

    aspect_types = _get_aspect_types(workunits)
    assert "DataPlatformInfoClass" in aspect_types

    document_wus = [wu for wu in workunits if "urn:li:document:" in wu.id]
    assert len(document_wus) >= 1
    assert any("page-integration" in wu.id for wu in document_wus)

    assert source.report.pages_scanned == 1
    assert source.report.pages_processed == 1


def test_document_mode_page_platform_instance() -> None:
    config = _make_config(
        mode="document",
        ingest_pages=True,
        ingest_files=False,
        platform_instance="testorg",
    )
    ctx = PipelineContext(run_id="integration-platform-instance")

    site = _make_site()
    page = _make_page()

    mock_chunking = MagicMock()
    mock_chunking.report = MagicMock(
        num_documents_with_embeddings=0,
        num_embedding_failures=0,
        embedding_failures=[],
        num_documents_limit_reached=False,
    )
    mock_chunking.process_elements_inline.return_value = iter([])

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_pages.return_value = iter([page])
        mock_client.get_page_html.return_value = "<p>Content</p>"

        source = SharePointSource(config, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking
        workunits = _collect_workunits(source)

    aspect_types = _get_aspect_types(workunits)
    assert "DataPlatformInstanceClass" in aspect_types


def test_document_mode_browse_path_emitted() -> None:
    config = _make_config(mode="document", ingest_pages=True, ingest_files=False)
    ctx = PipelineContext(run_id="integration-browse-path")

    site = _make_site()
    page = _make_page()

    mock_chunking = MagicMock()
    mock_chunking.report = MagicMock(
        num_documents_with_embeddings=0,
        num_embedding_failures=0,
        embedding_failures=[],
        num_documents_limit_reached=False,
    )
    mock_chunking.process_elements_inline.return_value = iter([])

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_pages.return_value = iter([page])
        mock_client.get_page_html.return_value = "<p>Content</p>"

        source = SharePointSource(config, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking
        workunits = _collect_workunits(source)

    aspect_types = _get_aspect_types(workunits)
    assert "BrowsePathsV2Class" in aspect_types


def test_document_mode_page_html_stripped() -> None:
    config = _make_config(mode="document", ingest_pages=True, ingest_files=False)
    ctx = PipelineContext(run_id="integration-html-strip")

    site = _make_site()
    page = _make_page()
    html_with_tags = "<h1>Title</h1><p>Important <strong>content</strong> here.</p>"

    mock_chunking = MagicMock()
    mock_chunking.report = MagicMock(
        num_documents_with_embeddings=0,
        num_embedding_failures=0,
        embedding_failures=[],
        num_documents_limit_reached=False,
    )

    captured_text = []

    def fake_process_inline(document_urn: str, elements: list) -> Iterable:
        for e in elements:
            captured_text.append(e.get("text", ""))
        return iter([])

    mock_chunking.process_elements_inline.side_effect = fake_process_inline

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_pages.return_value = iter([page])
        mock_client.get_page_html.return_value = html_with_tags

        source = SharePointSource(config, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking
        _collect_workunits(source)

    combined = " ".join(captured_text)
    assert "<" not in combined
    assert "Title" in combined
    assert "content" in combined


def test_document_mode_no_pages_when_api_fails_gracefully() -> None:
    config = _make_config(mode="document", ingest_pages=True, ingest_files=False)
    ctx = PipelineContext(run_id="integration-pages-api-fail")

    site = _make_site()

    mock_chunking = MagicMock()
    mock_chunking.report = MagicMock(
        num_documents_with_embeddings=0,
        num_embedding_failures=0,
        embedding_failures=[],
        num_documents_limit_reached=False,
    )
    mock_chunking.process_elements_inline.return_value = iter([])

    with (
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
        ) as MockClient,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_pages.return_value = iter([])

        source = SharePointSource(config, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking
        workunits = _collect_workunits(source)

    aspect_types = _get_aspect_types(workunits)
    assert "DataPlatformInfoClass" in aspect_types
    assert source.report.pages_processed == 0
