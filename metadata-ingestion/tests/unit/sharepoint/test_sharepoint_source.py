from datetime import datetime, timezone
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sharepoint.sharepoint_client import (
    SharePointClientError,
    SharePointDrive,
    SharePointItem,
    SharePointPage,
    SharePointSite,
)
from datahub.ingestion.source.sharepoint.sharepoint_config import SharePointSourceConfig
from datahub.ingestion.source.sharepoint.sharepoint_source import (
    SharePointSource,
    _strip_html,
)


def _config(**overrides: object) -> SharePointSourceConfig:
    base: Dict[str, object] = {
        "auth": {
            "tenant_id": "tenant-abc",
            "client_id": "app-123",
            "client_secret": "secret-xyz",
        },
        "site": {
            "hostname": "myorg.sharepoint.com",
        },
    }
    base.update(overrides)
    return SharePointSourceConfig.model_validate(base)


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(run_id="test-run")


def _make_site(
    site_id: str = "site-1",
    name: str = "Engineering",
    path: str = "/sites/Engineering",
) -> SharePointSite:
    return SharePointSite(
        id=site_id,
        name=name,
        display_name=name,
        web_url=f"https://myorg.sharepoint.com{path}",
        server_relative_url=path,
    )


def _make_drive(
    drive_id: str = "drive-1",
    name: str = "Documents",
    drive_type: str = "documentLibrary",
) -> SharePointDrive:
    return SharePointDrive(
        id=drive_id,
        name=name,
        drive_type=drive_type,
        web_url=f"https://myorg.sharepoint.com/sites/Eng/{name}",
    )


def _make_item(
    item_id: str = "item-1",
    name: str = "data.csv",
    size: int = 1024,
) -> SharePointItem:
    return SharePointItem(
        id=item_id,
        name=name,
        web_url=f"https://myorg.sharepoint.com/sites/Eng/Docs/{name}",
        size=size,
        last_modified=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        mime_type="text/csv",
        download_url="https://download.example.com/data.csv",
        parent_path="",
        is_folder=False,
    )


def _make_page(
    page_id: str = "page-1",
    title: str = "Welcome",
    site_path: str = "/sites/Engineering",
) -> SharePointPage:
    return SharePointPage(
        id=page_id,
        title=title,
        web_url=f"https://myorg.sharepoint.com{site_path}/SitePages/{title}.aspx",
        last_modified=datetime(2024, 6, 1, tzinfo=timezone.utc),
        created=datetime(2024, 5, 1, tzinfo=timezone.utc),
        content_html="",
    )


def _collect_workunits(source: SharePointSource) -> List[MetadataWorkUnit]:
    return list(source.get_workunits())


def test_strip_html_removes_tags() -> None:
    html = "<h1>Title</h1><p>Some <strong>text</strong> here.</p>"
    result = _strip_html(html)
    assert "<" not in result
    assert "Title" in result
    assert "text" in result


def test_strip_html_collapses_whitespace() -> None:
    html = "<p>  Lots   of   spaces  </p>"
    result = _strip_html(html)
    assert "  " not in result
    assert "Lots of spaces" in result


def test_strip_html_empty_string() -> None:
    assert _strip_html("") == ""


def test_source_initialises_data_lake_mode(ctx: PipelineContext) -> None:
    cfg = _config(mode="data_lake")
    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ):
        source = SharePointSource(cfg, ctx)
    assert source.platform == "sharepoint"
    assert source.document_builder is None
    assert source.chunking_source is None


def test_source_initialises_document_mode(ctx: PipelineContext) -> None:
    cfg = _config(mode="document")
    # DocumentChunkingSource is imported inline inside __init__, so patch at its origin.
    with (
        patch("datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentChunkingSource"
        ),
    ):
        source = SharePointSource(cfg, ctx)
    assert source.platform == "sharepoint"


def test_source_create_factory(ctx: PipelineContext) -> None:
    config_dict = {
        "auth": {"tenant_id": "t", "client_id": "c", "client_secret": "s"},
        "site": {"hostname": "myorg.sharepoint.com"},
        "mode": "data_lake",
    }
    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ):
        source = SharePointSource.create(config_dict, ctx)
    assert isinstance(source, SharePointSource)


def test_data_lake_emits_dataset_workunit(ctx: PipelineContext) -> None:
    cfg = _config(mode="data_lake", file_types=["csv"])
    site = _make_site()
    drive = _make_drive()
    item = _make_item()

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
        mock_client.list_items_recursive.return_value = iter([item])
        mock_client.download_file_bytes.return_value = b"col1,col2\n1,2\n3,4\n"

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        workunits = list(source.get_workunits_internal())

    urn_wus = [wu for wu in workunits if "urn:li:dataset:" in wu.id]
    assert len(urn_wus) >= 1


def test_data_lake_skips_unsupported_extension(ctx: PipelineContext) -> None:
    cfg = _config(mode="data_lake", file_types=["csv"])
    site = _make_site()
    drive = _make_drive()
    docx_item = _make_item(name="report.docx")

    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter([docx_item])

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        list(source.get_workunits_internal())

    assert source.report.files_skipped == 1
    assert source.report.files_processed == 0


def test_data_lake_max_files_enforced(ctx: PipelineContext) -> None:
    cfg = _config(mode="data_lake", file_types=["csv"], max_files=2)
    site = _make_site()
    drive = _make_drive()
    items = [_make_item(item_id=f"item-{i}", name=f"file{i}.csv") for i in range(5)]

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
        mock_client.download_file_bytes.return_value = b"a,b\n1,2\n"

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        list(source.get_workunits_internal())

    assert source.report.files_processed <= 2


def test_data_lake_site_pattern_filters_sites(ctx: PipelineContext) -> None:
    cfg = _config(
        mode="data_lake",
        file_types=["csv"],
        site={
            "hostname": "myorg.sharepoint.com",
            "site_pattern": {"allow": ["/sites/Engineering"]},
        },
    )
    eng_site = _make_site(path="/sites/Engineering")
    hr_site = _make_site(site_id="site-2", name="HR", path="/sites/HR")

    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([eng_site, hr_site])
        mock_client.list_drives.return_value = iter([])
        mock_client.list_items_recursive.return_value = iter([])

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        list(source.get_workunits_internal())

    assert source.report.sites_scanned == 2
    assert source.report.sites_processed == 1


def test_document_mode_emits_document_workunit(ctx: PipelineContext) -> None:
    cfg = _config(mode="document", ingest_pages=True, ingest_files=False)
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
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentChunkingSource"
        ),
    ):
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_pages.return_value = iter([page])
        mock_client.get_page_html.return_value = "<p>Welcome to Engineering Hub</p>"

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking

        workunits = list(source.get_workunits_internal())

    assert len(workunits) >= 1
    assert source.report.pages_scanned == 1
    assert source.report.pages_processed == 1


def test_document_mode_max_documents_enforced(ctx: PipelineContext) -> None:
    cfg = _config(
        mode="document",
        ingest_pages=True,
        ingest_files=False,
        max_documents=1,
    )
    site = _make_site()
    pages = [_make_page(page_id=f"p{i}", title=f"Page {i}") for i in range(3)]

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
        mock_client.list_pages.return_value = iter(pages)
        mock_client.get_page_html.return_value = "<p>Content</p>"

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking

        with pytest.raises(RuntimeError, match="Document limit of 1 reached"):
            list(source.get_workunits_internal())

    assert source.report.num_documents_limit_reached


def test_document_mode_page_failure_continues(ctx: PipelineContext) -> None:
    cfg = _config(
        mode="document",
        ingest_pages=True,
        ingest_files=False,
        advanced={"continue_on_failure": True},
    )
    site = _make_site()
    good_page = _make_page(page_id="good", title="Good Page")
    bad_page = _make_page(page_id="bad", title="Bad Page")

    mock_chunking = MagicMock()
    mock_chunking.report = MagicMock(
        num_documents_with_embeddings=0,
        num_embedding_failures=0,
        embedding_failures=[],
        num_documents_limit_reached=False,
    )
    mock_chunking.process_elements_inline.return_value = iter([])

    def fake_get_html(site_id: str, page_id: str) -> str:
        if page_id == "bad":
            raise ValueError("Simulated failure")
        return "<p>Good content</p>"

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
        mock_client.list_pages.return_value = iter([bad_page, good_page])
        mock_client.get_page_html.side_effect = fake_get_html

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        source.chunking_source = mock_chunking

        list(source.get_workunits_internal())

    assert source.report.pages_failed == 1
    assert source.report.pages_processed == 1


def test_test_connection_success() -> None:
    config_dict = {
        "auth": {"tenant_id": "t", "client_id": "c", "client_secret": "s"},
        "site": {"hostname": "myorg.sharepoint.com"},
    }
    mock_site = _make_site()

    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.test_connectivity.return_value = None
        mock_client.list_sites.return_value = iter([mock_site])

        report = SharePointSource.test_connection(config_dict)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable


def test_test_connection_invalid_config() -> None:
    report = SharePointSource.test_connection({"auth": {}})
    assert report.internal_failure


def test_test_connection_auth_failure() -> None:
    config_dict = {
        "auth": {"tenant_id": "t", "client_id": "c", "client_secret": "bad-secret"},
        "site": {"hostname": "myorg.sharepoint.com"},
    }

    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.test_connectivity.side_effect = SharePointClientError(
            401, "Unauthorized"
        )

        report = SharePointSource.test_connection(config_dict)

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable


def test_report_aggregates_embedding_stats(ctx: PipelineContext) -> None:
    cfg = _config(mode="document")

    mock_chunking = MagicMock()
    mock_chunking.report = MagicMock(
        num_documents_with_embeddings=5,
        num_embedding_failures=1,
        embedding_failures=["doc-1"],
    )

    with (
        patch("datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
    ):
        source = SharePointSource(cfg, ctx)
        source.chunking_source = mock_chunking

    report = source.get_report()
    assert report.num_documents_with_embeddings == 5
    assert report.num_embedding_failures == 1


def test_personal_sites_skipped_by_default(ctx: PipelineContext) -> None:
    cfg = _config(mode="data_lake", file_types=["csv"])
    personal_site = _make_site(
        site_id="personal-1",
        name="jane_doe",
        path="/personal/jane_doe_contoso_com",
    )
    regular_site = _make_site()
    drive = _make_drive()
    item = _make_item()

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
        mock_client.list_sites.return_value = iter([personal_site, regular_site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter([item])

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        list(source.get_workunits_internal())

    assert source.report.sites_scanned == 2
    assert source.report.sites_processed == 1


def test_personal_sites_included_when_disabled(ctx: PipelineContext) -> None:
    cfg = _config(
        mode="data_lake",
        file_types=["csv"],
        site={"hostname": "myorg.sharepoint.com", "skip_personal_sites": False},
    )
    personal_site = _make_site(
        site_id="personal-1",
        name="jane_doe",
        path="/personal/jane_doe_contoso_com",
    )
    drive = _make_drive()
    item = _make_item()

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
        mock_client.list_sites.return_value = iter([personal_site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter([item])

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        list(source.get_workunits_internal())

    assert source.report.sites_processed == 1


def test_pdf_file_produces_dataset_without_schema_download(
    ctx: PipelineContext,
) -> None:
    cfg = _config(mode="data_lake", file_types=["csv", "pdf"])
    site = _make_site()
    drive = _make_drive()
    pdf_item = SharePointItem(
        id="pdf-1",
        name="report.pdf",
        web_url="https://myorg.sharepoint.com/sites/Eng/Docs/report.pdf",
        size=50000,
        last_modified=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
        created=datetime(2024, 1, 1, tzinfo=timezone.utc),
        mime_type="application/pdf",
        download_url="https://download.example.com/report.pdf",
        parent_path="",
        is_folder=False,
    )

    with patch(
        "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"
    ) as MockClient:
        mock_client = MockClient.return_value
        mock_client.list_sites.return_value = iter([site])
        mock_client.list_drives.return_value = iter([drive])
        mock_client.list_items_recursive.return_value = iter([pdf_item])

        source = SharePointSource(cfg, ctx)
        source.client = mock_client
        workunits = list(source.get_workunits_internal())

    dataset_wus = [wu for wu in workunits if "urn:li:dataset:" in wu.id]
    assert len(dataset_wus) >= 1
    mock_client.download_file_bytes.assert_not_called()


def test_source_initialises_both_mode(ctx: PipelineContext) -> None:
    cfg = _config(mode="both")
    with (
        patch("datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentChunkingSource"
        ),
    ):
        source = SharePointSource(cfg, ctx)
    assert source.document_builder is not None
    assert source.chunking_source is not None


def test_both_mode_calls_both_workunits(ctx: PipelineContext) -> None:
    cfg = _config(mode="both", file_types=["csv"])

    with (
        patch("datahub.ingestion.source.sharepoint.sharepoint_source.SharePointClient"),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentEntityBuilder"
        ),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.DocumentChunkingSource"
        ),
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointSource._get_data_lake_workunits"
        ) as mock_data_lake,
        patch(
            "datahub.ingestion.source.sharepoint.sharepoint_source.SharePointSource._get_document_workunits"
        ) as mock_document,
    ):
        mock_data_lake.return_value = iter([])
        mock_document.return_value = iter([])
        source = SharePointSource(cfg, ctx)
        list(source.get_workunits_internal())

    mock_data_lake.assert_called_once()
    mock_document.assert_called_once()
