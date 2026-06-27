from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.quip.quip_client import (
    QuipFolder,
    QuipThread,
    QuipUser,
)
from datahub.ingestion.source.quip.quip_config import QuipSourceConfig
from datahub.ingestion.source.quip.quip_source import QuipSource


def _config(**overrides: object) -> QuipSourceConfig:
    payload: dict = {"access_token": "secret"}
    payload.update(overrides)
    return QuipSourceConfig.model_validate(payload)


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(run_id="test-run")


def _make_source(ctx: PipelineContext, **overrides: object) -> QuipSource:
    source = QuipSource(_config(**overrides), ctx)
    # The embedding sub-component is exercised separately; stub it so document
    # creation tests don't attempt real embedding calls.
    source.chunking_source = MagicMock()
    source.chunking_source.process_elements_inline.return_value = []
    source.chunking_source.report.num_documents_limit_reached = False
    return source


def test_source_initialization(ctx: PipelineContext) -> None:
    source = _make_source(ctx)
    assert source.platform == "quip"
    assert source.client is not None
    assert source.report is not None


def test_instance_id_uses_platform_instance(ctx: PipelineContext) -> None:
    source = _make_source(ctx, platform_instance="mycompany")
    assert source._get_instance_id() == "mycompany"


def test_instance_id_falls_back_to_url_hash(ctx: PipelineContext) -> None:
    source = _make_source(ctx)
    instance_id = source._get_instance_id()
    assert len(instance_id) == 8
    # Deterministic for a given base_url.
    assert instance_id == _make_source(ctx)._get_instance_id()


def test_doc_id_helpers(ctx: PipelineContext) -> None:
    source = _make_source(ctx, platform_instance="acme")
    assert source._thread_doc_id("ABC") == "quip-acme-ABC"
    assert source._folder_doc_id("F1") == "quip-acme-folder-F1"


def test_root_folder_ids_excludes_archive_and_dedupes() -> None:
    user = QuipUser.model_validate(
        {
            "private_folder_id": "p",
            "desktop_folder_id": "d",
            "shared_folder_ids": ["s1", "s2", "p"],  # p duplicated
            "group_folder_ids": ["g1"],
            "archive_folder_id": "a",
            "starred_folder_id": "st",
            "trash_folder_id": "tr",
        }
    )
    assert user.root_folder_ids == ["p", "d", "s1", "s2", "g1"]


def test_crawl_builds_hierarchy_and_dedupes_threads(ctx: PipelineContext) -> None:
    folders = {
        "root": QuipFolder.model_validate(
            {
                "folder": {"id": "root", "title": "Root"},
                "children": [{"folder_id": "sub"}, {"thread_id": "t1"}],
            }
        ),
        "sub": QuipFolder.model_validate(
            {
                "folder": {"id": "sub", "title": "Sub"},
                # t1 also lives here, but it was first reached via root.
                "children": [{"thread_id": "t2"}, {"thread_id": "t1"}],
            }
        ),
    }
    source = _make_source(ctx, folder_ids=["root"])
    source.client = MagicMock()
    source.client.get_folder.side_effect = lambda fid: folders[fid]

    crawl = source._crawl()

    assert set(crawl.folders) == {"root", "sub"}
    assert crawl.folder_parents == {"root": None, "sub": "root"}
    assert crawl.thread_parents == {"t1": "root", "t2": "sub"}


def test_crawl_non_recursive_skips_subfolders(ctx: PipelineContext) -> None:
    folders = {
        "root": QuipFolder.model_validate(
            {
                "folder": {"id": "root", "title": "Root"},
                "children": [{"folder_id": "sub"}, {"thread_id": "t1"}],
            }
        ),
        "sub": QuipFolder.model_validate(
            {"folder": {"id": "sub"}, "children": [{"thread_id": "t2"}]}
        ),
    }
    source = _make_source(ctx, folder_ids=["root"], recursive=False)
    source.client = MagicMock()
    source.client.get_folder.side_effect = lambda fid: folders[fid]

    crawl = source._crawl()

    assert set(crawl.folders) == {"root"}
    assert crawl.thread_parents == {"t1": "root"}


def test_thread_limit_stops_crawl_recursion(ctx: PipelineContext) -> None:
    folders = {
        "root": QuipFolder.model_validate(
            {
                "folder": {"id": "root", "title": "Root"},
                "children": [{"thread_id": "t1"}, {"folder_id": "sub"}],
            }
        ),
        "sub": QuipFolder.model_validate(
            {"folder": {"id": "sub"}, "children": [{"thread_id": "t2"}]}
        ),
    }
    source = _make_source(ctx, folder_ids=["root"], max_threads=1)
    source.client = MagicMock()
    source.client.get_folder.side_effect = lambda fid: folders[fid]

    crawl = source._crawl()

    # With a budget of one thread, only the root thread is recorded and the
    # sub-folder is never fetched once the limit is hit.
    assert crawl.thread_parents == {"t1": "root"}
    assert "sub" not in crawl.folders
    source.client.get_folder.assert_called_once_with("root")


def test_thread_type_filter_skips_unwanted_types(ctx: PipelineContext) -> None:
    source = _make_source(ctx, thread_types=["document"])
    source.client = MagicMock()
    source.client.get_thread.return_value = QuipThread.model_validate(
        {
            "thread": {
                "id": "t1",
                "title": "Chat",
                "type": "chat",
                "link": "https://q/t1",
            },
            "html": "<p>some chat content that is long enough to pass length filter</p>",
        }
    )

    workunits = list(source._create_thread_document("t1", None, set()))

    assert workunits == []
    assert source.report.threads_skipped == 1


def test_thread_below_min_length_skipped(ctx: PipelineContext) -> None:
    source = _make_source(ctx, filtering={"min_text_length": 100})
    source.client = MagicMock()
    source.client.get_thread.return_value = QuipThread.model_validate(
        {
            "thread": {
                "id": "t1",
                "title": "Tiny",
                "type": "document",
                "link": "https://q/t1",
            },
            "html": "<p>short</p>",
        }
    )

    workunits = list(source._create_thread_document("t1", None, set()))

    assert workunits == []
    assert source.report.threads_skipped == 1


def test_external_thread_document_links_back_to_quip(ctx: PipelineContext) -> None:
    source = _make_source(ctx, platform_instance="acme")
    doc = source._build_thread_document(
        doc_id="quip-acme-t1",
        title="Doc",
        text="hello world",
        url="https://quip.com/t1",
        thread_id="t1",
        subtype="Document",
        parent_urn="urn:li:document:quip-acme-folder-F1",
        custom_properties={},
        created_time=None,
        last_modified_time=None,
    )
    assert doc.is_external
    assert doc.external_url == "https://quip.com/t1"
    assert doc.external_id == "t1"
    assert doc.parent_document == "urn:li:document:quip-acme-folder-F1"


def test_native_thread_document(ctx: PipelineContext) -> None:
    source = _make_source(ctx, document_import_mode="NATIVE")
    doc = source._build_thread_document(
        doc_id="quip-x-t1",
        title="Doc",
        text="hello world",
        url="https://quip.com/t1",
        thread_id="t1",
        subtype="Document",
        parent_urn=None,
        custom_properties={},
        created_time=None,
        last_modified_time=None,
    )
    assert doc.is_native
    assert doc.text == "hello world"


def test_thread_parent_urn_only_for_in_scope_folder(ctx: PipelineContext) -> None:
    source = _make_source(ctx, platform_instance="acme")
    # Folder in scope -> parent points at folder document.
    assert (
        source._thread_parent_urn("F1", {"F1"}) == "urn:li:document:quip-acme-folder-F1"
    )
    # Folder not in scope -> falls back to configured root parent (None here).
    assert source._thread_parent_urn("F9", {"F1"}) is None
