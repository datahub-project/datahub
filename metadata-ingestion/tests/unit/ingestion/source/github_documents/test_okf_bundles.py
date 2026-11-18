from typing import Dict, List, Optional
from unittest.mock import MagicMock

from pydantic import SecretStr

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.github_documents.github_api import (
    GitHubApiClient,
    GitHubFileInfo,
)
from datahub.ingestion.source.github_documents.github_documents_config import (
    GitHubDocumentsSourceConfig,
)
from datahub.ingestion.source.github_documents.github_documents_source import (
    GitHubDocumentsSource,
    compute_source_hash,
)
from datahub.metadata.schema_classes import (
    DocumentInfoClass,
    OkfBundleClass,
    OkfPropertiesClass,
)


def _source(
    files: Dict[str, str],
    *,
    roots: Optional[List[str]] = None,
    listed: Optional[List[str]] = None,
    enable_okf: bool = True,
    path_prefix: str = "",
    max_files: int = 500,
) -> GitHubDocumentsSource:
    source = GitHubDocumentsSource(
        GitHubDocumentsSourceConfig(
            repository="example-org/analytics",
            github_token=SecretStr("test-token"),
            okf_bundle_roots=roots or [],
            enable_okf=enable_okf,
            path_prefix=path_prefix,
            max_files=max_files,
            create_repo_root_document=False,
        ),
        PipelineContext(run_id="bundle-test"),
    )
    client = MagicMock(spec=GitHubApiClient)
    client.index_file_paths = {path for path in files if path.endswith("index.md")}
    client.list_matching_files.return_value = (
        [
            GitHubFileInfo(path=path, size=len(files[path]))
            for path in (listed or list(files))
        ],
        False,
    )
    client.get_latest_commit_sha.return_value = "a" * 40
    client.fetch_file_content.side_effect = lambda repo, path, branch: files.get(path)
    source.client = client
    return source


def _concepts(source: GitHubDocumentsSource) -> Dict[str, OkfPropertiesClass]:
    concepts: Dict[str, OkfPropertiesClass] = {}
    for wu in source.get_workunits():
        aspect = wu.get_aspect_of_type(OkfPropertiesClass)
        if aspect is not None:
            assert aspect.bundle is not None and aspect.conceptId is not None
            path = "/".join(
                part
                for part in [aspect.bundle.rootPath, aspect.conceptId + ".md"]
                if part != "."
            )
            concepts[path] = aspect
    return concepts


def test_bundle_relative_identity_and_deepest_root() -> None:
    source = _source(
        {
            "index.md": '---\nokf_version: "0.2"\n---\nRoot listing',
            "root.md": "---\ntype: Guide\nid: producer-id\n---\nRoot concept",
            "context/index.md": '---\nokf_version: "0.3"\n---\nContext listing',
            "context/guides/mau.md": "---\ntype: Guide\nid: producer-id\n---\nMAU",
            "context/deep/index.md": '---\nokf_version: "0.4"\n---\nNested listing',
            "context/deep/mau.md": "---\ntype: Guide\n---\nNested MAU",
        }
    )
    concepts = _concepts(source)
    assert set(concepts) == {"root.md", "context/guides/mau.md", "context/deep/mau.md"}
    assert concepts["root.md"].conceptId == "root"
    root_bundle = concepts["root.md"].bundle
    assert root_bundle is not None
    assert root_bundle.rootPath == "."
    assert root_bundle.okfVersion == "0.2"
    mau = concepts["context/guides/mau.md"]
    assert mau.conceptId == "guides/mau"
    assert mau.bundle is not None
    assert mau.bundle.location == "https://github.com/example-org/analytics"
    assert mau.bundle.rootPath == "context"
    assert mau.bundle.okfVersion == "0.3"
    nested = concepts["context/deep/mau.md"]
    assert nested.conceptId == "mau"
    assert nested.bundle is not None
    assert nested.bundle.rootPath == "context/deep"
    assert nested.bundle.okfVersion == "0.4"
    assert mau.declaredId == concepts["root.md"].declaredId == "producer-id"
    fetch = source.client.fetch_file_content
    assert isinstance(fetch, MagicMock)
    fetched = [call.args[1] for call in fetch.call_args_list]
    for path in ["index.md", "context/index.md", "context/deep/index.md"]:
        assert fetched.count(path) == 1


def test_only_bundle_concepts_are_parsed_and_reserved_files_remain_documents() -> None:
    index = '---\nokf_version: "0.2"\ntype: Listing\n---\nBundle listing'
    reserved = "---\ntype: Guide\ntitle: Listing title\n---\nListing body"
    outside = "---\ntype: Post\ntitle: Hugo page\n---\nArticle body"
    files = {
        "context/index.md": index,
        "context/log.md": reserved,
        "context/nested/index.md": reserved,
        "context/nested/log.md": reserved,
        "context/mau.md": "---\ntype: Guide\n---\nMAU",
        "context/plain.txt": reserved,
        "outside.md": outside,
        "outside-broken.md": "---\ntype: Post\ninvalid: [\n---\nArticle",
    }
    source = _source(files)
    infos: Dict[str, DocumentInfoClass] = {}
    concepts: List[OkfPropertiesClass] = []
    for wu in source.get_workunits():
        info = wu.get_aspect_of_type(DocumentInfoClass)
        if (
            info
            and info.customProperties
            and "github_file_path" in info.customProperties
        ):
            infos[info.customProperties["github_file_path"]] = info
        concept = wu.get_aspect_of_type(OkfPropertiesClass)
        if concept is not None:
            concepts.append(concept)
    assert len(concepts) == 1
    assert concepts[0].conceptId == "mau"
    assert infos["context/index.md"].contents.text == index
    assert infos["outside.md"].title == "outside.md"
    for path in [
        "context/log.md",
        "context/nested/index.md",
        "context/nested/log.md",
        "context/plain.txt",
        "outside.md",
        "outside-broken.md",
    ]:
        assert infos[path].contents.text == files[path]
    assert source.report.okf_parse_errors == 0


def test_explicit_bundle_without_versioned_index() -> None:
    source = _source(
        {
            "context/mau.md": "---\ntype: Guide\n---\nMAU",
            "context/index.md": "---\ntitle: Listing\n---\nListing body",
            "contextual/outside.md": "---\ntype: Post\n---\nNot in context",
        },
        roots=["context/"],
    )
    concepts = _concepts(source)
    assert set(concepts) == {"context/mau.md"}
    assert concepts["context/mau.md"].conceptId == "mau"
    bundle = concepts["context/mau.md"].bundle
    assert bundle is not None
    assert bundle.rootPath == "context"
    assert bundle.okfVersion is None
    assert source.report.okf_unversioned_index_files == 1
    assert source.report.infos


def test_discovery_uses_ancestor_indexes_outside_prefix_and_fetches_only_once() -> None:
    source = _source(
        {
            "index.md": '---\nokf_version: "0.1"\n---\nRoot',
            "context/index.md": '---\nokf_version: "0.2"\n---\nContext',
            "unrelated/index.md": '---\nokf_version: "0.3"\n---\nUnrelated',
            "context/guides/mau.md": "---\ntype: Guide\n---\nMAU",
        },
        listed=["context/guides/mau.md"],
        path_prefix="context/guides",
    )
    concepts = _concepts(source)
    bundle = concepts["context/guides/mau.md"].bundle
    assert bundle is not None
    assert bundle.rootPath == "context"
    fetch = source.client.fetch_file_content
    assert isinstance(fetch, MagicMock)
    paths = [call.args[1] for call in fetch.call_args_list]
    assert sorted(paths) == ["context/guides/mau.md", "context/index.md", "index.md"]


def test_index_is_discovered_even_when_outside_import_limit() -> None:
    source = _source(
        {
            "mau.md": "---\ntype: Guide\n---\nMAU",
            "index.md": '---\nokf_version: "0.2"\n---\nRoot',
        },
        max_files=1,
    )
    concepts = _concepts(source)
    bundle = concepts["mau.md"].bundle
    assert bundle is not None
    assert bundle.okfVersion == "0.2"
    assert source.report.files_processed == 1


def test_master_switch_disables_discovery_and_preserves_index_frontmatter() -> None:
    raw = '---\nokf_version: "0.2"\n---\nRoot listing'
    source = _source({"index.md": raw}, roots=[""], enable_okf=False)
    workunits = list(source.get_workunits())
    assert not any(wu.get_aspect_of_type(OkfPropertiesClass) for wu in workunits)
    info = next(
        wu.get_aspect_of_type(DocumentInfoClass)
        for wu in workunits
        if wu.get_aspect_of_type(DocumentInfoClass)
    )
    assert info is not None
    assert info.contents.text == raw
    fetch = source.client.fetch_file_content
    assert isinstance(fetch, MagicMock)
    assert fetch.call_count == 1


def test_bundle_metadata_participates_in_content_hash() -> None:
    content = "---\ntype: Guide\n---\nBody"
    bundle = OkfBundleClass(
        location="https://github.com/example-org/analytics",
        rootPath=".",
        okfVersion="0.2",
    )
    original = compute_source_hash(content, bundle=bundle, enable_okf=True)
    assert original != compute_source_hash(content, bundle=None, enable_okf=True)
    bundle.okfVersion = "0.3"
    assert original != compute_source_hash(content, bundle=bundle, enable_okf=True)
    bundle.okfVersion = "0.2"
    bundle.rootPath = "context"
    assert original != compute_source_hash(content, bundle=bundle, enable_okf=True)

    bundle.rootPath = "."
    bundle.location = "https://github.com/example-org/other"
    assert original != compute_source_hash(content, bundle=bundle, enable_okf=True)


def test_unversioned_index_is_reported_without_enabling_a_bundle() -> None:
    content = "---\ntype: Guide\n---\nBody"
    source = _source(
        {
            "context/index.md": "---\ntitle: Listing\n---\nListing",
            "context/guide.md": content,
        }
    )
    workunits = list(source.get_workunits())
    assert not any(wu.get_aspect_of_type(OkfPropertiesClass) for wu in workunits)
    assert source.report.okf_unversioned_index_files == 1
    assert source.report.infos
    assert not source.report.warnings
