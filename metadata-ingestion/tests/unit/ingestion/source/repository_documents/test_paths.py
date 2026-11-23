import pytest

from datahub.ingestion.source.repository_documents.client import RepositoryFileInfo
from datahub.ingestion.source.repository_documents.paths import (
    RepositoryPropertyKeys,
    TreeSelection,
    collect_intermediate_directories,
    make_dir_source_id,
    make_file_source_id,
    make_repo_source_id,
    make_source_prefix,
    matches_filters,
    normalize_document_id,
    resolve_parent_dir_source_id,
)


def test_make_file_and_dir_source_ids() -> None:
    assert (
        make_file_source_id("acme/docs", "guides/setup.md", source_prefix="github")
        == "github.acme.docs.guides.setup"
    )
    assert (
        make_dir_source_id("acme/docs", "guides", source_prefix="github")
        == "github.acme.docs.guides._dir"
    )


def test_make_repo_source_id() -> None:
    assert (
        make_repo_source_id("acme/docs", source_prefix="github")
        == "github.acme.docs._repo"
    )


def test_collect_intermediate_directories() -> None:
    files = [RepositoryFileInfo(path="docs/guides/setup.md", size=10)]
    assert collect_intermediate_directories(files, "docs") == {"docs/guides"}


def test_collect_intermediate_directories_without_prefix() -> None:
    files = [RepositoryFileInfo(path="guides/setup.md", size=10)]
    assert collect_intermediate_directories(files, "") == {"guides"}


def test_resolve_parent_dir_source_id() -> None:
    parent = resolve_parent_dir_source_id(
        "acme/docs", "docs/guides/setup.md", "docs", source_prefix="github"
    )
    assert parent == make_dir_source_id(
        "acme/docs", "docs/guides", source_prefix="github"
    )


def test_resolve_parent_dir_source_id_uses_repo_root() -> None:
    repo_root = make_repo_source_id("acme/docs", source_prefix="github")
    parent = resolve_parent_dir_source_id(
        "acme/docs",
        "readme.md",
        "",
        repo_root_source_id=repo_root,
        source_prefix="github",
    )
    assert parent == repo_root


def test_matches_filters_respects_path_prefix_boundary() -> None:
    assert matches_filters("docs/readme.md", "docs", {".md"})
    assert not matches_filters("documentation/readme.md", "docs", {".md"})


def test_normalize_document_id_includes_hash_suffix() -> None:
    id_a = normalize_document_id("upload.docs/a b")
    id_b = normalize_document_id("upload.docs/a-b")
    assert id_a != id_b
    assert len(id_a.split("-")[-1]) == 16


def test_make_source_prefix() -> None:
    assert make_source_prefix("github", None) == "github"
    assert make_source_prefix("github", " ghe ") == "github-ghe"


@pytest.mark.parametrize("platform_instance", ["", "a.b", "a,b", "a(b)"])
def test_make_source_prefix_rejects_invalid_instances(platform_instance: str) -> None:
    with pytest.raises(ValueError, match="platform_instance"):
        make_source_prefix("github", platform_instance)


def test_repository_property_keys() -> None:
    keys = RepositoryPropertyKeys("gitlab")
    assert (keys.repo, keys.file_path, keys.directory_path) == (
        "gitlab_repo",
        "gitlab_file_path",
        "gitlab_directory_path",
    )


def test_tree_selection_keeps_every_index_but_filters_files() -> None:
    selection = TreeSelection.for_filters("/docs/", [".MD"])
    for path in ["index.md", "docs/a.md", "docs/b.png", "other/index.md"]:
        selection.add(RepositoryFileInfo(path=path, size=None))
    assert [file.path for file in selection.files] == ["docs/a.md"]
    assert selection.index_paths == {"index.md", "other/index.md"}
