"""Unit tests for GitHub API helpers."""

import pytest

from datahub.ingestion.source.github_documents.github_api import (
    GitHubFileInfo,
    _matches_filters,
    collect_intermediate_directories,
    make_dir_source_id,
    make_file_source_id,
    make_repo_source_id,
    normalize_document_id,
    parse_repo_identifier,
    resolve_parent_dir_source_id,
)


def test_parse_repo_identifier_shorthand() -> None:
    assert parse_repo_identifier("acme/docs") == "acme/docs"


def test_parse_repo_identifier_url() -> None:
    assert parse_repo_identifier("https://github.com/acme/docs.git") == "acme/docs"


def test_parse_repo_identifier_invalid() -> None:
    with pytest.raises(ValueError):
        parse_repo_identifier("not-a-repo")


def test_make_file_and_dir_source_ids() -> None:
    assert (
        make_file_source_id("acme/docs", "guides/setup.md")
        == "github.acme.docs.guides.setup"
    )
    assert make_dir_source_id("acme/docs", "guides") == "github.acme.docs.guides._dir"


def test_collect_intermediate_directories() -> None:
    files = [GitHubFileInfo(path="docs/guides/setup.md", size=10)]
    dirs = collect_intermediate_directories(files, "docs")
    assert dirs == {"docs/guides"}


def test_resolve_parent_dir_source_id() -> None:
    parent = resolve_parent_dir_source_id("acme/docs", "docs/guides/setup.md", "docs")
    assert parent == make_dir_source_id("acme/docs", "docs/guides")


def test_resolve_parent_dir_source_id_uses_repo_root() -> None:
    repo_root = make_repo_source_id("acme/docs")
    parent = resolve_parent_dir_source_id(
        "acme/docs",
        "readme.md",
        "",
        repo_root_source_id=repo_root,
    )
    assert parent == repo_root


def test_matches_filters_respects_path_prefix_boundary() -> None:
    assert _matches_filters("docs/readme.md", "docs", {".md"})
    assert not _matches_filters("documentation/readme.md", "docs", {".md"})


def test_normalize_document_id_includes_hash_suffix() -> None:
    id_a = normalize_document_id("upload.docs/a b")
    id_b = normalize_document_id("upload.docs/a-b")
    assert id_a != id_b
    assert id_a.endswith(id_a.split("-")[-1])
