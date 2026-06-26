"""Unit tests for GitHub documents hierarchy extraction."""

from typing import Dict, Optional

from datahub.ingestion.source.github_documents.github_hierarchy import (
    GitHubHierarchyExtractor,
)
from datahub.metadata.schema_classes import BrowsePathsV2Class

# repo -> docs -> guides -> setup.md
PARENT_LINKS: Dict[str, Optional[str]] = {
    "repo": None,
    "docs": "repo",
    "guides": "docs",
    "setup": "guides",
}
TITLES = {
    "repo": "docs",
    "docs": "docs",
    "guides": "guides",
    "setup": "setup.md",
}
URNS = {
    "repo": "urn:li:document:repo",
    "docs": "urn:li:document:docs",
    "guides": "urn:li:document:guides",
    "setup": "urn:li:document:setup",
}


def test_build_ancestor_chain_multi_level() -> None:
    chain = GitHubHierarchyExtractor.build_ancestor_chain("setup", PARENT_LINKS)
    # Root first, immediate parent last; the document itself is excluded.
    assert chain == ["repo", "docs", "guides"]


def test_build_ancestor_chain_root_has_no_ancestors() -> None:
    assert GitHubHierarchyExtractor.build_ancestor_chain("repo", PARENT_LINKS) == []


def test_build_ancestor_chain_stops_on_missing_parent() -> None:
    # "ghost" is referenced as a parent but absent from the link map.
    links: Dict[str, Optional[str]] = {"orphan": "ghost"}
    assert GitHubHierarchyExtractor.build_ancestor_chain("orphan", links) == ["ghost"]


def test_build_ancestor_chain_is_cycle_safe() -> None:
    links: Dict[str, Optional[str]] = {"a": "b", "b": "a"}
    assert GitHubHierarchyExtractor.build_ancestor_chain("a", links) == ["b"]


def test_build_browse_path_v2_with_ancestors() -> None:
    browse_path = GitHubHierarchyExtractor.build_browse_path_v2(
        source_id="setup",
        parent_links=PARENT_LINKS,
        titles=TITLES,
        urns=URNS,
    )

    assert browse_path is not None
    assert isinstance(browse_path, BrowsePathsV2Class)
    assert [e.id for e in browse_path.path] == ["docs", "docs", "guides"]
    assert [e.urn for e in browse_path.path] == [
        URNS["repo"],
        URNS["docs"],
        URNS["guides"],
    ]


def test_build_browse_path_v2_repo_root_returns_none() -> None:
    browse_path = GitHubHierarchyExtractor.build_browse_path_v2(
        source_id="repo",
        parent_links=PARENT_LINKS,
        titles=TITLES,
        urns=URNS,
    )
    assert browse_path is None


def test_build_browse_path_v2_empty_source_id_returns_none() -> None:
    assert (
        GitHubHierarchyExtractor.build_browse_path_v2(
            source_id="",
            parent_links=PARENT_LINKS,
            titles=TITLES,
            urns=URNS,
        )
        is None
    )


def test_build_browse_path_v2_with_root_parent_urn() -> None:
    # When nesting under an external configured parent, top-level docs have no
    # in-repo ancestors, but the configured parent roots the path.
    links: Dict[str, Optional[str]] = {"guides": None, "setup": "guides"}
    titles = {"guides": "guides", "setup": "setup.md"}
    urns = {"guides": "urn:li:document:guides", "setup": "urn:li:document:setup"}
    root = "urn:li:document:external-root"

    browse_path = GitHubHierarchyExtractor.build_browse_path_v2(
        source_id="setup",
        parent_links=links,
        titles=titles,
        urns=urns,
        root_parent_urn=root,
    )

    assert browse_path is not None
    assert [e.id for e in browse_path.path] == [root, "guides"]
    assert [e.urn for e in browse_path.path] == [root, urns["guides"]]


def test_build_browse_path_v2_root_parent_only_for_top_level() -> None:
    # A top-level document under an external parent gets just that parent.
    links: Dict[str, Optional[str]] = {"setup": None}
    root = "urn:li:document:external-root"

    browse_path = GitHubHierarchyExtractor.build_browse_path_v2(
        source_id="setup",
        parent_links=links,
        titles={"setup": "setup.md"},
        urns={"setup": "urn:li:document:setup"},
        root_parent_urn=root,
    )

    assert browse_path is not None
    assert [e.id for e in browse_path.path] == [root]
    assert browse_path.path[0].urn == root
