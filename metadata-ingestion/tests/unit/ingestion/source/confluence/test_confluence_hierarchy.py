from datahub.ingestion.source.confluence.hierarchy import (
    build_browse_path,
    collect_folder_nodes,
    document_urn,
)
from datahub.ingestion.source.confluence.models import (
    ConfluenceAncestor,
    ConfluencePage,
    ConfluenceSpaceRef,
)


def _page(page_id: str, ancestors: list) -> ConfluencePage:
    return ConfluencePage.model_validate(
        {
            "id": page_id,
            "space": {"key": "TEAM", "name": "Team"},
            "ancestors": ancestors,
        }
    )


def test_document_urn_includes_the_instance() -> None:
    assert document_urn("abc123", "42") == "urn:li:document:confluence-abc123-42"


def test_browse_path_links_only_ingested_ancestors() -> None:
    path = build_browse_path(
        ConfluenceSpaceRef(key="TEAM", name="Team"),
        [
            ConfluenceAncestor(id="1", title="Home"),
            ConfluenceAncestor(id="2", title="Guides"),
        ],
        instance_id="abc",
        ingested_ids={"1"},
    )

    assert path is not None
    assert [(e.id, e.urn) for e in path.path] == [
        ("Team", None),
        ("Home", "urn:li:document:confluence-abc-1"),
        ("Guides", None),
    ]


def test_browse_path_falls_back_to_space_key_and_needs_a_space() -> None:
    by_key = build_browse_path(ConfluenceSpaceRef(key="TEAM"), [], "abc", set())

    assert by_key is not None
    assert [e.id for e in by_key.path] == ["TEAM"]
    assert build_browse_path(ConfluenceSpaceRef(), [], "abc", set()) is None


def test_folders_are_rebuilt_from_page_ancestry() -> None:
    pages = [
        _page(
            "10",
            [
                {"id": "f1", "title": "Area", "type": "folder"},
                {"id": "f2", "title": "Sub", "type": "folder"},
                {"id": "3", "title": "Parent", "type": "page"},
            ],
        ),
        _page("11", [{"id": "f1", "title": "Area", "type": "folder"}]),
    ]

    folders = collect_folder_nodes(pages)

    assert set(folders) == {"f1", "f2"}
    assert folders["f1"].parent_id is None
    assert folders["f2"].parent_id == "f1"
    assert [a.id for a in folders["f2"].ancestor_prefix] == ["f1"]
    assert folders["f2"].space.key == "TEAM"
