from typing import Dict, List, Optional, Tuple

from datahub.ingestion.source.google_drive.google_drive_hierarchy import (
    build_browse_path_v2,
    make_document_urn,
    resolve_parent_urn,
)
from datahub.ingestion.source.google_drive.models import FolderNode

PARENT_DOC = "urn:li:document:existing-parent"


def node(folder_id: str, title: str, parent_id: Optional[str]) -> FolderNode:
    return FolderNode(
        id=folder_id,
        title=title,
        parent_id=parent_id,
        path=title,
        drive_id=None,
        web_view_link=None,
    )


ROOT = node("root00000000", "Root", None)
SUB = node("sub000000000", "Sub", ROOT.id)
LEAF = node("leaf00000000", "Leaf", SUB.id)
FOLDERS: Dict[str, FolderNode] = {f.id: f for f in (ROOT, SUB, LEAF)}


def entries(
    folder: Optional[FolderNode], create_roots: bool, parent: Optional[str]
) -> List[Tuple[str, Optional[str]]]:
    path = build_browse_path_v2(folder, FOLDERS, create_roots, parent)
    assert path is not None
    return [(e.id, e.urn) for e in path.path]


def test_parent_urn_rules() -> None:
    assert resolve_parent_urn(LEAF, True, None) == make_document_urn(LEAF.id)
    assert resolve_parent_urn(ROOT, True, PARENT_DOC) == make_document_urn(ROOT.id)
    # Without root documents, items in a root attach to the configured parent.
    assert resolve_parent_urn(ROOT, False, PARENT_DOC) == PARENT_DOC
    assert resolve_parent_urn(ROOT, False, None) is None
    # A root itself.
    assert resolve_parent_urn(None, True, PARENT_DOC) == PARENT_DOC


def test_browse_path_lists_all_ancestors_in_order() -> None:
    expected: List[Tuple[str, Optional[str]]] = [
        ("Root", make_document_urn(ROOT.id)),
        ("Sub", make_document_urn(SUB.id)),
        ("Leaf", make_document_urn(LEAF.id)),
    ]
    assert entries(LEAF, True, None) == expected
    assert entries(LEAF, True, PARENT_DOC) == [(PARENT_DOC, PARENT_DOC), *expected]


def test_browse_path_root_entry_has_no_urn_without_root_documents() -> None:
    assert entries(SUB, False, None) == [
        ("Root", None),
        ("Sub", make_document_urn(SUB.id)),
    ]


def test_browse_path_for_root_itself() -> None:
    assert build_browse_path_v2(None, FOLDERS, True, None) is None
    assert entries(None, True, PARENT_DOC) == [(PARENT_DOC, PARENT_DOC)]
