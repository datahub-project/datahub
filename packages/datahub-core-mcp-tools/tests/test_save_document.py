"""save_document: new documents, in-place updates, and failures that must not
re-parent a document or create a second one."""

from datetime import datetime
from typing import Iterator, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests
from datahub.configuration.common import OperationalError
from datahub.metadata import schema_classes as models
from datahub.sdk.document import Document
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools import document_edit_policy
from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tool_groups import groups_for_tags
from datahub_core_mcp_tools.tools.save_document import (
    ROOT_PARENT_URN,
    save_document,
)

AUTHOR = {
    "urn": "urn:li:corpuser:john.doe",
    "username": "john.doe",
    "info": {"displayName": "John Doe"},
    "editableProperties": None,
}
TEAM_FOLDER = "urn:li:document:team-runbooks"
EXISTING = "urn:li:document:team-doc"
ASSET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD)"


def _existing_document(**overrides: object) -> Document:
    """A user's document in a team folder, private, unpublished, with an owner."""
    args: dict = {
        "id": "team-doc",
        "title": "Old title",
        "text": "Old body",
        "status": "UNPUBLISHED",
        "parent_document": TEAM_FOLDER,
        "related_assets": [ASSET],
        "owners": ["urn:li:corpuser:alice"],
        "show_in_global_context": False,
        **overrides,
    }
    return Document.create_document(**args)


def _folder_aspects(removed: Optional[bool]) -> dict:
    aspects: dict = {
        "documentInfo": models.DocumentInfoClass(
            title="Shared",
            status=models.DocumentStatusClass(state="PUBLISHED"),
            contents=models.DocumentContentsClass(text=""),
            created=models.AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
            lastModified=models.AuditStampClass(
                time=0, actor="urn:li:corpuser:datahub"
            ),
        )
    }
    if removed is not None:
        aspects["status"] = models.StatusClass(removed=removed)
    return aspects


class Catalog:
    """A DataHub with ``documents`` stored (SDK objects) and a shared folder."""

    def __init__(
        self, documents: dict[str, Document], shared_folder: Optional[dict]
    ) -> None:
        self.client = MagicMock()
        self.documents = documents
        graph = self.client._graph
        graph.exists.side_effect = lambda urn: (
            urn in documents or (urn == ROOT_PARENT_URN and shared_folder is not None)
        )
        graph.get_entity_semityped.side_effect = lambda urn: (
            shared_folder if urn == ROOT_PARENT_URN and shared_folder else {}
        )
        graph.execute_graphql.return_value = {"me": {"corpUser": AUTHOR}}
        self.client.entities.get.side_effect = lambda urn: documents[str(urn)]

    @property
    def upserts(self) -> list[Document]:
        return [c.args[0] for c in self.client.entities.upsert.call_args_list]


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "SAVE_DOCUMENT_RESTRICT_UPDATES",
        "SAVE_DOCUMENT_ORGANIZE_BY_USER",
        "SAVE_DOCUMENT_PARENT_TITLE",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def catalog(env: None) -> Iterator[Catalog]:
    catalog = Catalog({EXISTING: _existing_document()}, _folder_aspects(removed=False))
    with with_datahub_client(catalog.client):
        yield catalog


def _refused(status: int, message: str) -> OperationalError:
    response = requests.Response()
    response.status_code = status
    try:
        raise requests.HTTPError(response=response)
    except requests.HTTPError as http_error:
        error = OperationalError(
            "Unable to emit metadata to DataHub GMS", {"message": message}
        )
        error.__cause__ = http_error
        return error


# ---------------------------------------------------------------------------
# New documents
# ---------------------------------------------------------------------------


def test_a_new_document_goes_in_the_shared_folder(catalog: Catalog) -> None:
    result = save_document(
        document_type="Insight",
        title="High null rate",
        content="23% of emails are null.",
        topics=["data-quality", " email "],
        related_assets=[ASSET],
    )

    [doc] = catalog.upserts
    assert result["urn"] == str(doc.urn)
    assert result["urn"].startswith("urn:li:document:shared-")
    assert result["author"] == "John Doe"
    assert doc.parent_document == ROOT_PARENT_URN
    assert doc.subtype == "Insight"
    assert [str(o.owner) for o in doc.owners or []] == [AUTHOR["urn"]]
    assert [str(t.tag) for t in doc.tags or []] == [
        "urn:li:tag:data-quality",
        "urn:li:tag:email",
    ]
    assert doc.related_assets == [ASSET]
    settings = doc._get_aspect(models.DocumentSettingsClass)
    assert settings is not None and settings.lastModified is not None
    assert settings.lastModified.actor == AUTHOR["urn"]
    # GraphQL, search and the sidebar read DocumentInfo's stamps, not the SDK's
    # default ingestion actor.
    info = doc._get_aspect(models.DocumentInfoClass)
    assert info is not None
    assert info.created.actor == info.lastModified.actor == AUTHOR["urn"]


def test_each_new_document_is_a_new_urn(catalog: Catalog) -> None:
    first = save_document(document_type="Note", title="A", content="a")
    second = save_document(document_type="Note", title="A", content="a")

    assert first["urn"] != second["urn"]


def test_a_missing_shared_folder_is_created_first(env: None) -> None:
    catalog = Catalog({}, shared_folder=None)
    with with_datahub_client(catalog.client):
        save_document(document_type="Note", title="A", content="a")

    folder, doc = catalog.upserts
    assert str(folder.urn) == ROOT_PARENT_URN
    assert folder.subtype == "Folder"
    assert folder._get_aspect(models.StatusClass).removed is False  # type: ignore[union-attr]
    assert doc.parent_document == ROOT_PARENT_URN


def test_a_soft_deleted_shared_folder_is_restored(env: None) -> None:
    catalog = Catalog({}, shared_folder=_folder_aspects(removed=True))
    with with_datahub_client(catalog.client):
        save_document(document_type="Note", title="A", content="a")

    restored = catalog.client._graph.emit_mcp.call_args.args[0]
    assert restored.entityUrn == ROOT_PARENT_URN
    assert restored.aspect.removed is False
    assert len(catalog.upserts) == 1


def test_a_folder_that_cannot_be_created_stops_the_save(env: None) -> None:
    catalog = Catalog({}, shared_folder=None)
    catalog.client.entities.upsert.side_effect = _refused(403, "Unauthorized")

    with with_datahub_client(catalog.client):
        with pytest.raises(ToolError, match="rejected creating the folder"):
            save_document(document_type="Note", title="A", content="a")

    # Only the folder was attempted: a document under a missing folder is hidden.
    assert len(catalog.upserts) == 1


def test_a_new_document_under_a_given_parent(env: None) -> None:
    catalog = Catalog(
        {TEAM_FOLDER: _existing_document(id="team-runbooks", parent_document=None)},
        None,
    )
    with with_datahub_client(catalog.client):
        save_document(
            document_type="Note",
            title="Runbook",
            content="Steps",
            parent_urn=TEAM_FOLDER,
        )

    [doc] = catalog.upserts
    assert doc.parent_document == TEAM_FOLDER


def test_a_new_document_under_a_deleted_parent_is_refused(env: None) -> None:
    catalog = Catalog(
        {TEAM_FOLDER: _existing_document(id="team-runbooks", parent_document=None)},
        None,
    )
    catalog.client._graph.get_entity_semityped.side_effect = lambda urn: (
        {"status": models.StatusClass(removed=True)} if urn == TEAM_FOLDER else {}
    )

    with with_datahub_client(catalog.client):
        with pytest.raises(ToolError, match="is deleted"):
            save_document(
                document_type="Note",
                title="Runbook",
                content="Steps",
                parent_urn=TEAM_FOLDER,
            )

    # The caller's folder is not restored: only the shared folder is ours to undelete.
    assert catalog.upserts == []
    catalog.client._graph.emit_mcp.assert_not_called()


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"title": " "}, "`title` cannot be empty"),
        ({"content": ""}, "`content` cannot be empty"),
        ({"parent_urn": "urn:li:document:gone"}, "does not exist"),
        ({"parent_urn": "team-runbooks"}, "must be a document URN"),
        ({"urn": EXISTING, "parent_urn": TEAM_FOLDER}, "only be set when creating"),
        ({"urn": "team-doc"}, "must be a document URN"),
        ({"related_documents": [ASSET]}, "must be a document URN"),
        ({"related_assets": ["orders"]}, "must be a DataHub URN"),
        ({"urn": "urn:li:document:retrieval-policy-default"}, "configuration"),
    ],
)
def test_invalid_arguments_write_nothing(
    catalog: Catalog, kwargs: dict, error: str
) -> None:
    args = {"document_type": "Note", "title": "T", "content": "C", **kwargs}

    with pytest.raises(ToolError, match=error):
        save_document(**args)

    assert catalog.upserts == []


def test_a_create_without_a_reply_says_how_to_retry_without_a_duplicate(
    catalog: Catalog,
) -> None:
    catalog.client.entities.upsert.side_effect = requests.ConnectionError("reset")

    with pytest.raises(ToolError) as raised:
        save_document(document_type="Note", title="A", content="a")

    [doc] = catalog.upserts
    assert "may or may not have been applied" in str(raised.value)
    assert f"urn={str(doc.urn)!r}" in str(raised.value)


# ---------------------------------------------------------------------------
# Updates in place
# ---------------------------------------------------------------------------


def test_an_update_overwrites_in_place_and_keeps_the_rest(catalog: Catalog) -> None:
    result = save_document(
        urn=EXISTING, document_type="FAQ", title="New title", content="New body"
    )

    [doc] = catalog.upserts
    assert result["urn"] == EXISTING == str(doc.urn)
    assert result["message"] == "Successfully updated document: New title"
    assert (doc.title, doc.text, doc.subtype) == ("New title", "New body", "FAQ")
    # What the update does not set stays as it was: before in-place updates, an
    # update rebuilt the document and moved it into the shared folder.
    assert doc.parent_document == TEAM_FOLDER
    assert doc.status == "UNPUBLISHED"
    assert doc.show_in_global_context is False
    assert doc.related_assets == [ASSET]
    assert [str(o.owner) for o in doc.owners or []] == ["urn:li:corpuser:alice"]
    # No folder is touched for an update.
    catalog.client._graph.get_entity_semityped.assert_not_called()


def test_an_update_is_stamped_as_the_callers_edit(env: None) -> None:
    long_ago = datetime(2020, 1, 1)
    stored = _existing_document(created_time=long_ago, last_modified_time=long_ago)
    created_before = stored._get_aspect(models.DocumentInfoClass).created  # type: ignore[union-attr]
    catalog = Catalog({EXISTING: stored}, None)
    with with_datahub_client(catalog.client):
        save_document(urn=EXISTING, document_type="Note", title="T", content="C")

    [doc] = catalog.upserts
    info = doc._get_aspect(models.DocumentInfoClass)
    assert info is not None
    assert info.lastModified.actor == AUTHOR["urn"]
    assert info.lastModified.time > long_ago.timestamp() * 1000
    # An edit is not a creation: the original creator and time stay.
    assert info.created == created_before
    assert info.created.actor != AUTHOR["urn"]


def test_an_update_moves_a_stored_semantic_text_to_the_new_body(env: None) -> None:
    stored = _existing_document()
    stored._set_aspect(models.SemanticTextClass(text="Old body"))
    catalog = Catalog({EXISTING: stored}, None)
    with with_datahub_client(catalog.client):
        save_document(urn=EXISTING, document_type="Note", title="T", content="New")

    # Embeddings prefer semanticText over the body: writing back the old one would
    # leave search on the previous body.
    [doc] = catalog.upserts
    semantic = doc._get_aspect(models.SemanticTextClass)
    assert semantic is not None and semantic.text == "New"


def test_an_update_replaces_the_lists_it_is_given(catalog: Catalog) -> None:
    other = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.customers,PROD)"

    save_document(
        urn=EXISTING,
        document_type="Note",
        title="T",
        content="C",
        related_assets=[other],
        topics=["faq"],
    )

    [doc] = catalog.upserts
    assert doc.related_assets == [other]
    assert [str(t.tag) for t in doc.tags or []] == ["urn:li:tag:faq"]


def test_a_refused_update_is_not_retried_or_turned_into_a_create(
    catalog: Catalog,
) -> None:
    catalog.client.entities.upsert.side_effect = _refused(
        403, "Unauthorized to perform this action."
    )

    with pytest.raises(
        ToolError, match="rejected saving urn:li:document:team-doc: Unauthorized"
    ):
        save_document(urn=EXISTING, document_type="Note", title="T", content="C")

    # One write, of the same document in its own folder.
    [doc] = catalog.upserts
    assert str(doc.urn) == EXISTING
    assert doc.parent_document == TEAM_FOLDER


def test_an_update_without_a_reply_names_the_document_to_check(
    catalog: Catalog,
) -> None:
    catalog.client.entities.upsert.side_effect = requests.ReadTimeout("timed out")

    with pytest.raises(ToolError) as raised:
        save_document(urn=EXISTING, document_type="Note", title="T", content="C")

    assert "may or may not have been applied" in str(raised.value)
    assert EXISTING in str(raised.value)
    assert len(catalog.upserts) == 1
    assert catalog.upserts[0].parent_document == TEAM_FOLDER


@pytest.mark.parametrize("failing", ["exists", "get"])
def test_an_update_whose_document_cannot_be_read_writes_nothing(
    catalog: Catalog, failing: str
) -> None:
    # Reading it as missing would create it again in the shared folder.
    if failing == "exists":
        catalog.client._graph.exists.side_effect = requests.ConnectionError("reset")
    else:
        catalog.client.entities.get.side_effect = requests.ConnectionError("reset")

    with pytest.raises(ToolError, match="could not read .* nothing was saved"):
        save_document(urn=EXISTING, document_type="Note", title="T", content="C")

    assert catalog.upserts == []


def test_an_unknown_urn_is_created_at_that_urn(catalog: Catalog) -> None:
    result = save_document(
        urn="urn:li:document:not-yet-saved",
        document_type="Note",
        title="T",
        content="C",
    )

    [doc] = catalog.upserts
    assert result["urn"] == "urn:li:document:not-yet-saved" == str(doc.urn)
    assert doc.parent_document == ROOT_PARENT_URN


def test_restricted_updates_stay_in_the_shared_folder(
    catalog: Catalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SAVE_DOCUMENT_RESTRICT_UPDATES", "true")
    inside = "urn:li:document:shared-1"
    catalog.documents[inside] = _existing_document(
        id="shared-1", parent_document=ROOT_PARENT_URN
    )
    catalog.client._graph.get_entity_semityped.side_effect = lambda urn: (
        dict(catalog.documents[urn]._aspects) if urn in catalog.documents else {}
    )

    with pytest.raises(ToolError, match="not in the shared documents folder"):
        save_document(urn=EXISTING, document_type="Note", title="T", content="C")
    assert catalog.upserts == []

    save_document(urn=inside, document_type="Note", title="T", content="C")
    assert [str(d.urn) for d in catalog.upserts] == [inside]


def test_a_locked_field_is_refused_before_approval_and_by_the_tool(
    catalog: Catalog,
) -> None:
    class LockedContent:
        def ensure_editable(self, doc: Document, fields: frozenset) -> None:
            if "content" in fields:
                raise ToolError(
                    "This document's content is generated; edit its source."
                )

    validator = vars(save_document)["_tool_validator"]
    with patch.object(
        document_edit_policy, "_load_policy", return_value=LockedContent()
    ):
        refusal = validator({"urn": EXISTING, "title": "T"})
        with pytest.raises(ToolError, match="generated"):
            save_document(urn=EXISTING, document_type="Note", title="T", content="C")

    assert refusal is not None and "generated" in refusal
    assert catalog.upserts == []


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


async def test_registered_as_a_destructive_write_taking_string_lists(
    catalog: Catalog,
) -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    async with Client(mcp) as client:
        tool = {t.name: t for t in await client.list_tools()}["save_document"]
        await client.call_tool(
            "save_document",
            {
                "document_type": "Note",
                "title": "T",
                "content": "C",
                "topics": '["a", "b"]',
                "related_assets": ASSET,
            },
        )

    assert tool.annotations is not None
    assert tool.annotations.destructive_hint is True
    assert tool.annotations.idempotent_hint is False
    assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]
    [doc] = catalog.upserts
    assert doc.related_assets == [ASSET]


@pytest.mark.parametrize(("value", "registered"), [(None, True), ("false", False)])
async def test_the_document_tool_gate(
    monkeypatch: pytest.MonkeyPatch, value: Optional[str], registered: bool
) -> None:
    if value is None:
        monkeypatch.delenv("SAVE_DOCUMENT_TOOL_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SAVE_DOCUMENT_TOOL_ENABLED", value)

    names = register_tools(FastMCP("t"), is_oss=True)

    assert ("save_document" in names) is registered
