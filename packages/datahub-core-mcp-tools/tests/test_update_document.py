"""update_document: partial edits in place, moves, and failures that must not
re-parent a document or create one."""

from typing import Iterator, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests
from datahub.configuration.common import GraphError, OperationalError
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.document import Document
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools import document_edit_policy
from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tool_groups import groups_for_tags
from datahub_core_mcp_tools.tools.update_document import update_document

DOC = "urn:li:document:team-doc"
TEAM_FOLDER = "urn:li:document:team-runbooks"
OTHER_FOLDER = "urn:li:document:finance-runbooks"
ASSET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD)"
AUTHOR_URN = "urn:li:corpuser:john.doe"
BODY = "## ARR\nARR is MRR x 12.\n## Notes\nSee ARR."


def _document() -> Document:
    return Document.create_document(
        id="team-doc",
        title="Revenue",
        text=BODY,
        status="UNPUBLISHED",
        parent_document=TEAM_FOLDER,
        related_assets=[ASSET],
        owners=[CorpUserUrn("alice")],
        show_in_global_context=False,
    )


class Catalog:
    def __init__(self, documents: dict[str, Document]) -> None:
        self.client = MagicMock()
        graph = self.client._graph
        graph.exists.side_effect = lambda urn: urn in documents
        graph.get_entity_semityped.side_effect = lambda urn: (
            dict(documents[urn]._aspects) if urn in documents else {}
        )
        self.writes: list[dict] = []

        def graphql(**kwargs: object) -> dict:
            if kwargs["operation_name"] == "getDocumentAuthor":
                return {"me": {"corpUser": {"urn": AUTHOR_URN, "username": "john.doe"}}}
            self.writes.append(kwargs)
            return {kwargs["operation_name"]: True}

        graph.execute_graphql.side_effect = graphql
        self.client.entities.get.side_effect = lambda urn: documents[str(urn)]

    @property
    def upserts(self) -> list[Document]:
        return [c.args[0] for c in self.client.entities.upsert.call_args_list]

    def written_body(self) -> str:
        """The body sent through updateDocumentContents, the only write made."""
        [write] = self.writes
        assert write["operation_name"] == "updateDocumentContents"
        assert write["variables"]["input"]["urn"] == DOC
        assert set(write["variables"]["input"]) == {"urn", "contents"}
        assert self.upserts == []
        return write["variables"]["input"]["contents"]["text"]


@pytest.fixture
def catalog(monkeypatch: pytest.MonkeyPatch) -> Iterator[Catalog]:
    monkeypatch.delenv("SAVE_DOCUMENT_RESTRICT_UPDATES", raising=False)
    catalog = Catalog({DOC: _document()})
    with with_datahub_client(catalog.client):
        yield catalog


def test_anchored_edits_go_through_gms_content_write(catalog: Catalog) -> None:
    result = update_document(
        urn=DOC,
        command="update_content",
        content_edits=[
            {"old_str": "MRR x 12", "new_str": "MRR × 12, excluding one-time fees"},
            {
                "old_str": "ARR",
                "new_str": "Annual recurring revenue",
                "replace_all_matches": True,
            },
        ],
    )

    # Not a whole-entity upsert, which would leave the semanticText override
    # (preferred by the embedding pipeline) holding the previous body.
    assert catalog.written_body() == (
        "## Annual recurring revenue\nAnnual recurring revenue is MRR × 12, excluding "
        "one-time fees.\n## Notes\nSee Annual recurring revenue."
    )
    assert result["command"] == "update_content"


@pytest.mark.parametrize(
    ("edit", "error"),
    [
        ({"old_str": "missing", "new_str": "x"}, "not found"),
        ({"old_str": "ARR", "new_str": "x"}, "matches 3 places"),
        ({"new_str": "x"}, "no old_str"),
        # Without new_str the edit would delete the anchored text.
        ({"old_str": "ARR is"}, "no new_str"),
    ],
)
def test_an_edit_that_does_not_anchor_writes_nothing(
    catalog: Catalog, edit: dict, error: str
) -> None:
    with pytest.raises(ToolError, match=error):
        update_document(urn=DOC, command="update_content", content_edits=[edit])

    assert catalog.upserts == [] and catalog.writes == []


def test_replace_content_needs_confirmation_to_discard_the_body(
    catalog: Catalog,
) -> None:
    with pytest.raises(ToolError, match="allow_deleting_content=true"):
        update_document(urn=DOC, command="replace_content", new_content="New")
    assert catalog.upserts == [] and catalog.writes == []

    update_document(
        urn=DOC,
        command="replace_content",
        new_content="New",
        allow_deleting_content=True,
    )

    assert catalog.written_body() == "New"


def test_append_content(catalog: Catalog) -> None:
    update_document(urn=DOC, command="append_content", new_content="\n## FAQ")

    assert catalog.written_body() == BODY + "\n## FAQ"


def test_update_properties_writes_only_the_fields_it_changes(
    catalog: Catalog,
) -> None:
    result = update_document(
        urn=DOC,
        command="update_properties",
        properties={
            "title": "ARR",
            "topics": ["finance", " finance "],
            "related_documents": [OTHER_FOLDER],
            "status": "PUBLISHED",
        },
    )

    # Field-scoped writes only: an upsert of the document as read would write
    # back its body, parent and links, reverting a concurrent edit of them.
    assert catalog.upserts == []
    assert [(w["operation_name"], w["variables"]["input"]) for w in catalog.writes] == [
        ("updateDocumentContents", {"urn": DOC, "title": "ARR"}),
        (
            "updateDocumentRelatedEntities",
            {"urn": DOC, "relatedDocuments": [OTHER_FOLDER]},
        ),
        ("updateDocumentStatus", {"urn": DOC, "state": "PUBLISHED"}),
    ]
    [mcp] = [c.args[0] for c in catalog.client._graph.emit_mcp.call_args_list]
    assert mcp.entityUrn == DOC
    assert mcp.aspect == GlobalTagsClass(
        tags=[TagAssociationClass(tag="urn:li:tag:finance")]
    )
    assert result["changed"] == [
        "title",
        "topics",
        "related_documents",
        "status=PUBLISHED",
    ]


def test_an_empty_topics_list_removes_the_topics(catalog: Catalog) -> None:
    update_document(urn=DOC, command="update_properties", properties={"topics": []})

    [mcp] = [c.args[0] for c in catalog.client._graph.emit_mcp.call_args_list]
    assert mcp.aspect == GlobalTagsClass(tags=[])
    assert catalog.writes == [] and catalog.upserts == []


def test_a_properties_update_that_fails_partway_says_what_was_applied(
    catalog: Catalog,
) -> None:
    def graphql(**kwargs: object) -> dict:
        if kwargs["operation_name"] == "updateDocumentStatus":
            raise GraphError([{"message": "Unauthorized to perform this action."}])
        catalog.writes.append(kwargs)
        return {str(kwargs["operation_name"]): True}

    catalog.client._graph.execute_graphql.side_effect = graphql

    with pytest.raises(ToolError) as raised:
        update_document(
            urn=DOC,
            command="update_properties",
            properties={"title": "ARR", "status": "PUBLISHED"},
        )

    assert "Do not retry" in str(raised.value)
    assert "Already applied before this failure: title." in str(raised.value)
    assert [w["operation_name"] for w in catalog.writes] == ["updateDocumentContents"]


def test_move_goes_through_movedocument_and_writes_nothing_else(
    catalog: Catalog,
) -> None:
    result = update_document(urn=DOC, command="move", parent_urn=OTHER_FOLDER)

    [move] = catalog.writes
    assert move["operation_name"] == "moveDocument"
    assert move["variables"] == {"input": {"urn": DOC, "parentDocument": OTHER_FOLDER}}
    assert catalog.upserts == []
    assert result["changed"] == [f"parent ({TEAM_FOLDER} -> {OTHER_FOLDER})"]


def test_a_refused_move_changes_nothing(catalog: Catalog) -> None:
    def graphql(**kwargs: object) -> dict:
        if kwargs["operation_name"] == "getDocumentAuthor":
            return {"me": None}
        raise GraphError(
            [{"message": "Cannot move a document under its own descendant."}]
        )

    catalog.client._graph.execute_graphql.side_effect = graphql

    with pytest.raises(ToolError, match="could not move .* own descendant"):
        update_document(urn=DOC, command="move", parent_urn=OTHER_FOLDER)

    assert catalog.upserts == []


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        (
            {"command": "append_content", "content_edits": [{"old_str": "a"}]},
            "use command=update_content",
        ),
        ({"command": "update_content"}, "requires content_edits"),
        ({"command": "update_properties", "properties": {}}, "requires properties"),
        ({"command": "update_content", "parent_urn": TEAM_FOLDER}, "use command=move"),
        ({"command": "move", "parent_urn": "team"}, "must be a document URN"),
        (
            {"command": "update_properties", "properties": {"status": "DRAFT"}},
            "PUBLISHED",
        ),
        (
            {"command": "update_properties", "properties": {"title": " "}},
            "title cannot be empty",
        ),
        (
            {
                "command": "update_properties",
                "properties": {"title": "ARR", "related_assets": ["orders"]},
            },
            "must be a DataHub URN",
        ),
    ],
)
def test_arguments_that_do_not_fit_the_command_write_nothing(
    catalog: Catalog, kwargs: dict, error: str
) -> None:
    with pytest.raises(ToolError, match=error):
        update_document(urn=DOC, **kwargs)

    assert catalog.upserts == [] and catalog.writes == []


def test_a_missing_document_is_not_created(catalog: Catalog) -> None:
    with pytest.raises(ToolError, match="not found. Use save_document"):
        update_document(
            urn="urn:li:document:gone", command="append_content", new_content="x"
        )

    assert catalog.upserts == [] and catalog.writes == []


def test_a_failed_read_changes_nothing(catalog: Catalog) -> None:
    catalog.client.entities.get.side_effect = requests.ConnectionError("reset")

    with pytest.raises(ToolError, match="could not read .* nothing was changed"):
        update_document(urn=DOC, command="append_content", new_content="x")

    assert catalog.upserts == [] and catalog.writes == []


def test_a_refused_topics_update_is_reported_as_refused(catalog: Catalog) -> None:
    response = requests.Response()
    response.status_code = 403
    error = OperationalError("Unable to emit", {"message": "Unauthorized"})
    error.__cause__ = requests.HTTPError(response=response)
    catalog.client._graph.emit_mcp.side_effect = error

    with pytest.raises(ToolError, match="rejected .*updating the topics.*Unauthorized"):
        update_document(
            urn=DOC, command="update_properties", properties={"topics": ["finance"]}
        )

    assert catalog.upserts == [] and catalog.writes == []


def _content_write_fails(catalog: Catalog, error: Exception) -> None:
    def graphql(**kwargs: object) -> dict:
        if kwargs["operation_name"] == "getDocumentAuthor":
            return {"me": None}
        catalog.writes.append(dict(kwargs))
        raise error

    catalog.client._graph.execute_graphql.side_effect = graphql


def test_a_refused_content_edit_says_not_to_retry(catalog: Catalog) -> None:
    _content_write_fails(
        catalog, GraphError([{"message": "Unauthorized to perform this action."}])
    )

    with pytest.raises(ToolError, match="could not update the content .* Do not retry"):
        update_document(urn=DOC, command="append_content", new_content="x")

    assert len(catalog.writes) == 1 and catalog.upserts == []


def test_an_append_without_a_reply_warns_against_appending_again(
    catalog: Catalog,
) -> None:
    _content_write_fails(catalog, requests.ReadTimeout("timed out"))

    with pytest.raises(ToolError) as raised:
        update_document(urn=DOC, command="append_content", new_content="x")

    assert "may or may not have been applied" in str(raised.value)
    assert "appending again would add the text twice" in str(raised.value)
    assert len(catalog.writes) == 1 and catalog.upserts == []


def test_restricted_updates_and_moves_stay_in_the_shared_folder(
    catalog: Catalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SAVE_DOCUMENT_RESTRICT_UPDATES", "true")

    with pytest.raises(ToolError, match="not in the shared documents folder"):
        update_document(urn=DOC, command="append_content", new_content="x")

    assert catalog.upserts == [] and catalog.writes == []


def test_a_locked_field_is_refused_before_approval_and_by_the_tool(
    catalog: Catalog,
) -> None:
    class LockedParent:
        def ensure_editable(self, doc: Document, fields: frozenset) -> None:
            if "parent" in fields:
                raise ToolError("This document's place is managed by its source.")

    validator = vars(update_document)["_tool_validator"]
    with patch.object(
        document_edit_policy, "_load_policy", return_value=LockedParent()
    ):
        refusal = validator({"urn": DOC, "command": "move"})
        allowed = validator({"urn": DOC, "command": "append_content"})
        with pytest.raises(ToolError, match="managed by its source"):
            update_document(urn=DOC, command="move", parent_urn=OTHER_FOLDER)

    assert refusal is not None and allowed is None
    assert catalog.writes == []


async def test_registered_with_save_document_behind_one_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SAVE_DOCUMENT_TOOL_ENABLED", raising=False)
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)
    async with Client(mcp) as client:
        tool = {t.name: t for t in await client.list_tools()}["update_document"]

    assert tool.annotations is not None
    assert tool.annotations.destructive_hint is True
    assert tool.annotations.idempotent_hint is False
    assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]

    monkeypatch.setenv("SAVE_DOCUMENT_TOOL_ENABLED", "false")
    names: Optional[list[str]] = register_tools(FastMCP("u"), is_oss=True)
    assert names is not None
    assert not {"save_document", "update_document"} & set(names)
