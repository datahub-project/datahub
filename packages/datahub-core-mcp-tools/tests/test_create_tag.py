"""create_tag: idempotent by name, with color as a second write."""

from typing import Callable, Dict, Iterator
from unittest.mock import MagicMock

import pytest
from datahub.configuration.common import GraphError
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tools.tags import create_tag

Responder = Callable[[dict], object]


class FakeGms:
    """Answers GraphQL by operation name and records each operation sent."""

    def __init__(self, graph: MagicMock) -> None:
        self.graph = graph
        self.sent: list[tuple[str, dict]] = []
        self.responders: Dict[str, Responder] = {
            "findTagsByName": lambda _: {"search": {"searchResults": []}},
            "createTag": lambda v: {"createTag": f"urn:li:tag:{v['input']['id']}"},
            "setTagColor": lambda _: {"setTagColor": True},
        }
        graph.execute_graphql.side_effect = self._execute
        graph.exists.return_value = False

    def _execute(self, *, operation_name: str, variables: dict, **_: object) -> object:
        self.sent.append((operation_name, variables))
        answer = self.responders[operation_name](variables)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def operations(self) -> list[str]:
        return [name for name, _ in self.sent]

    def writes(self) -> list[str]:
        return [name for name in self.operations() if name != "findTagsByName"]


@pytest.fixture
def gms() -> Iterator[FakeGms]:
    client = MagicMock()
    with with_datahub_client(client):
        yield FakeGms(client._graph)


def _search_finds(*tags: tuple[str, str]) -> Responder:
    return lambda _: {
        "search": {
            "searchResults": [
                {"entity": {"urn": urn, "properties": {"name": name}}}
                for urn, name in tags
            ]
        }
    }


def test_creates_a_new_tag_with_its_name_as_id_and_sets_the_color(
    gms: FakeGms,
) -> None:
    result = create_tag(name=" tier-1 ", description="Most critical", color="1890ff")

    assert result == {"urn": "urn:li:tag:tier-1", "created": True}
    assert gms.writes() == ["createTag", "setTagColor"]
    assert gms.sent[1][1] == {
        "input": {"id": "tier-1", "name": "tier-1", "description": "Most critical"}
    }
    assert gms.sent[2][1] == {"urn": "urn:li:tag:tier-1", "colorHex": "#1890FF"}


def test_an_existing_id_is_returned_without_writing(gms: FakeGms) -> None:
    gms.graph.exists.side_effect = lambda urn: urn == "urn:li:tag:PII"

    result = create_tag(name="PII", description="ignored", color="#000000")

    assert result["urn"] == "urn:li:tag:PII"
    assert result["created"] is False
    assert result["matched"] == "id"
    assert "description and color were not applied" in result["note"]
    assert gms.sent == []


def test_an_existing_display_name_in_another_case_is_returned(gms: FakeGms) -> None:
    gms.responders["findTagsByName"] = _search_finds(
        ("urn:li:tag:pii-data", "PII data"),
        ("urn:li:tag:3f2a", "PII"),
    )

    result = create_tag(name="pii")

    assert result == {"urn": "urn:li:tag:3f2a", "created": False, "matched": "name"}
    assert gms.writes() == []


def test_a_similar_name_does_not_count_as_existing(gms: FakeGms) -> None:
    gms.responders["findTagsByName"] = _search_finds(
        ("urn:li:tag:pii-data", "PII data")
    )

    assert create_tag(name="PII")["created"] is True
    assert gms.writes() == ["createTag"]


def test_a_failed_name_search_still_creates(gms: FakeGms) -> None:
    gms.responders["findTagsByName"] = lambda _: RuntimeError("search is down")

    assert create_tag(name="PII")["created"] is True


@pytest.mark.parametrize("name", ["a,b", "x(y)", "   "])
def test_names_that_cannot_be_an_id_are_refused(gms: FakeGms, name: str) -> None:
    with pytest.raises(ToolError, match="create_tag"):
        create_tag(name=name)

    assert gms.sent == []
    gms.graph.exists.assert_not_called()


def test_a_bad_color_is_refused_before_anything_is_written(gms: FakeGms) -> None:
    with pytest.raises(ToolError, match="hex color"):
        create_tag(name="PII", color="blue")

    assert gms.sent == []


def test_a_color_failure_still_reports_the_created_tag(gms: FakeGms) -> None:
    gms.responders["setTagColor"] = lambda _: GraphError(
        [{"message": "Failed to set color"}]
    )

    result = create_tag(name="PII", color="#FF0000")

    assert result["urn"] == "urn:li:tag:PII"
    assert result["created"] is True
    assert "Failed to set color" in result["color_error"]


def test_a_tag_created_concurrently_is_returned(gms: FakeGms) -> None:
    gms.responders["createTag"] = lambda _: GraphError(
        [{"message": "This Tag already exists!"}]
    )

    result = create_tag(name="PII", color="#FF0000")

    assert (result["created"], result["matched"]) == (False, "id")
    # Its color belongs to whoever made it.
    assert gms.writes() == ["createTag"]


def test_a_refused_create_is_an_error(gms: FakeGms) -> None:
    gms.responders["createTag"] = lambda _: GraphError(
        [{"message": "Unauthorized to perform this action."}]
    )

    with pytest.raises(ToolError, match="rejected createTag: Unauthorized"):
        create_tag(name="PII")

    assert gms.writes() == ["createTag"]


async def test_annotations_mark_create_tag_idempotent_and_not_destructive() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}

    hints = {
        name: (
            tools[name].annotations.read_only_hint,
            tools[name].annotations.destructive_hint,
            tools[name].annotations.idempotent_hint,
        )
        for name in ("create_tag", "add_tags", "remove_tags", "search")
    }
    assert hints == {
        "create_tag": (False, False, True),
        "add_tags": (False, False, True),
        "remove_tags": (False, True, True),
        # The spec gives idempotentHint no meaning on read-only tools.
        "search": (True, False, None),
    }
    assert "create_tag" in (tools["add_tags"].description or "")
