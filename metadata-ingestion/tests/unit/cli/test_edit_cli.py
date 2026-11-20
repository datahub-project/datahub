import contextlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from datahub.cli.edit_cli import edit
from datahub.cli.edit_history import HISTORY_FILE_ENV
from datahub.cli.edit_operations import (
    EditKind,
    EditOp,
    owner_urn,
    ownership_type_urn,
    plan_deprecation,
    plan_domain,
    plan_owner,
    plan_tag,
)
from datahub.cli.get_cli import get

DS1 = "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.table_a,PROD)"
DS2 = "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.table_b,PROD)"
TECHNICAL = "urn:li:ownershipType:__system__technical_owner"

Invoke = Callable[..., Dict]


@dataclass
class FakeEntity:
    tags: Set[str] = field(default_factory=set)
    terms: Set[str] = field(default_factory=set)
    owners: Set[Tuple[str, str]] = field(default_factory=set)
    domain: Optional[str] = None
    description: Optional[str] = None
    deprecated: bool = False
    note: str = ""


class FakeServerConfig:
    def __init__(self, server: str) -> None:
        self.server = server


class FakeGraph:
    """Stands in for GMS: answers the summary query and applies mutations."""

    def __init__(self, server: str = "http://localhost:8080") -> None:
        self.config = FakeServerConfig(server)
        self.entities: Dict[str, FakeEntity] = {}
        self.mutations: List[str] = []
        self.fail_on: Optional[str] = None
        # Tags, terms, owners and domains that edits may reference.
        self.references: Set[str] = set()

    def exists(self, urn: str) -> bool:
        return urn in self.entities or urn in self.references

    def execute_graphql(self, query: str, variables: Dict) -> Dict:
        if "getEntitySummary" in query:
            return {"entity": self._summary(variables["urn"])}
        match = re.search(r"\{ (\w+)\(", query)
        assert match, query
        name = match.group(1)
        if (
            self.fail_on
            and variables.get("input", {}).get("resourceUrn") == self.fail_on
        ):
            raise RuntimeError("server error")
        self.mutations.append(name)
        getattr(self, f"_{name}")(variables)
        return {name: True}

    def _summary(self, urn: str) -> Dict:
        e = self.entities[urn]
        return {
            "urn": urn,
            "type": "DATASET",
            "name": urn.split(",")[1],
            "platform": {"urn": "urn:li:dataPlatform:hive"},
            "properties": {"name": urn.split(",")[1], "description": "from source"},
            "editableProperties": {"description": e.description}
            if e.description
            else None,
            "tags": {"tags": [{"tag": {"urn": t}} for t in e.tags]},
            "glossaryTerms": {"terms": [{"term": {"urn": t}} for t in e.terms]},
            "ownership": {
                "owners": [
                    {"owner": {"urn": o}, "ownershipType": {"urn": t}}
                    for o, t in e.owners
                ]
            },
            "domain": {"domain": {"urn": e.domain}} if e.domain else None,
            "deprecation": {"deprecated": e.deprecated, "note": e.note},
        }

    def _addTag(self, v: Dict) -> None:
        self.entities[v["input"]["resourceUrn"]].tags.add(v["input"]["tagUrn"])

    def _removeTag(self, v: Dict) -> None:
        self.entities[v["input"]["resourceUrn"]].tags.discard(v["input"]["tagUrn"])

    def _addOwner(self, v: Dict) -> None:
        i = v["input"]
        self.entities[i["resourceUrn"]].owners.add(
            (i["ownerUrn"], i["ownershipTypeUrn"])
        )

    def _removeOwner(self, v: Dict) -> None:
        i = v["input"]
        self.entities[i["resourceUrn"]].owners.discard(
            (i["ownerUrn"], i["ownershipTypeUrn"])
        )

    def _setDomain(self, v: Dict) -> None:
        self.entities[v["e"]].domain = v["d"]

    def _unsetDomain(self, v: Dict) -> None:
        self.entities[v["e"]].domain = None

    def _updateDescription(self, v: Dict) -> None:
        self.entities[v["input"]["resourceUrn"]].description = v["input"]["description"]

    def _updateDeprecation(self, v: Dict) -> None:
        e = self.entities[v["input"]["urn"]]
        e.deprecated = v["input"]["deprecated"]
        e.note = v["input"]["note"]


@pytest.fixture
def graph() -> FakeGraph:
    g = FakeGraph()
    g.entities[DS1] = FakeEntity()
    g.entities[DS2] = FakeEntity()
    g.references = {
        "urn:li:tag:pii",
        "urn:li:tag:a",
        "urn:li:tag:b",
        "urn:li:corpGroup:data-eng",
        "urn:li:domain:finance",
    }
    return g


@pytest.fixture
def run(graph: FakeGraph, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator:
    monkeypatch.setenv(HISTORY_FILE_ENV, str(tmp_path / "history.json"))

    @contextlib.contextmanager
    def fake_default_graph(*args: object) -> Iterator[FakeGraph]:
        yield graph

    def invoke(*args: str, expect_ok: bool = True) -> Dict:
        result = CliRunner().invoke(edit, list(args))
        if expect_ok:
            assert result.exit_code == 0, result.output
            return json.loads(result.stdout)
        assert result.exit_code != 0
        return {"exit_code": result.exit_code, "output": result.output}

    with patch("datahub.cli.edit_cli.get_default_graph", fake_default_graph):
        yield invoke


class TestPlanning:
    def test_noop_edits_are_not_planned(self, graph: FakeGraph) -> None:
        graph.entities[DS1].tags.add("urn:li:tag:pii")
        assert plan_tag(graph, DS1, "pii", add=True) is None  # type: ignore[arg-type]
        assert plan_tag(graph, DS1, "other", add=False) is None  # type: ignore[arg-type]
        assert plan_domain(graph, DS1, None) is None  # type: ignore[arg-type]

    def test_domain_inverse_restores_previous_domain(self, graph: FakeGraph) -> None:
        graph.entities[DS1].domain = "urn:li:domain:old"
        plan = plan_domain(graph, DS1, "new")  # type: ignore[arg-type]
        assert plan is not None
        assert plan.forward == EditOp(
            EditKind.SET_DOMAIN, DS1, domain="urn:li:domain:new"
        )
        assert plan.inverse == EditOp(
            EditKind.SET_DOMAIN, DS1, domain="urn:li:domain:old"
        )

    def test_owner_matching_uses_ownership_type(self, graph: FakeGraph) -> None:
        graph.entities[DS1].owners.add(("urn:li:corpuser:alice", TECHNICAL))
        assert plan_owner(graph, DS1, "alice", "technical", add=True) is None  # type: ignore[arg-type]
        plan = plan_owner(graph, DS1, "alice", "business", add=True)  # type: ignore[arg-type]
        assert plan is not None and plan.inverse.kind == EditKind.REMOVE_OWNER

    def test_deprecation_inverse_keeps_old_note(self, graph: FakeGraph) -> None:
        graph.entities[DS1].deprecated = True
        graph.entities[DS1].note = "use table_b"
        plan = plan_deprecation(graph, DS1, deprecated=False)  # type: ignore[arg-type]
        assert plan is not None
        assert plan.inverse == EditOp(
            EditKind.SET_DEPRECATION, DS1, deprecated=True, note="use table_b"
        )

    def test_owner_and_ownership_type_parsing(self) -> None:
        assert owner_urn("alice") == "urn:li:corpuser:alice"
        assert owner_urn("group:data-eng") == "urn:li:corpGroup:data-eng"
        assert owner_urn("urn:li:corpGroup:x") == "urn:li:corpGroup:x"
        assert ownership_type_urn("technical") == TECHNICAL
        with pytest.raises(Exception, match="Unknown ownership type"):
            ownership_type_urn("captain")

    def test_op_round_trips_through_dict(self) -> None:
        op = EditOp(EditKind.SET_DEPRECATION, DS1, deprecated=False, note="")
        assert EditOp.from_dict(json.loads(json.dumps(op.to_dict()))) == op


class TestEditCommands:
    def test_add_tag_to_several_entities_and_skip_noops(
        self, run: Invoke, graph: FakeGraph
    ) -> None:
        graph.entities[DS2].tags.add("urn:li:tag:pii")
        out = run("add-tag", "--urn", DS1, "--urn", DS2, "--tag", "pii")
        assert [c["urn"] for c in out["changed"]] == [DS1]
        assert out["unchanged"] == [DS2]
        assert graph.mutations == ["addTag"]

    def test_dry_run_changes_nothing(self, run: Invoke, graph: FakeGraph) -> None:
        out = run("set-description", "--urn", DS1, "--text", "hello", "--dry-run")
        assert out["would_apply"][0]["description"] == "hello"
        assert graph.mutations == []
        assert run("history")["edits"] == []

    def test_unknown_urn_fails_before_any_edit(
        self, run: Invoke, graph: FakeGraph
    ) -> None:
        missing = "urn:li:dataset:(urn:li:dataPlatform:hive,nope,PROD)"
        out = run(
            "add-tag", "--urn", DS1, "--urn", missing, "--tag", "pii", expect_ok=False
        )
        assert out["exit_code"] == 3
        assert graph.mutations == []

    def test_missing_reference_fails_before_any_edit(
        self, run: Invoke, graph: FakeGraph
    ) -> None:
        out = run("add-owner", "--urn", DS1, "--owner", "group:nobody", expect_ok=False)
        assert out["exit_code"] == 3
        assert graph.mutations == []

    def test_undo_and_redo(self, run: Invoke, graph: FakeGraph) -> None:
        run("add-owner", "--urn", DS1, "--urn", DS2, "--owner", "group:data-eng")
        run("set-domain", "--urn", DS1, "--domain", "finance")

        assert run("undo")["undone"] == "set-domain"
        assert graph.entities[DS1].domain is None
        assert run("undo")["undone"] == "add-owner"
        assert not graph.entities[DS1].owners and not graph.entities[DS2].owners
        run("undo", expect_ok=False)

        assert run("redo")["redone"] == "add-owner"
        assert graph.entities[DS2].owners == {("urn:li:corpGroup:data-eng", TECHNICAL)}

    def test_new_edit_clears_redo(self, run: Invoke, graph: FakeGraph) -> None:
        run("add-tag", "--urn", DS1, "--tag", "a")
        run("undo")
        run("add-tag", "--urn", DS1, "--tag", "b")
        run("redo", expect_ok=False)

    def test_partial_failure_records_what_was_applied(
        self, run: Invoke, graph: FakeGraph
    ) -> None:
        graph.fail_on = DS2
        run("add-tag", "--urn", DS1, "--urn", DS2, "--tag", "pii", expect_ok=False)
        assert graph.entities[DS1].tags == {"urn:li:tag:pii"}
        graph.fail_on = None
        run("undo")
        assert graph.entities[DS1].tags == set()

    def test_history_is_per_server(self, run: Invoke, graph: FakeGraph) -> None:
        run("add-tag", "--urn", DS1, "--tag", "pii")
        graph.config.server = "http://other:8080"
        run("undo", expect_ok=False)
        assert graph.entities[DS1].tags == {"urn:li:tag:pii"}

    def test_corrupt_history_file_is_reported(
        self, run: Invoke, tmp_path: Path, graph: FakeGraph
    ) -> None:
        (tmp_path / "history.json").write_text("{not json")
        run("add-tag", "--urn", DS1, "--tag", "pii", expect_ok=False)
        assert graph.mutations == []


def test_get_summary(graph: FakeGraph) -> None:
    graph.entities[DS1].tags.add("urn:li:tag:pii")
    graph.entities[DS1].description = "edited"

    @contextlib.contextmanager
    def fake_default_graph(*args: object) -> Iterator[FakeGraph]:
        yield graph

    with patch("datahub.cli.get_cli.get_default_graph", fake_default_graph):
        result = CliRunner().invoke(get, ["summary", "--urn", DS1])
    assert result.exit_code == 0, result.output
    summary = json.loads(result.stdout)
    assert summary["tags"] == ["urn:li:tag:pii"]
    assert summary["description"] == "edited"
    assert summary["editable_description"] == "edited"
