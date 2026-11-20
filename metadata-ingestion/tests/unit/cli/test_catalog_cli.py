import contextlib
import json
from typing import Dict, Iterator, List, Tuple
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from datahub.cli.catalog_cli import EXIT_ALREADY_EXISTS, domain, glossary, tag
from datahub.configuration.common import GraphError


class RecordingGraph:
    def __init__(self, existing: Tuple[str, ...] = ()) -> None:
        self.existing = set(existing)
        self.calls: List[Tuple[str, Dict]] = []

    def exists(self, urn: str) -> bool:
        return urn in self.existing

    def execute_graphql(self, query: str, variables: Dict) -> Dict:
        self.calls.append((query, variables))
        if "createTag" in query and variables["input"]["id"] == "pii":
            raise GraphError("[{'message': 'This Tag already exists!'}]")
        for name in ("createTag", "createGlossaryTerm", "createDomain"):
            if name in query:
                return {name: "urn:li:created"}
        for name in ("deleteTag", "deleteGlossaryEntity", "deleteDomain"):
            if name in query:
                return {name: True}
        return {
            "searchAcrossEntities": {
                "total": 1,
                "searchResults": [
                    {"entity": {"urn": "urn:li:tag:pii", "properties": {"name": "PII"}}}
                ],
            }
        }


@pytest.fixture
def graph() -> Iterator[RecordingGraph]:
    g = RecordingGraph(existing=("urn:li:tag:pii", "urn:li:domain:finance"))

    @contextlib.contextmanager
    def fake_default_graph(*args: object) -> Iterator[RecordingGraph]:
        yield g

    with patch("datahub.cli.catalog_cli.get_default_graph", fake_default_graph):
        yield g


def test_tag_create_defaults_id_to_name(graph: RecordingGraph) -> None:
    result = CliRunner().invoke(tag, ["create", "--name", "gdpr"])
    assert result.exit_code == 0, result.output
    assert graph.calls[0][1]["input"] == {"id": "gdpr", "name": "gdpr"}


def test_create_existing_reports_already_exists(graph: RecordingGraph) -> None:
    result = CliRunner().invoke(tag, ["create", "--name", "pii"])
    assert result.exit_code == EXIT_ALREADY_EXISTS
    assert json.loads(result.stderr)["error"] == "already_exists"


def test_glossary_term_parent_becomes_node_urn(graph: RecordingGraph) -> None:
    result = CliRunner().invoke(
        glossary, ["create-term", "--name", "Revenue", "--parent", "finance"]
    )
    assert result.exit_code == 0, result.output
    assert graph.calls[0][1]["input"]["parentNode"] == "urn:li:glossaryNode:finance"


def test_list_returns_names(graph: RecordingGraph) -> None:
    result = CliRunner().invoke(tag, ["list"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["results"][0]["name"] == "PII"
    assert graph.calls[0][1]["input"]["types"] == ["TAG"]


def test_delete_needs_confirmation_or_force(graph: RecordingGraph) -> None:
    aborted = CliRunner().invoke(domain, ["delete", "--domain", "finance"], input="n\n")
    assert aborted.exit_code != 0
    assert graph.calls == []

    result = CliRunner().invoke(domain, ["delete", "--domain", "finance", "--force"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "urn": "urn:li:domain:finance",
        "deleted": True,
    }


def test_delete_missing_and_ambiguous(graph: RecordingGraph) -> None:
    missing = CliRunner().invoke(tag, ["delete", "--tag", "nope", "--force"])
    assert missing.exit_code == 3
    bare_id = CliRunner().invoke(glossary, ["delete", "--urn", "revenue", "--force"])
    assert bare_id.exit_code == 2
    assert graph.calls == []
