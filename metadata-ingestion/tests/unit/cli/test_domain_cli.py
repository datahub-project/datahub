import json
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner, Result

from datahub.cli.specific.domain_cli import domain
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DomainPropertiesClass,
    OwnershipTypeClass,
)


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    graph = MagicMock()
    graph.exists.return_value = True
    graph.get_aspect.return_value = None
    graph.get_urns_by_filter.return_value = []
    with (
        patch("datahub.cli.specific.domain_cli.get_default_graph") as get_default_graph,
        patch("datahub.upgrade.upgrade.check_upgrade_post"),
    ):
        get_default_graph.return_value.__enter__.return_value = graph
        yield graph


def _invoke(args: List[str]) -> Result:
    return CliRunner().invoke(domain, args)


def _emitted(graph: MagicMock) -> list:
    return [c.args[0] for c in graph.emit.call_args_list]


def _patch_ops(graph: MagicMock, aspect_name: str) -> List[dict]:
    ops: List[dict] = []
    for mcp in _emitted(graph):
        if mcp.aspectName == aspect_name and mcp.changeType == ChangeTypeClass.PATCH:
            payload = json.loads(mcp.aspect.value)
            # Array aspects such as ownership use the GenericJsonPatch envelope.
            ops.extend(payload["patch"] if isinstance(payload, dict) else payload)
    return ops


def _hierarchy(
    parents: Dict[str, str],
) -> Callable[..., Optional[DomainPropertiesClass]]:
    def get_aspect(urn: str, aspect_type: type) -> Optional[DomainPropertiesClass]:
        return DomainPropertiesClass(name=urn, parentDomain=parents.get(urn))

    return get_aspect


@pytest.fixture
def write_yaml(tmp_path: Path) -> Callable[[str], str]:
    def write(text: str) -> str:
        yaml_file = tmp_path / "domain.yaml"
        yaml_file.write_text(text)
        return str(yaml_file)

    return write


def test_upsert_exits_non_zero_when_emit_fails(
    graph: MagicMock, write_yaml: Callable[[str], str]
) -> None:
    graph.emit.side_effect = RuntimeError("GMS rejected the write")

    result = _invoke(["upsert", "-f", write_yaml("id: marketing\ndescription: d\n")])

    assert result.exit_code != 0
    assert "GMS rejected the write" in result.output


def test_upsert_creates_missing_domain_with_full_aspect(
    graph: MagicMock, write_yaml: Callable[[str], str]
) -> None:
    graph.exists.return_value = False

    result = _invoke(
        ["upsert", "-f", write_yaml("id: marketing\ndisplay_name: Marketing\n")]
    )

    assert result.exit_code == 0, result.output
    assert [
        mcp.aspect.name
        for mcp in _emitted(graph)
        if isinstance(mcp, MetadataChangeProposalWrapper)
        and isinstance(mcp.aspect, DomainPropertiesClass)
    ] == ["Marketing"]


def test_upsert_patches_an_existing_domain(
    graph: MagicMock, write_yaml: Callable[[str], str]
) -> None:
    result = _invoke(["upsert", "-f", write_yaml("id: marketing\ndescription: New\n")])

    assert result.exit_code == 0, result.output
    assert all(mcp.changeType == ChangeTypeClass.PATCH for mcp in _emitted(graph))
    assert _patch_ops(graph, "domainProperties") == [
        {"op": "add", "path": "/description", "value": "New"}
    ]


@pytest.mark.parametrize(
    "yaml_text, expected_name",
    [
        ("id: marketing\ndisplay_name: Marketing\n", "Marketing"),
        ("id: marketing\n", "Old"),
    ],
)
def test_update_keeps_name_created_and_custom_properties(
    graph: MagicMock,
    write_yaml: Callable[[str], str],
    yaml_text: str,
    expected_name: str,
) -> None:
    created = AuditStampClass(time=1, actor="urn:li:corpuser:admin")
    graph.get_aspect.return_value = DomainPropertiesClass(
        name="Old", created=created, customProperties={"team": "growth"}
    )

    result = _invoke(["update", "-f", write_yaml(yaml_text)])

    assert result.exit_code == 0, result.output
    properties = _emitted(graph)[0].aspect
    assert properties.name == expected_name
    assert properties.created == created
    assert properties.customProperties == {"team": "growth"}


def test_upsert_rejects_parent_cycle(
    graph: MagicMock, write_yaml: Callable[[str], str]
) -> None:
    graph.get_aspect.side_effect = _hierarchy(
        {"urn:li:domain:child": "urn:li:domain:marketing"}
    )

    result = _invoke(
        [
            "upsert",
            "-f",
            write_yaml("id: marketing\nparent_domain: urn:li:domain:child\n"),
        ]
    )

    assert result.exit_code != 0
    assert "descendants" in result.output
    graph.emit.assert_not_called()


def test_get_exits_non_zero_for_missing_domain(graph: MagicMock) -> None:
    graph.exists.return_value = False

    result = _invoke(["get", "--urn", "marketing"])

    assert result.exit_code != 0
    assert "does not exist" in result.output


def test_delete_hard_deletes_then_removes_references(graph: MagicMock) -> None:
    result = _invoke(["delete", "--urn", "marketing"])

    assert result.exit_code == 0, result.output
    assert graph.method_calls[-2:] == [
        call.delete_entity("urn:li:domain:marketing", hard=True),
        call.delete_references_to_urn("urn:li:domain:marketing"),
    ]


def test_delete_refuses_a_domain_with_child_domains(graph: MagicMock) -> None:
    # Removing references would strip parentDomain from the children.
    graph.get_urns_by_filter.return_value = ["urn:li:domain:campaigns"]

    result = _invoke(["delete", "--urn", "marketing"])

    assert result.exit_code != 0
    assert "urn:li:domain:campaigns" in result.output
    graph.delete_entity.assert_not_called()
    graph.delete_references_to_urn.assert_not_called()


def test_delete_from_file_only_needs_the_id(
    graph: MagicMock, write_yaml: Callable[[str], str]
) -> None:
    # The parent no longer resolving must not block deleting the child.
    result = _invoke(
        ["delete", "-f", write_yaml("id: campaigns\nparent_domain: Gone\n")]
    )

    assert result.exit_code == 0, result.output
    graph.delete_entity.assert_called_once_with("urn:li:domain:campaigns", hard=True)


def test_delete_aborts_for_missing_domain(graph: MagicMock) -> None:
    graph.exists.return_value = False

    result = _invoke(["delete", "--urn", "marketing"])

    assert result.exit_code != 0
    graph.delete_entity.assert_not_called()


def test_delete_requires_urn_or_file(graph: MagicMock) -> None:
    result = _invoke(["delete"])

    assert result.exit_code != 0
    graph.delete_entity.assert_not_called()


@pytest.mark.parametrize(
    "missing_urn", ["urn:li:domain:marketing", "urn:li:domain:root"]
)
def test_set_parent_aborts_when_domain_or_parent_is_missing(
    graph: MagicMock, missing_urn: str
) -> None:
    graph.exists.side_effect = lambda urn: urn != missing_urn

    result = _invoke(["set_parent", "--urn", "marketing", "--parent", "root"])

    assert result.exit_code != 0
    graph.emit.assert_not_called()


def test_set_parent_rejects_moving_under_a_descendant(graph: MagicMock) -> None:
    # root <- child <- grandchild
    graph.get_aspect.side_effect = _hierarchy(
        {
            "urn:li:domain:child": "urn:li:domain:root",
            "urn:li:domain:grandchild": "urn:li:domain:child",
        }
    )

    result = _invoke(["set_parent", "--urn", "root", "--parent", "grandchild"])

    assert result.exit_code != 0
    assert "descendants" in result.output
    graph.emit.assert_not_called()


def test_set_parent_moves_under_an_unrelated_domain(graph: MagicMock) -> None:
    graph.get_aspect.side_effect = _hierarchy(
        {"urn:li:domain:sales": "urn:li:domain:root"}
    )

    result = _invoke(["set_parent", "--urn", "marketing", "--parent", "sales"])

    assert result.exit_code == 0, result.output
    assert _patch_ops(graph, "domainProperties") == [
        {"op": "add", "path": "/parentDomain", "value": "urn:li:domain:sales"}
    ]


def test_set_parent_without_parent_makes_domain_top_level(graph: MagicMock) -> None:
    graph.get_aspect.return_value = DomainPropertiesClass(
        name="Marketing", parentDomain="urn:li:domain:root"
    )

    result = _invoke(["set_parent", "--urn", "marketing"])

    assert result.exit_code == 0, result.output
    assert [(op["op"], op["path"]) for op in _patch_ops(graph, "domainProperties")] == [
        ("remove", "/parentDomain")
    ]


def test_clearing_absent_parent_emits_nothing(graph: MagicMock) -> None:
    graph.get_aspect.return_value = DomainPropertiesClass(name="Marketing")

    result = _invoke(["set_parent", "--urn", "marketing"])

    assert result.exit_code == 0, result.output
    graph.emit.assert_not_called()


def test_set_description_from_markdown_file(graph: MagicMock, tmp_path: Path) -> None:
    md_file = tmp_path / "description.md"
    md_file.write_text("# Marketing\nOwns brand assets.")

    result = _invoke(
        ["set_description", "--urn", "marketing", "--md-file", str(md_file)]
    )

    assert result.exit_code == 0, result.output
    assert _patch_ops(graph, "domainProperties") == [
        {
            "op": "add",
            "path": "/description",
            "value": "# Marketing\nOwns brand assets.",
        }
    ]


def test_add_and_remove_owner(graph: MagicMock) -> None:
    result = _invoke(
        [
            "add_owner",
            "--urn",
            "marketing",
            "--owner",
            "jdoe",
            "--owner-type",
            OwnershipTypeClass.BUSINESS_OWNER,
        ]
    )
    assert result.exit_code == 0, result.output
    [added] = _patch_ops(graph, "ownership")
    assert added["op"] == "add"
    assert added["value"]["owner"] == "urn:li:corpuser:jdoe"
    assert added["value"]["type"] == OwnershipTypeClass.BUSINESS_OWNER

    graph.emit.reset_mock()
    result = _invoke(["remove_owner", "--urn", "marketing", "urn:li:corpuser:jdoe"])
    assert result.exit_code == 0, result.output
    assert [op["op"] for op in _patch_ops(graph, "ownership")] == ["remove"]
