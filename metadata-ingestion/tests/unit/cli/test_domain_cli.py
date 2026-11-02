from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner, Result

from datahub.cli.specific.domain_cli import domain
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import AuditStampClass, DomainPropertiesClass


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    graph = MagicMock()
    graph.exists.return_value = True
    graph.get_aspect.return_value = None
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


@pytest.fixture
def domain_yaml(tmp_path: Path) -> Path:
    yaml_file = tmp_path / "domain.yaml"
    yaml_file.write_text("id: marketing\ndisplay_name: Marketing\n")
    return yaml_file


def test_upsert_exits_non_zero_when_emit_fails(
    graph: MagicMock, domain_yaml: Path
) -> None:
    graph.emit.side_effect = RuntimeError("GMS rejected the write")

    result = _invoke(["upsert", "-f", str(domain_yaml)])

    assert result.exit_code != 0
    assert "GMS rejected the write" in result.output


def test_upsert_creates_missing_domain_with_full_aspect(
    graph: MagicMock, domain_yaml: Path
) -> None:
    graph.exists.return_value = False

    result = _invoke(["upsert", "-f", str(domain_yaml)])

    assert result.exit_code == 0, result.output
    properties = [
        mcp.aspect
        for mcp in _emitted(graph)
        if isinstance(mcp, MetadataChangeProposalWrapper)
        and isinstance(mcp.aspect, DomainPropertiesClass)
    ]
    assert [p.name for p in properties] == ["Marketing"]


def test_update_keeps_existing_created_stamp_and_custom_properties(
    graph: MagicMock, domain_yaml: Path
) -> None:
    created = AuditStampClass(time=1, actor="urn:li:corpuser:admin")
    graph.get_aspect.return_value = DomainPropertiesClass(
        name="Old", created=created, customProperties={"team": "growth"}
    )

    result = _invoke(["update", "-f", str(domain_yaml)])

    assert result.exit_code == 0, result.output
    properties = _emitted(graph)[0].aspect
    assert properties.name == "Marketing"
    assert properties.created == created
    assert properties.customProperties == {"team": "growth"}


def test_delete_removes_references_and_hard_deletes(graph: MagicMock) -> None:
    result = _invoke(["delete", "--urn", "marketing"])

    assert result.exit_code == 0, result.output
    graph.delete_references_to_urn.assert_called_once_with("urn:li:domain:marketing")
    graph.delete_entity.assert_called_once_with("urn:li:domain:marketing", hard=True)


def test_delete_requires_urn_or_file(graph: MagicMock) -> None:
    result = _invoke(["delete"])

    assert result.exit_code != 0
    graph.delete_entity.assert_not_called()


def test_set_parent_aborts_for_missing_domain(graph: MagicMock) -> None:
    graph.exists.return_value = False

    result = _invoke(["set_parent", "--urn", "marketing", "--parent", "root"])

    assert result.exit_code != 0
    graph.emit.assert_not_called()


def test_clearing_absent_parent_emits_nothing(graph: MagicMock) -> None:
    graph.get_aspect.return_value = DomainPropertiesClass(name="Marketing")

    result = _invoke(["set_parent", "--urn", "marketing"])

    assert result.exit_code == 0, result.output
    graph.emit.assert_not_called()


def _hierarchy(
    parents: Dict[str, str],
) -> Callable[..., Optional[DomainPropertiesClass]]:
    def get_aspect(urn: str, aspect_type: type) -> Optional[DomainPropertiesClass]:
        return DomainPropertiesClass(name=urn, parentDomain=parents.get(urn))

    return get_aspect


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


def test_upsert_rejects_parent_cycle(graph: MagicMock, tmp_path: Path) -> None:
    graph.get_aspect.side_effect = _hierarchy(
        {"urn:li:domain:child": "urn:li:domain:marketing"}
    )
    yaml_file = tmp_path / "domain.yaml"
    yaml_file.write_text("id: marketing\nparent_domain: urn:li:domain:child\n")

    result = _invoke(["upsert", "-f", str(yaml_file)])

    assert result.exit_code != 0
    graph.emit.assert_not_called()


def test_set_parent_allows_moving_under_an_unrelated_domain(graph: MagicMock) -> None:
    graph.get_aspect.side_effect = _hierarchy(
        {"urn:li:domain:sales": "urn:li:domain:root"}
    )

    result = _invoke(["set_parent", "--urn", "marketing", "--parent", "sales"])

    assert result.exit_code == 0, result.output
    graph.emit.assert_called_once()
