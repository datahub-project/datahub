import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from datahub.api.entities.dataproduct.dataproduct import Ownership
from datahub.api.entities.domain.domain import Domain
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DomainPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.specific.domain import DomainPatchBuilder
from tests.test_helpers.graph_helpers import MockDataHubGraph


@pytest.fixture
def base_mock_graph() -> MockDataHubGraph:
    return MockDataHubGraph(
        entity_graph={
            "urn:li:domain:12345": {
                "domainProperties": DomainPropertiesClass(
                    name="Marketing", description="Marketing Domain"
                )
            }
        }
    )


@pytest.fixture
def hierarchy_graph() -> MockDataHubGraph:
    # root <- child <- grandchild
    return MockDataHubGraph(
        entity_graph={
            "urn:li:domain:root": {
                "domainProperties": DomainPropertiesClass(name="Root")
            },
            "urn:li:domain:child": {
                "domainProperties": DomainPropertiesClass(
                    name="Child", parentDomain="urn:li:domain:root"
                )
            },
            "urn:li:domain:grandchild": {
                "domainProperties": DomainPropertiesClass(
                    name="Grandchild", parentDomain="urn:li:domain:child"
                )
            },
        }
    )


@pytest.fixture
def test_resources_dir(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath / "tests/unit/api/entities/domain"


def _write_yaml(tmp_path: Path, text: str) -> Path:
    yaml_file = tmp_path / "domain.yaml"
    yaml_file.write_text(text)
    return yaml_file


def _aspects(mcps: List[Any]) -> Dict[str, Any]:
    return {
        mcp.aspectName: mcp.aspect
        for mcp in mcps
        if isinstance(mcp, MetadataChangeProposalWrapper) and mcp.aspectName
    }


def _patch_ops(mcps: List[Any], aspect_name: str = "domainProperties") -> List[dict]:
    ops: List[dict] = []
    for mcp in mcps:
        if mcp.aspectName == aspect_name and mcp.changeType == ChangeTypeClass.PATCH:
            ops.extend(json.loads(mcp.aspect.value))
    return ops


def test_snapshot_emits_properties_owners_and_links(test_resources_dir: Path) -> None:
    domain = Domain.from_yaml(test_resources_dir / "domain.yaml")

    emitted = _aspects(list(domain.generate_mcp(upsert=False)))

    # GMS rejects aspects the domain entity doesn't declare in entity-registry.yml.
    assert set(emitted) == {"domainProperties", "ownership", "institutionalMemory"}
    properties = emitted["domainProperties"]
    assert properties.name == "Marketing"
    assert properties.description and "brand" in properties.description
    assert properties.parentDomain is None
    assert {o.owner: o.type for o in emitted["ownership"].owners} == {
        "urn:li:corpuser:jdoe": OwnershipTypeClass.BUSINESS_OWNER,
        "urn:li:corpuser:asmith": OwnershipTypeClass.TECHNICAL_OWNER,
    }
    assert [
        (e.url, e.description) for e in emitted["institutionalMemory"].elements
    ] == [("https://wiki.example.com/marketing", "Marketing runbook")]


def test_snapshot_name_falls_back_to_id() -> None:
    # name is required in the PDL, so a domain created without display_name
    # still needs one.
    emitted = _aspects(list(Domain(id="marketing").generate_mcp(upsert=False)))
    assert emitted["domainProperties"].name == "marketing"


def test_upsert_patches_name_description_and_parent() -> None:
    domain = Domain(
        id="campaigns",
        display_name="Campaigns",
        description="Campaign assets",
        parent_domain="urn:li:domain:12345",
    )

    ops = _patch_ops(list(domain.generate_mcp(upsert=True)))

    assert {"op": "add", "path": "/name", "value": "Campaigns"} in ops
    assert {"op": "add", "path": "/description", "value": "Campaign assets"} in ops
    assert {
        "op": "add",
        "path": "/parentDomain",
        "value": "urn:li:domain:12345",
    } in ops


def test_upsert_without_display_name_keeps_existing_name() -> None:
    domain = Domain(id="urn:li:domain:marketing", description="New description")

    ops = _patch_ops(list(domain.generate_mcp(upsert=True)))

    assert {"op": "add", "path": "/description", "value": "New description"} in ops
    assert not any(op["path"] == "/name" for op in ops)


def test_resolves_parent_by_name(
    test_resources_dir: Path, base_mock_graph: MockDataHubGraph
) -> None:
    domain = Domain.from_yaml(test_resources_dir / "domain_child.yaml", base_mock_graph)

    emitted = _aspects(list(domain.generate_mcp(upsert=False)))
    assert emitted["domainProperties"].parentDomain == "urn:li:domain:12345"


def test_parent_id_wins_over_another_domains_display_name(tmp_path: Path) -> None:
    graph = MockDataHubGraph(
        entity_graph={
            "urn:li:domain:sales": {
                "domainProperties": DomainPropertiesClass(name="Sales")
            },
            "urn:li:domain:other": {
                "domainProperties": DomainPropertiesClass(name="sales")
            },
        }
    )
    yaml_file = _write_yaml(tmp_path, "id: child\nparent_domain: sales\n")

    assert (
        Domain.from_yaml(yaml_file, graph).resolved_parent_domain_urn
        == "urn:li:domain:sales"
    )


def test_resolves_parent_name_with_four_hyphens(tmp_path: Path) -> None:
    # DomainRegistry passes these through unresolved, which would write a bare
    # name into the URN-typed parentDomain.
    graph = MockDataHubGraph(
        entity_graph={
            "urn:li:domain:abc": {
                "domainProperties": DomainPropertiesClass(name="eu-west-1-sales-ops")
            }
        }
    )
    yaml_file = _write_yaml(tmp_path, "id: child\nparent_domain: eu-west-1-sales-ops\n")

    assert (
        Domain.from_yaml(yaml_file, graph).resolved_parent_domain_urn
        == "urn:li:domain:abc"
    )


@pytest.mark.parametrize(
    "parent_domain, error",
    [
        ("Missing", "not found"),
        ("Analytics", "several domains"),
        ("urn:li:domain:missing", "does not exist"),
    ],
)
def test_from_yaml_rejects_unusable_parent(
    tmp_path: Path, parent_domain: str, error: str
) -> None:
    # Names only need to be unique among siblings.
    graph = MockDataHubGraph(
        entity_graph={
            "urn:li:domain:a1": {
                "domainProperties": DomainPropertiesClass(name="Analytics")
            },
            "urn:li:domain:a2": {
                "domainProperties": DomainPropertiesClass(name="Analytics")
            },
        }
    )
    yaml_file = _write_yaml(tmp_path, f"id: child\nparent_domain: {parent_domain}\n")

    with pytest.raises(ValueError, match=error):
        Domain.from_yaml(yaml_file, graph)


@pytest.mark.parametrize(
    "parent_domain", ["urn:li:domain:grandchild", "Grandchild", "urn:li:domain:root"]
)
def test_from_yaml_rejects_nesting_under_a_descendant(
    tmp_path: Path, hierarchy_graph: MockDataHubGraph, parent_domain: str
) -> None:
    yaml_file = _write_yaml(tmp_path, f"id: root\nparent_domain: {parent_domain}\n")

    with pytest.raises(ValueError, match="descendants"):
        Domain.from_yaml(yaml_file, hierarchy_graph)


def test_cycle_check_stops_on_a_cycle_already_in_the_hierarchy(tmp_path: Path) -> None:
    # a <-> b is already stored; nesting c under a must not loop forever.
    graph = MockDataHubGraph(
        entity_graph={
            "urn:li:domain:a": {
                "domainProperties": DomainPropertiesClass(
                    name="A", parentDomain="urn:li:domain:b"
                )
            },
            "urn:li:domain:b": {
                "domainProperties": DomainPropertiesClass(
                    name="B", parentDomain="urn:li:domain:a"
                )
            },
        }
    )
    yaml_file = _write_yaml(tmp_path, "id: c\nparent_domain: urn:li:domain:a\n")

    assert Domain.from_yaml(yaml_file, graph).resolved_parent_domain_urn == (
        "urn:li:domain:a"
    )


def test_emit_to_a_graph_checks_the_parent(hierarchy_graph: MockDataHubGraph) -> None:
    domain = Domain(id="root", parent_domain="urn:li:domain:grandchild")

    with pytest.raises(ValueError, match="descendants"):
        domain.emit(hierarchy_graph, upsert=True)
    assert hierarchy_graph.emitted == []


def test_generate_mcp_rejects_self_parent_without_graph() -> None:
    domain = Domain(id="marketing", parent_domain="urn:li:domain:marketing")
    with pytest.raises(ValueError, match="own parent"):
        list(domain.generate_mcp(upsert=False))


def test_parent_by_name_requires_graph(test_resources_dir: Path) -> None:
    with pytest.raises(ValueError, match="no DataHubGraph"):
        Domain.from_yaml(test_resources_dir / "domain_child.yaml")


def test_generate_mcp_rejects_unresolved_parent_name() -> None:
    # Emitting parentDomain=None instead would silently make it top-level.
    domain = Domain(id="x", display_name="X", parent_domain="Marketing")
    with pytest.raises(ValueError, match="not resolved"):
        list(domain.generate_mcp(upsert=False))


def test_round_trip_through_datahub_and_yaml(
    test_resources_dir: Path, tmp_path: Path
) -> None:
    graph = MockDataHubGraph()
    for mcp in Domain.from_yaml(test_resources_dir / "domain.yaml").generate_mcp(
        upsert=False
    ):
        assert mcp.entityUrn and mcp.aspectName
        graph.entity_graph.setdefault(mcp.entityUrn, {})[mcp.aspectName] = mcp.aspect

    reloaded = Domain.from_datahub(graph, id="urn:li:domain:marketing")
    assert reloaded.display_name == "Marketing"
    assert reloaded.owners == [
        Ownership(id="urn:li:corpuser:jdoe", type=OwnershipTypeClass.BUSINESS_OWNER),
        "urn:li:corpuser:asmith",
    ]
    assert reloaded.institutional_memory is not None

    # `domain get --to-file` followed by `domain upsert -f` targets the same URN.
    reloaded.to_yaml(tmp_path / "out.yaml")
    mcps = list(Domain.from_yaml(tmp_path / "out.yaml").generate_mcp(upsert=False))
    assert {mcp.entityUrn for mcp in mcps} == {"urn:li:domain:marketing"}
    assert _aspects(mcps)["domainProperties"].name == "Marketing"


def test_from_datahub_translates_custom_ownership_type() -> None:
    graph = MockDataHubGraph(
        entity_graph={
            "urn:li:domain:abc": {
                "domainProperties": DomainPropertiesClass(name="ABC"),
                "ownership": OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner="urn:li:corpuser:steward",
                            type=OwnershipTypeClass.CUSTOM,
                            typeUrn="urn:li:ownershipType:dataSteward",
                        )
                    ]
                ),
            }
        }
    )
    domain = Domain.from_datahub(graph, id="urn:li:domain:abc")
    assert domain.owners == [
        Ownership(id="urn:li:corpuser:steward", type="urn:li:ownershipType:dataSteward")
    ]


def test_from_datahub_raises_when_no_properties() -> None:
    graph = MockDataHubGraph(entity_graph={"urn:li:domain:ghost": {}})
    with pytest.raises(ValueError, match="domainProperties"):
        Domain.from_datahub(graph, id="urn:li:domain:ghost")


def test_patch_builder_parent_domain_ops() -> None:
    set_ops = _patch_ops(
        DomainPatchBuilder("urn:li:domain:x")
        .set_parent_domain("urn:li:domain:parent")
        .build()
    )
    assert set_ops == [
        {"op": "add", "path": "/parentDomain", "value": "urn:li:domain:parent"}
    ]

    remove_ops = _patch_ops(
        DomainPatchBuilder("urn:li:domain:x").remove_parent_domain().build()
    )
    assert [(op["op"], op["path"]) for op in remove_ops] == [
        ("remove", "/parentDomain")
    ]

    with pytest.raises(ValueError, match="remove_parent_domain"):
        DomainPatchBuilder("urn:li:domain:x").set_parent_domain("")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"id": ""},
        {"id": "urn:li:not-a-real-thing"},
        {"id": "urn:li:corpuser:jdoe"},
        {"id": "x", "parent_domain": "urn:li:glossaryTerm:Revenue"},
        {"id": "x", "parent_domain": "urn:li:domain"},
        {"id": "x", "owners": [{"id": "jdoe", "type": "urn:li:"}]},
        {"id": "x", "owners": [{"id": "jdoe", "type": "not-a-real-type"}]},
    ],
)
def test_rejects_invalid_ids_parents_and_owner_types(kwargs: Dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        Domain.model_validate(kwargs)
