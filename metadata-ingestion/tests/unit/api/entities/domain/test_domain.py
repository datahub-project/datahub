from pathlib import Path
from typing import Any, Dict, cast

import pytest

from datahub.api.entities.domain.domain import Domain, Ownership
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DomainPropertiesClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.specific.domain import DomainPatchBuilder
from tests.test_helpers.graph_helpers import MockDataHubGraph


@pytest.fixture
def base_entity_metadata() -> Dict[str, Dict[str, Any]]:
    """Pre-populated parent ``Marketing`` Domain for name-based lookup."""
    return {
        "urn:li:domain:12345": {
            "domainProperties": DomainPropertiesClass(
                name="Marketing", description="Marketing Domain"
            )
        }
    }


@pytest.fixture
def base_mock_graph(
    base_entity_metadata: Dict[str, Dict[str, Any]],
) -> MockDataHubGraph:
    return MockDataHubGraph(entity_graph=base_entity_metadata)


@pytest.fixture
def test_resources_dir(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath / "tests/unit/api/entities/domain"


def _aspects_by_name(mock_graph: MockDataHubGraph, urn: str) -> Dict[str, Any]:
    """Collect emitted ``aspectName → aspect`` pairs for a single URN.

    The return type is ``Any``-valued so test bodies can use duck-typed
    attribute access (``aspects["domainProperties"].name``) without
    casting each lookup to a concrete aspect class.
    """
    out: Dict[str, Any] = {}
    for mcp in mock_graph.emitted:
        if not isinstance(mcp, MetadataChangeProposalWrapper):
            continue
        if mcp.entityUrn == urn and mcp.aspectName:
            out[mcp.aspectName] = mcp.aspect
    return out


# ---------------------------------------------------------------------------
# generate_mcp — non-upsert (snapshot) path
# ---------------------------------------------------------------------------


def test_domain_from_yaml_emits_full_snapshot(
    test_resources_dir: Path, base_mock_graph: MockDataHubGraph
) -> None:
    domain = Domain.from_yaml(test_resources_dir / "domain.yaml")

    for mcp in domain.generate_mcp(upsert=False):
        base_mock_graph.emit(mcp)

    emitted = _aspects_by_name(base_mock_graph, "urn:li:domain:marketing")

    assert "domainProperties" in emitted
    properties = emitted["domainProperties"]
    assert properties.name == "Marketing"
    assert properties.description and "brand" in properties.description
    assert properties.parentDomain is None

    assert "ownership" in emitted
    assert {o.owner for o in emitted["ownership"].owners} == {
        "urn:li:corpuser:jdoe",
        "urn:li:corpuser:asmith",
    }
    owner_types = {o.owner: o.type for o in emitted["ownership"].owners}
    assert owner_types["urn:li:corpuser:jdoe"] == OwnershipTypeClass.BUSINESS_OWNER
    assert owner_types["urn:li:corpuser:asmith"] == OwnershipTypeClass.TECHNICAL_OWNER

    assert "globalTags" in emitted
    assert [t.tag for t in emitted["globalTags"].tags] == ["urn:li:tag:gold-tier"]

    assert "glossaryTerms" in emitted
    assert [t.urn for t in emitted["glossaryTerms"].terms] == [
        "urn:li:glossaryTerm:ClientsAndAccounts.AccountBalance"
    ]

    assert "status" in emitted and emitted["status"].removed is False


# ---------------------------------------------------------------------------
# Parent-domain resolution
# ---------------------------------------------------------------------------


def test_domain_resolves_parent_by_name_via_registry(
    test_resources_dir: Path, base_mock_graph: MockDataHubGraph
) -> None:
    domain = Domain.from_yaml(test_resources_dir / "domain_child.yaml", base_mock_graph)
    assert domain._resolved_parent_domain_urn == "urn:li:domain:12345"

    for mcp in domain.generate_mcp(upsert=False):
        base_mock_graph.emit(mcp)

    properties = cast(
        DomainPropertiesClass,
        _aspects_by_name(base_mock_graph, "urn:li:domain:marketing.campaigns")[
            "domainProperties"
        ],
    )
    assert properties.parentDomain == "urn:li:domain:12345"


def test_domain_parent_by_name_requires_graph(test_resources_dir: Path) -> None:
    with pytest.raises(ValueError, match="parent domain"):
        Domain.from_yaml(test_resources_dir / "domain_child.yaml")


def test_domain_with_urn_parent_does_not_require_graph() -> None:
    domain = Domain(
        id="marketing.brand",
        display_name="Brand",
        parent_domain="urn:li:domain:12345",
    )
    assert domain._resolved_parent_domain_urn == "urn:li:domain:12345"
    properties_aspect = next(
        m
        for m in domain.generate_mcp(upsert=False)
        if isinstance(m, MetadataChangeProposalWrapper)
        and m.aspectName == "domainProperties"
    ).aspect
    properties = cast(DomainPropertiesClass, properties_aspect)
    assert properties.parentDomain == "urn:li:domain:12345"


def test_generate_mcp_raises_when_parent_unresolved() -> None:
    """A name-based parent without a graph must fail loudly at MCP-time
    rather than silently emit ``parentDomain=None``."""
    domain = Domain(id="x", display_name="X", parent_domain="Marketing")
    with pytest.raises(Exception, match="parent domain"):
        list(domain.generate_mcp(upsert=False))


# ---------------------------------------------------------------------------
# Round-trip via from_datahub
# ---------------------------------------------------------------------------


def test_domain_round_trip_from_datahub(
    test_resources_dir: Path, base_mock_graph: MockDataHubGraph
) -> None:
    original = Domain.from_yaml(test_resources_dir / "domain.yaml")
    for mcp in original.generate_mcp(upsert=False):
        base_mock_graph.emit(mcp)
        # Replay into entity_graph since from_datahub reads from there.
        if mcp.entityUrn and mcp.aspectName and mcp.aspect:
            base_mock_graph.entity_graph.setdefault(mcp.entityUrn, {})[
                mcp.aspectName
            ] = mcp.aspect

    reloaded = Domain.from_datahub(base_mock_graph, id="urn:li:domain:marketing")

    assert reloaded.id == "urn:li:domain:marketing"
    assert reloaded.display_name == "Marketing"
    assert reloaded.description and "brand" in reloaded.description
    assert reloaded.parent_domain is None
    assert reloaded.tags == ["urn:li:tag:gold-tier"]
    assert reloaded.terms == ["urn:li:glossaryTerm:ClientsAndAccounts.AccountBalance"]


def test_domain_from_datahub_translates_custom_ownership_type() -> None:
    """CUSTOM ownership type round-trips via ``Ownership(type=typeUrn)``."""
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
                "globalTags": GlobalTagsClass(
                    tags=[TagAssociationClass(tag="urn:li:tag:a")]
                ),
                "glossaryTerms": None,
            }
        }
    )
    domain = Domain.from_datahub(graph, id="urn:li:domain:abc")
    assert domain.owners == [
        Ownership(
            id="urn:li:corpuser:steward",
            type="urn:li:ownershipType:dataSteward",
        )
    ]


def test_domain_from_datahub_raises_when_no_properties() -> None:
    graph = MockDataHubGraph(entity_graph={"urn:li:domain:ghost": {}})
    with pytest.raises(Exception, match="domainProperties"):
        Domain.from_datahub(graph, id="urn:li:domain:ghost")


# ---------------------------------------------------------------------------
# Upsert path uses the patch builder
# ---------------------------------------------------------------------------


def test_generate_mcp_upsert_uses_patch_builder(test_resources_dir: Path) -> None:
    """Upsert mode emits PATCH MCPs, not a full DomainProperties snapshot."""
    domain = Domain.from_yaml(test_resources_dir / "domain.yaml")
    mcps = list(domain.generate_mcp(upsert=True))
    property_mcps = [m for m in mcps if m.aspectName == "domainProperties"]
    assert property_mcps, "Expected at least one domainProperties patch MCP"
    assert all(getattr(m, "changeType", None) == "PATCH" for m in property_mcps), [
        getattr(m, "changeType", None) for m in property_mcps
    ]


# ---------------------------------------------------------------------------
# DomainPatchBuilder
# ---------------------------------------------------------------------------


def _patch_ops_for_aspect(builder: DomainPatchBuilder, aspect_name: str) -> list:
    """Return all JSON-Patch ops targeting ``aspect_name``.

    PATCH MCPs carry the raw JSON-Patch array in ``aspect.value``.
    """
    import json

    ops: list = []
    for mcp in builder.build():
        if mcp.aspectName != aspect_name or mcp.aspect is None:
            continue
        raw_bytes = mcp.aspect.value
        text = raw_bytes.decode("utf-8") if isinstance(raw_bytes, bytes) else raw_bytes
        ops.extend(json.loads(text))
    return ops


def test_patch_builder_set_name_emits_replace_op() -> None:
    ops = _patch_ops_for_aspect(
        DomainPatchBuilder("urn:li:domain:x").set_name("New Name"),
        "domainProperties",
    )
    assert {"op": "add", "path": "/name", "value": "New Name"} in ops


def test_patch_builder_set_parent_domain_add_and_clear() -> None:
    set_ops = _patch_ops_for_aspect(
        DomainPatchBuilder("urn:li:domain:x").set_parent_domain("urn:li:domain:parent"),
        "domainProperties",
    )
    assert {
        "op": "add",
        "path": "/parentDomain",
        "value": "urn:li:domain:parent",
    } in set_ops

    clear_ops = _patch_ops_for_aspect(
        DomainPatchBuilder("urn:li:domain:x").set_parent_domain(""),
        "domainProperties",
    )
    assert any(
        op["op"] == "remove" and op["path"] == "/parentDomain" for op in clear_ops
    )


def test_patch_builder_supports_ownership_mixin() -> None:
    """Pin that ``HasOwnershipPatch`` stays wired on the Domain builder."""
    builder = DomainPatchBuilder("urn:li:domain:x")
    builder.add_owner(
        OwnerClass(
            owner="urn:li:corpuser:jdoe", type=OwnershipTypeClass.TECHNICAL_OWNER
        )
    )
    ownership_mcps = [m for m in builder.build() if m.aspectName == "ownership"]
    assert ownership_mcps, "Expected ownership patch MCP from add_owner"


def test_patch_builder_supports_tag_and_term_mixins() -> None:
    """Pin that ``HasTagsPatch`` / ``HasTermsPatch`` stay wired on the
    Domain builder. ``GlobalTagsTemplate`` / ``GlossaryTermsTemplate``
    are registered as common templates on the backend, so these
    patches apply against a real Domain instance.
    """
    builder = DomainPatchBuilder("urn:li:domain:x")
    builder.add_tag(TagAssociationClass(tag="urn:li:tag:gold"))
    tag_mcps = [m for m in builder.build() if m.aspectName == "globalTags"]
    assert tag_mcps, "Expected globalTags patch MCP from add_tag"

    builder = DomainPatchBuilder("urn:li:domain:x")
    from datahub.metadata.schema_classes import GlossaryTermAssociationClass

    builder.add_term(GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:Revenue"))
    term_mcps = [m for m in builder.build() if m.aspectName == "glossaryTerms"]
    assert term_mcps, "Expected glossaryTerms patch MCP from add_term"


# ---------------------------------------------------------------------------
# Pydantic validation
# ---------------------------------------------------------------------------


def test_domain_rejects_empty_id() -> None:
    with pytest.raises(ValueError, match="id cannot be empty"):
        Domain(id="", display_name="X")


def test_domain_rejects_malformed_urn_id() -> None:
    with pytest.raises(ValueError):
        Domain(id="urn:li:not-a-real-thing", display_name="X")


def test_domain_invalid_owner_type_rejected() -> None:
    """``Ownership.type`` rejects values neither in the enum nor URN-shaped."""
    with pytest.raises(ValueError):
        Domain(
            id="x",
            display_name="X",
            owners=[Ownership(id="jdoe", type="not-a-real-type")],
        )
