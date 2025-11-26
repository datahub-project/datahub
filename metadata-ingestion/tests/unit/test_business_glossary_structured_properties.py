"""Tests for structured properties support in business glossary terms."""

from typing import cast

from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metadata.business_glossary import (
    BusinessGlossarySourceConfig,
    DefaultConfig,
    GlossaryTermConfig,
    Owners,
    get_mces_from_term,
)
from datahub.metadata.schema_classes import (
    OwnershipClass,
    StructuredPropertiesClass,
)


def create_default_ownership() -> OwnershipClass:
    """Helper to create a default OwnershipClass for tests."""
    return OwnershipClass(owners=[])


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_with_structured_properties():
    """Test that structured properties are correctly emitted for glossary terms."""
    # Create a glossary term with structured properties
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term with structured properties",
        structured_properties={
            "io.appsflyer.glossary.status": "In Glossary",
            "io.appsflyer.glossary.priority": "P1",
        },
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    # Create minimal context
    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    # Generate MCEs/MCPs
    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Should have at least 2 items: MCE for term + MCP for structured properties
    assert len(results) >= 2

    # Find the structured properties MCP
    structured_props_mcp = None
    for result in results:
        if hasattr(result, "aspect") and isinstance(
            result.aspect, StructuredPropertiesClass
        ):
            structured_props_mcp = result
            break

    assert structured_props_mcp is not None, "Structured properties MCP not found"

    # Verify the structured properties aspect
    aspect = structured_props_mcp.aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    # Cast for type safety
    aspect = cast(StructuredPropertiesClass, aspect)
    assert aspect.properties is not None
    assert len(aspect.properties) == 2

    # Verify property URNs are correctly formatted
    property_urns = {prop.propertyUrn for prop in aspect.properties}
    assert "urn:li:structuredProperty:io.appsflyer.glossary.status" in property_urns
    assert "urn:li:structuredProperty:io.appsflyer.glossary.priority" in property_urns

    # Verify values
    for prop in aspect.properties:
        if prop.propertyUrn == "urn:li:structuredProperty:io.appsflyer.glossary.status":
            assert prop.values == ["In Glossary"]
        elif (
            prop.propertyUrn
            == "urn:li:structuredProperty:io.appsflyer.glossary.priority"
        ):
            assert prop.values == ["P1"]


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_with_multi_valued_structured_properties():
    """Test that multi-valued structured properties are correctly handled."""
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term with multi-valued structured properties",
        structured_properties={
            "io.appsflyer.glossary.tags": ["tag1", "tag2", "tag3"],
            "io.appsflyer.glossary.owners": ["owner1", "owner2"],
        },
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Find the structured properties MCP
    structured_props_mcp = None
    for result in results:
        if hasattr(result, "aspect") and isinstance(
            result.aspect, StructuredPropertiesClass
        ):
            structured_props_mcp = result
            break

    assert structured_props_mcp is not None

    aspect = structured_props_mcp.aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    aspect = cast(StructuredPropertiesClass, aspect)
    assert aspect.properties is not None
    assert len(aspect.properties) == 2

    # Verify multi-valued properties
    for prop in aspect.properties:
        if prop.propertyUrn == "urn:li:structuredProperty:io.appsflyer.glossary.tags":
            assert prop.values == ["tag1", "tag2", "tag3"]
        elif (
            prop.propertyUrn == "urn:li:structuredProperty:io.appsflyer.glossary.owners"
        ):
            assert prop.values == ["owner1", "owner2"]


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_with_numeric_structured_properties():
    """Test that numeric structured properties are correctly handled."""
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term with numeric structured properties",
        structured_properties={
            "io.appsflyer.glossary.score": 95.5,
            "io.appsflyer.glossary.count": 42,
        },
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Find the structured properties MCP
    structured_props_mcp = None
    for result in results:
        if hasattr(result, "aspect") and isinstance(
            result.aspect, StructuredPropertiesClass
        ):
            structured_props_mcp = result
            break

    assert structured_props_mcp is not None

    aspect = structured_props_mcp.aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    aspect = cast(StructuredPropertiesClass, aspect)
    assert aspect.properties is not None
    assert len(aspect.properties) == 2

    # Verify numeric values are preserved
    for prop in aspect.properties:
        if prop.propertyUrn == "urn:li:structuredProperty:io.appsflyer.glossary.score":
            assert prop.values == [95.5]
        elif (
            prop.propertyUrn == "urn:li:structuredProperty:io.appsflyer.glossary.count"
        ):
            assert prop.values == [42]


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_without_structured_properties():
    """Test that terms without structured properties work correctly (backward compatibility)."""
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term without structured properties",
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Should have at least the MCE for the term
    assert len(results) >= 1

    # Verify no structured properties MCP is emitted
    has_structured_props = any(
        hasattr(result, "aspect")
        and isinstance(result.aspect, StructuredPropertiesClass)
        for result in results
    )
    assert not has_structured_props, "Structured properties MCP should not be emitted"


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_with_empty_structured_properties():
    """Test that terms with empty structured properties dict work correctly."""
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term with empty structured properties",
        structured_properties={},
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Verify no structured properties MCP is emitted for empty dict
    has_structured_props = any(
        hasattr(result, "aspect")
        and isinstance(result.aspect, StructuredPropertiesClass)
        for result in results
    )
    assert not has_structured_props, (
        "Structured properties MCP should not be emitted for empty dict"
    )


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_with_urn_prefixed_properties():
    """Test that property keys with URN prefix are handled correctly."""
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term with URN-prefixed structured properties",
        structured_properties={
            "urn:li:structuredProperty:io.appsflyer.glossary.status": "Active",
            "io.appsflyer.glossary.priority": "P1",  # Without prefix
        },
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Find the structured properties MCP
    structured_props_mcp = None
    for result in results:
        if hasattr(result, "aspect") and isinstance(
            result.aspect, StructuredPropertiesClass
        ):
            structured_props_mcp = result
            break

    assert structured_props_mcp is not None

    aspect = structured_props_mcp.aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    aspect = cast(StructuredPropertiesClass, aspect)
    assert aspect.properties is not None
    assert len(aspect.properties) == 2

    # Verify both properties have correct URN format
    property_urns = {prop.propertyUrn for prop in aspect.properties}
    assert "urn:li:structuredProperty:io.appsflyer.glossary.status" in property_urns
    assert "urn:li:structuredProperty:io.appsflyer.glossary.priority" in property_urns


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_with_empty_string_values():
    """Test that empty string values are handled correctly."""
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term with empty string structured property",
        structured_properties={
            "io.appsflyer.glossary.empty": "",
            "io.appsflyer.glossary.valid": "value",
        },
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Find the structured properties MCP
    structured_props_mcp = None
    for result in results:
        if hasattr(result, "aspect") and isinstance(
            result.aspect, StructuredPropertiesClass
        ):
            structured_props_mcp = result
            break

    assert structured_props_mcp is not None

    aspect = structured_props_mcp.aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    aspect = cast(StructuredPropertiesClass, aspect)
    assert aspect.properties is not None
    assert len(aspect.properties) == 2

    # Verify empty string is preserved (DataHub backend will validate)
    for prop in aspect.properties:
        if prop.propertyUrn == "urn:li:structuredProperty:io.appsflyer.glossary.empty":
            assert prop.values == [""]
        elif (
            prop.propertyUrn == "urn:li:structuredProperty:io.appsflyer.glossary.valid"
        ):
            assert prop.values == ["value"]


@freeze_time("2024-01-01 00:00:00")
def test_glossary_term_with_mixed_type_structured_properties():
    """Test that mixed types (string, number, list) are handled correctly."""
    term = GlossaryTermConfig(
        name="Test Term",
        description="A test term with mixed-type structured properties",
        structured_properties={
            "io.appsflyer.glossary.string_prop": "value",
            "io.appsflyer.glossary.number_prop": 123.45,
            "io.appsflyer.glossary.list_prop": ["item1", "item2"],
            "io.appsflyer.glossary.mixed_list": ["string", 42, 3.14],
        },
    )
    term._urn = "urn:li:glossaryTerm:test_term"

    ctx = PipelineContext(run_id="test-run")
    defaults = DefaultConfig(owners=Owners(users=["test_user"]))
    config = BusinessGlossarySourceConfig(
        file="test.yml",
        enable_auto_id=False,
    )

    results = list(
        get_mces_from_term(
            glossaryTerm=term,
            path_vs_id={},
            parentNode=None,
            parentOwnership=create_default_ownership(),
            defaults=defaults,
            ingestion_config=config,
            ctx=ctx,
        )
    )

    # Find the structured properties MCP
    structured_props_mcp = None
    for result in results:
        if hasattr(result, "aspect") and isinstance(
            result.aspect, StructuredPropertiesClass
        ):
            structured_props_mcp = result
            break

    assert structured_props_mcp is not None

    aspect = structured_props_mcp.aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    aspect = cast(StructuredPropertiesClass, aspect)
    assert aspect.properties is not None
    assert len(aspect.properties) == 4

    # Verify each property type
    for prop in aspect.properties:
        if (
            prop.propertyUrn
            == "urn:li:structuredProperty:io.appsflyer.glossary.string_prop"
        ):
            assert prop.values == ["value"]
        elif (
            prop.propertyUrn
            == "urn:li:structuredProperty:io.appsflyer.glossary.number_prop"
        ):
            assert prop.values == [123.45]
        elif (
            prop.propertyUrn
            == "urn:li:structuredProperty:io.appsflyer.glossary.list_prop"
        ):
            assert prop.values == ["item1", "item2"]
        elif (
            prop.propertyUrn
            == "urn:li:structuredProperty:io.appsflyer.glossary.mixed_list"
        ):
            assert prop.values == ["string", 42, 3.14]
