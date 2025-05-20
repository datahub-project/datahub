from typing import Dict
from unittest.mock import MagicMock

import datahub.metadata.schema_classes as models
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metadata.business_glossary import (
    BusinessGlossarySourceConfig,
    DefaultConfig,
    GlossaryNodeConfig,
    GlossaryTermConfig,
    Owners,
    clean_url,
    create_id,
    get_mces_from_node,
    get_mces_from_term,
)


def test_clean_url():
    """Test the clean_url function with various input cases"""
    test_cases = [
        ("Basic Term", "Basic-Term"),
        ("Term With Spaces", "Term-With-Spaces"),
        ("Special@#$Characters!", "SpecialCharacters"),
        ("MixedCase Term", "MixedCase-Term"),
        ("Multiple   Spaces", "Multiple-Spaces"),
        ("Term-With-Hyphens", "Term-With-Hyphens"),
        ("Term.With.Dots", "Term.With.Dots"),  # Preserve periods
        ("Term_With_Underscores", "TermWithUnderscores"),
        ("123 Numeric Term", "123-Numeric-Term"),
        ("@#$Special At Start", "Special-At-Start"),
        ("-Leading-Trailing-", "Leading-Trailing"),
        ("Multiple...Periods", "Multiple.Periods"),  # Test multiple periods
        ("Mixed-Hyphens.Periods", "Mixed-Hyphens.Periods"),  # Test mixed separators
    ]

    for input_str, expected in test_cases:
        result = clean_url(input_str)
        assert result == expected, (
            f"Expected '{expected}' for input '{input_str}', got '{result}'"
        )


def test_clean_url_edge_cases():
    """Test clean_url function with edge cases"""
    test_cases = [
        ("", ""),  # Empty string
        (" ", ""),  # Single space
        ("   ", ""),  # Multiple spaces
        ("@#$%", ""),  # Only special characters
        ("A", "A"),  # Single character
        ("A B", "A-B"),  # Two characters with space
        ("A.B", "A.B"),  # Period separator
        ("...", ""),  # Only periods
        (".Leading.Trailing.", "Leading.Trailing"),  # Leading/trailing periods
    ]

    for input_str, expected in test_cases:
        result = clean_url(input_str)
        assert result == expected, (
            f"Expected '{expected}' for input '{input_str}', got '{result}'"
        )


def test_create_id_url_cleaning():
    """Test create_id function's URL cleaning behavior"""
    # Test basic URL cleaning
    id_ = create_id(["pii", "secure % password"], None, False)
    assert id_ == "pii.secure-password"

    # Test with multiple path components
    id_ = create_id(["Term One", "Term Two", "Term Three"], None, False)
    assert id_ == "Term-One.Term-Two.Term-Three"

    # Test with path components containing periods
    id_ = create_id(["Term.One", "Term.Two"], None, False)
    assert id_ == "Term.One.Term.Two"


def test_create_id_with_special_chars():
    """Test create_id function's handling of special characters"""
    # Test with non-ASCII characters (should trigger auto_id)
    id_ = create_id(["pii", "secure パスワード"], None, False)
    assert len(id_) == 32  # GUID length
    assert id_.isalnum()  # Should only contain alphanumeric characters

    # Test with characters that aren't periods or hyphens
    id_ = create_id(["test", "special@#$chars"], None, False)
    assert id_ == "test.specialchars"


def test_create_id_with_default():
    """Test create_id function with default_id parameter"""
    # Test that default_id is respected
    id_ = create_id(["any", "path"], "custom-id", False)
    assert id_ == "custom-id"

    # Test with URN as default_id
    id_ = create_id(["any", "path"], "urn:li:glossaryTerm:custom-id", False)
    assert id_ == "urn:li:glossaryTerm:custom-id"


def test_glossary_node_tags():
    """Test that tags are properly handled for glossary nodes"""
    # Create a glossary node with tags
    node = GlossaryNodeConfig(
        name="Test Node",
        description="Test node description",
        tags=["Tag1", "Tag2"],
    )
    node._urn = "urn:li:glossaryNode:test-node"

    # Mock objects required for get_mces_from_node
    path_vs_id: Dict[str, str] = {}
    parent_owners = models.OwnershipClass(owners=[])
    defaults = DefaultConfig(
        source="test-source",
        owners=Owners(users=["testuser"]),
        url="http://example.com",
        source_type="INTERNAL",
    )
    ingestion_config = BusinessGlossarySourceConfig(
        file="test.yml", enable_auto_id=False
    )

    # Create a mock PipelineContext
    mock_ctx = MagicMock(spec=PipelineContext)
    mock_ctx.graph = None

    # Get MCEs from the node
    mces = list(
        get_mces_from_node(
            node, path_vs_id, None, parent_owners, defaults, ingestion_config, mock_ctx
        )
    )

    # Ensure we have at least 2 MCEs (snapshot and tags MCP)
    assert len(mces) >= 2

    # Find the GlobalTags MCP
    tags_mcp = next(
        (
            mce
            for mce in mces
            if hasattr(mce, "aspect") and isinstance(mce.aspect, models.GlobalTagsClass)
        ),
        None,
    )

    # Verify the tags MCP exists and contains the expected tags
    assert tags_mcp is not None
    assert isinstance(tags_mcp.aspect, models.GlobalTagsClass)
    assert len(tags_mcp.aspect.tags) == 2
    tag_values = [tag.tag.split(":")[-1] for tag in tags_mcp.aspect.tags]
    assert "Tag1" in tag_values
    assert "Tag2" in tag_values


def test_glossary_term_tags():
    """Test that tags are properly handled for glossary terms"""
    # Create a glossary term with tags
    term = GlossaryTermConfig(
        name="Test Term",
        description="Test term description",
        tags=["Tag1", "Tag2"],
    )
    term._urn = "urn:li:glossaryTerm:test-term"

    # Mock objects required for get_mces_from_term
    path_vs_id: Dict[str, str] = {}
    parent_ownership = models.OwnershipClass(owners=[])
    defaults = DefaultConfig(
        source="test-source",
        owners=Owners(users=["testuser"]),
        url="http://example.com",
        source_type="INTERNAL",
    )
    ingestion_config = BusinessGlossarySourceConfig(
        file="test.yml", enable_auto_id=False
    )

    # Create a mock PipelineContext
    mock_ctx = MagicMock(spec=PipelineContext)
    mock_ctx.graph = None

    # Get MCEs from the term
    mces = list(
        get_mces_from_term(
            term,
            path_vs_id,
            None,
            parent_ownership,
            defaults,
            ingestion_config,
            mock_ctx,
        )
    )

    # Ensure we have at least 2 MCEs (snapshot and tags MCP)
    assert len(mces) >= 2

    # Find the GlobalTags MCP
    tags_mcp = next(
        (
            mce
            for mce in mces
            if hasattr(mce, "aspect") and isinstance(mce.aspect, models.GlobalTagsClass)
        ),
        None,
    )

    # Verify the tags MCP exists and contains the expected tags
    assert tags_mcp is not None
    assert isinstance(tags_mcp.aspect, models.GlobalTagsClass)
    assert len(tags_mcp.aspect.tags) == 2
    tag_values = [tag.tag.split(":")[-1] for tag in tags_mcp.aspect.tags]
    assert "Tag1" in tag_values
    assert "Tag2" in tag_values
