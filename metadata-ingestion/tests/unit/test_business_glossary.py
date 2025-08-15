from datahub.ingestion.source.metadata.business_glossary import clean_url, create_id


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
