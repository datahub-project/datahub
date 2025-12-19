from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery


class TestFivetranLogQuery:
    """Unit tests for FivetranLogQuery class."""

    def test_is_valid_unquoted_identifier_valid_cases(self):
        """Test _is_valid_unquoted_identifier with valid identifiers."""
        valid_identifiers = [
            "test_database",  # lowercase with underscore
            "TEST_DATABASE",  # uppercase with underscore
            "my_schema",  # lowercase
            "schema_123",  # lowercase with numbers
            "SCHEMA_123",  # uppercase with numbers
            "a",  # single letter
            "_private",  # starts with underscore
            "_123",  # starts with underscore, has numbers
            "table_name_123",  # mixed case with underscore and numbers
            "A1B2C3",  # alphanumeric
        ]

        for identifier in valid_identifiers:
            assert FivetranLogQuery._is_valid_unquoted_identifier(identifier), (
                f"Expected {identifier!r} to be a valid unquoted identifier"
            )

    def test_is_valid_unquoted_identifier_invalid_empty(self):
        """Test _is_valid_unquoted_identifier with empty string."""
        assert not FivetranLogQuery._is_valid_unquoted_identifier("")

    def test_is_valid_unquoted_identifier_invalid_already_quoted(self):
        """Test _is_valid_unquoted_identifier with already quoted identifiers."""
        invalid_quoted = [
            '"test_database"',  # fully quoted
            '"test"',  # single word quoted
            '""',  # empty quoted
            '"test_database"extra',  # quoted with extra text
            'extra"test_database"',  # extra text before quoted
        ]

        for identifier in invalid_quoted:
            assert not FivetranLogQuery._is_valid_unquoted_identifier(identifier), (
                f"Expected {identifier!r} to be invalid (already quoted)"
            )

    def test_is_valid_unquoted_identifier_invalid_special_characters(self):
        """Test _is_valid_unquoted_identifier with special characters."""
        invalid_special = [
            "test-database",  # hyphen
            "test.database",  # dot
            "test database",  # space
            "test'database",  # single quote
            'test"database',  # double quote (not fully quoted)
            "test`database",  # backtick
            "test@database",  # at symbol
            "test#database",  # hash
            "test$database",  # dollar
            "test%database",  # percent
            "test&database",  # ampersand
            "test*database",  # asterisk
            "test+database",  # plus
            "test=database",  # equals
            "test[database",  # bracket
            "test]database",  # bracket
            "test{database",  # brace
            "test}database",  # brace
            "test|database",  # pipe
            "test\\database",  # backslash
            "test:database",  # colon
            "test;database",  # semicolon
            "test<database",  # less than
            "test>database",  # greater than
            "test?database",  # question mark
            "test/database",  # slash
            "test,database",  # comma
        ]

        for identifier in invalid_special:
            assert not FivetranLogQuery._is_valid_unquoted_identifier(identifier), (
                f"Expected {identifier!r} to be invalid (contains special characters)"
            )

    def test_is_valid_unquoted_identifier_invalid_starts_with_number(self):
        """Test _is_valid_unquoted_identifier with identifiers starting with numbers."""
        invalid_starts_with_number = [
            "123database",  # starts with number
            "0test",  # starts with zero
            "9schema",  # starts with nine
        ]

        for identifier in invalid_starts_with_number:
            assert not FivetranLogQuery._is_valid_unquoted_identifier(identifier), (
                f"Expected {identifier!r} to be invalid (starts with number)"
            )

    def test_is_valid_unquoted_identifier_valid_with_numbers(self):
        """Test _is_valid_unquoted_identifier with valid identifiers containing numbers."""
        valid_with_numbers = [
            "test123",  # ends with numbers
            "test_123",  # underscore and numbers
            "a1b2c3",  # mixed letters and numbers
            "schema1",  # number at end
            "table_2024",  # underscore and year
        ]

        for identifier in valid_with_numbers:
            assert FivetranLogQuery._is_valid_unquoted_identifier(identifier), (
                f"Expected {identifier!r} to be valid (contains numbers but starts with letter/underscore)"
            )

    def test_is_valid_unquoted_identifier_valid_starts_with_underscore(self):
        """Test _is_valid_unquoted_identifier with identifiers starting with underscore."""
        valid_underscore_start = [
            "_test",  # underscore then letters
            "_123",  # underscore then numbers
            "__double",  # double underscore
            "_test_database",  # underscore, letters, underscore
        ]

        for identifier in valid_underscore_start:
            assert FivetranLogQuery._is_valid_unquoted_identifier(identifier), (
                f"Expected {identifier!r} to be valid (starts with underscore)"
            )

    def test_is_valid_unquoted_identifier_case_insensitive(self):
        """Test _is_valid_unquoted_identifier is case-insensitive for valid identifiers."""
        # All these should be valid regardless of case
        case_variants = [
            "test_database",
            "TEST_DATABASE",
            "Test_Database",
            "tEsT_dAtAbAsE",
        ]

        for identifier in case_variants:
            assert FivetranLogQuery._is_valid_unquoted_identifier(identifier), (
                f"Expected {identifier!r} to be valid (case should not matter)"
            )

    def test_is_valid_unquoted_identifier_edge_cases(self):
        """Test _is_valid_unquoted_identifier with edge cases."""
        # Single character valid cases
        assert FivetranLogQuery._is_valid_unquoted_identifier("a")
        assert FivetranLogQuery._is_valid_unquoted_identifier("A")
        assert FivetranLogQuery._is_valid_unquoted_identifier("_")

        # Single character invalid cases
        assert not FivetranLogQuery._is_valid_unquoted_identifier("1")
        assert not FivetranLogQuery._is_valid_unquoted_identifier("-")
        assert not FivetranLogQuery._is_valid_unquoted_identifier(".")

        # Very long valid identifier
        long_valid = "a" + "_" * 100 + "b"
        assert FivetranLogQuery._is_valid_unquoted_identifier(long_valid)

        # Unicode characters (should be invalid)
        assert not FivetranLogQuery._is_valid_unquoted_identifier("test_数据库")
        assert not FivetranLogQuery._is_valid_unquoted_identifier("test_ñ")
