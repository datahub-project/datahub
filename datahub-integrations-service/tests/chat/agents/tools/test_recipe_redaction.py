"""Tests for recipe redaction utilities."""

from typing import cast

from datahub_integrations.chat.agents.tools.recipe_redaction import (
    redact_recipe,
    redact_recipe_in_dict,
)


class TestRedactRecipe:
    """Tests for redact_recipe function."""

    def test_redacts_password_field(self) -> None:
        """Test that password field is redacted."""
        recipe = """
source:
  type: snowflake
  config:
    account_id: abc123
    password: my-secret-password
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "my-secret-password" not in result
        assert "********" in result
        assert "account_id" in result
        assert "abc123" in result

    def test_redacts_token_field(self) -> None:
        """Test that token field is redacted."""
        recipe = """
source:
  type: bigquery
  config:
    project_id: my-project
    token: secret-token-value
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "secret-token-value" not in result
        assert "********" in result

    def test_redacts_api_key_field(self) -> None:
        """Test that API key field is redacted."""
        recipe = """
source:
  type: looker
  config:
    base_url: https://looker.example.com
    client_secret: super-secret-key
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "super-secret-key" not in result
        assert "********" in result

    def test_redacts_fields_with_password_suffix(self) -> None:
        """Test that fields ending in _password are redacted."""
        recipe = """
source:
  type: snowflake
  config:
    account_id: abc123
    private_key_password: my-private-key-pass
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "my-private-key-pass" not in result
        assert "********" in result

    def test_redacts_fields_with_token_suffix(self) -> None:
        """Test that fields ending in _token are redacted."""
        recipe = """
source:
  type: github
  config:
    access_token: ghp_1234567890abcdef
    repo: my-org/my-repo
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "ghp_1234567890abcdef" not in result
        assert "********" in result
        assert "my-org/my-repo" in result

    def test_redacts_fields_with_key_suffix(self) -> None:
        """Test that fields ending in _key are redacted."""
        recipe = """
source:
  type: s3
  config:
    aws_access_key_id: AKIAIOSFODNN7EXAMPLE
    aws_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    bucket: my-bucket
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "AKIAIOSFODNN7EXAMPLE" not in result
        assert "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" not in result
        assert "my-bucket" in result

    def test_preserves_environment_variables(self) -> None:
        """Test that environment variable references are preserved."""
        recipe = """
source:
  type: snowflake
  config:
    account_id: abc123
    password: ${SNOWFLAKE_PASSWORD}
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "${SNOWFLAKE_PASSWORD}" in result
        assert "abc123" in result

    def test_redacts_nested_fields(self) -> None:
        """Test that nested secret fields are redacted."""
        recipe = """
source:
  type: bigquery
  config:
    project_id: my-project
    credentials:
      private_key: -----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBg...
      client_email: service@project.iam.gserviceaccount.com
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "-----BEGIN PRIVATE KEY-----" not in result
        assert "service@project.iam.gserviceaccount.com" in result

    def test_redacts_options_field(self) -> None:
        """Test that options field is redacted (common for connection strings)."""
        recipe = """
source:
  type: mysql
  config:
    host: localhost
    options:
      ssl_cert: /path/to/cert
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "/path/to/cert" not in result
        assert "localhost" in result

    def test_redacts_sqlalchemy_uri(self) -> None:
        """Test that sqlalchemy_uri is redacted."""
        recipe = """
source:
  type: sql
  config:
    sqlalchemy_uri: postgresql://user:password@host:5432/database
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "postgresql://user:password@host:5432/database" not in result
        assert "********" in result

    def test_handles_json_format(self) -> None:
        """Test that JSON format recipes work."""
        recipe = (
            '{"source": {"type": "snowflake", "config": {"password": "secret123"}}}'
        )
        result = redact_recipe(recipe)
        assert result is not None
        assert "secret123" not in result
        assert "********" in result

    def test_handles_none_input(self) -> None:
        """Test that None input returns None."""
        result = redact_recipe(None)
        assert result is None

    def test_handles_empty_string(self) -> None:
        """Test that empty string returns empty string."""
        result = redact_recipe("")
        assert result == ""

    def test_handles_whitespace_only(self) -> None:
        """Test that whitespace-only string is preserved."""
        result = redact_recipe("   \n  ")
        assert result == "   \n  "

    def test_handles_invalid_yaml(self) -> None:
        """Test that invalid YAML is handled gracefully."""
        recipe = "this is not valid yaml: {[bad syntax"
        result = redact_recipe(recipe)
        assert result is not None
        assert "Unable to parse recipe" in result

    def test_handles_null_yaml(self) -> None:
        """Test that YAML null value is handled."""
        recipe = "null"
        result = redact_recipe(recipe)
        assert result == "null"

    def test_redacts_multiple_secret_fields(self) -> None:
        """Test that multiple secret fields are all redacted."""
        recipe = """
source:
  type: snowflake
  config:
    account_id: abc123
    password: password123
    private_key_password: keypass456
    oauth_token: token789
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "password123" not in result
        assert "keypass456" not in result
        assert "token789" not in result
        assert result.count("********") >= 3

    def test_preserves_empty_password(self) -> None:
        """Test that empty/null password values are preserved."""
        recipe = """
source:
  type: snowflake
  config:
    account_id: abc123
    password: null
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "null" in result or "None" in result

    def test_preserves_boolean_values(self) -> None:
        """Test that boolean values are preserved."""
        recipe = """
source:
  type: snowflake
  config:
    account_id: abc123
    use_ssl: true
    password_enabled: false
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "true" in result or "True" in result
        assert "false" in result or "False" in result

    def test_redacts_secret_in_list(self) -> None:
        """Test that secrets in lists are redacted."""
        recipe = """
source:
  type: multi
  config:
    sources:
      - type: snowflake
        config:
          password: secret1
      - type: bigquery
        config:
          token: secret2
"""
        result = redact_recipe(recipe)
        assert result is not None
        assert "secret1" not in result
        assert "secret2" not in result


class TestRedactRecipeInDict:
    """Tests for redact_recipe_in_dict function."""

    def test_redacts_recipe_at_simple_path(self) -> None:
        """Test redacting recipe at a simple path."""
        data = {"config": {"recipe": "source:\n  config:\n    password: secret123\n"}}
        redact_recipe_in_dict(data, ["config", "recipe"])
        assert "secret123" not in str(data)
        assert "********" in data["config"]["recipe"]

    def test_redacts_recipe_at_nested_path(self) -> None:
        """Test redacting recipe at a nested path."""
        data = {
            "source": {"config": {"recipe": "source:\n  config:\n    token: abc123\n"}}
        }
        redact_recipe_in_dict(data, ["source", "config", "recipe"])
        assert "abc123" not in str(data)
        assert "********" in data["source"]["config"]["recipe"]

    def test_handles_missing_path(self) -> None:
        """Test that missing path is handled gracefully."""
        data = {"config": {"other_field": "value"}}
        redact_recipe_in_dict(data, ["config", "recipe"])
        assert data == {"config": {"other_field": "value"}}

    def test_handles_non_dict_in_path(self) -> None:
        """Test that non-dict values in path are handled gracefully."""
        data = {"config": "not a dict"}
        redact_recipe_in_dict(data, ["config", "recipe"])
        assert data == {"config": "not a dict"}

    def test_handles_empty_path(self) -> None:
        """Test that empty path is handled gracefully."""
        data = {"config": {"recipe": "source:\n  config:\n    password: secret\n"}}
        original_data = data.copy()
        redact_recipe_in_dict(data, [])
        assert data == original_data

    def test_modifies_dict_in_place(self) -> None:
        """Test that the function modifies the dict in place."""
        data = {"config": {"recipe": "source:\n  config:\n    password: secret123\n"}}
        redact_recipe_in_dict(data, ["config", "recipe"])
        assert "secret123" not in data["config"]["recipe"]

    def test_handles_none_recipe(self) -> None:
        """Test that None recipe value is handled."""
        data = {"config": {"recipe": None}}
        redact_recipe_in_dict(data, ["config", "recipe"])
        assert data["config"]["recipe"] is None

    def test_real_graphql_response_structure(self) -> None:
        """Test with structure similar to actual GraphQL response."""
        data: dict[str, object] = {
            "urn": "urn:li:dataHubIngestionSource:test",
            "name": "Test Source",
            "config": {
                "recipe": """
source:
  type: snowflake
  config:
    account_id: abc123
    password: my-secret-password
    warehouse: COMPUTE_WH
""",
                "version": "1.0",
                "executorId": "default",
            },
        }
        redact_recipe_in_dict(data, ["config", "recipe"])
        assert "my-secret-password" not in str(data)
        config = cast(dict[str, object], data["config"])
        redacted_recipe = cast(str, config["recipe"])
        assert "abc123" in redacted_recipe
        assert "COMPUTE_WH" in redacted_recipe
        assert "********" in redacted_recipe
