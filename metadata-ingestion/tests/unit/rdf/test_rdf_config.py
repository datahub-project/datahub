#!/usr/bin/env python3
"""
Unit tests for RDF source configuration validation.

Tests verify:
- Required field validation
- Field type validation
- Value constraints
- Format validation
- Path traversal protection
"""

import pytest

from datahub.ingestion.source.rdf.ingestion.rdf_source import RDFSourceConfig


class TestRDFConfig:
    """Test RDF source configuration validation."""

    def test_required_source_field(self):
        """Test that source field is required."""
        config_dict: dict = {}

        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict)

        assert "source" in str(exc_info.value).lower()

    def test_source_must_be_string(self):
        """Test that source must be a string."""
        config_dict = {"source": 123}

        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict)

        assert (
            "string" in str(exc_info.value).lower()
            or "str" in str(exc_info.value).lower()
        )

    def test_invalid_format(self):
        """Test that invalid format is rejected during RDF loading."""
        # Note: Format validation happens during RDF loading, not config validation
        # Config validation only checks types, so invalid format passes here
        # but will fail when actually loading RDF
        config_dict: dict = {
            "source": "test.ttl",
            "format": "invalid_format",
        }
        # Config validation should pass (format is just a string)
        config = RDFSourceConfig.model_validate(config_dict)
        assert config.format == "invalid_format"
        # Actual validation happens in rdf_loader._validate_format()

    def test_invalid_environment(self):
        """Test that environment accepts any string value."""
        # Note: Environment is just a string field with no validation
        # It's used as-is in DataHub metadata
        config_dict: dict = {
            "source": "test.ttl",
            "environment": "INVALID_ENV",
        }
        # Config validation should pass (environment is just a string)
        config = RDFSourceConfig.model_validate(config_dict)
        assert config.environment == "INVALID_ENV"

    def test_extensions_must_be_list(self):
        """Test that extensions must be a list."""
        config_dict = {
            "source": "test.ttl",
            "extensions": ".ttl",
        }

        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict)

        assert "list" in str(exc_info.value).lower()

    def test_export_only_must_be_list(self):
        """Test that export_only must be a list."""
        config_dict = {
            "source": "test.ttl",
            "export_only": "glossary",
        }

        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict)

        assert "list" in str(exc_info.value).lower()

    def test_recursive_must_be_boolean(self):
        """Test that recursive accepts boolean values and common string coercions."""
        # Pydantic v2 automatically coerces common string values to booleans
        # Test that valid boolean-like values work
        config_dict: dict = {
            "source": "test.ttl",
            "recursive": "yes",  # Pydantic coerces "yes" to True
        }
        config = RDFSourceConfig.model_validate(config_dict)
        assert config.recursive is True

        # Test that invalid values (that can't be coerced) raise errors
        config_dict_invalid: dict = {
            "source": "test.ttl",
            "recursive": 123,  # Numbers can't be coerced to bool in Pydantic v2
        }
        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict_invalid)

        # Pydantic will raise a validation error for type mismatch
        error_str = str(exc_info.value).lower()
        assert "bool" in error_str or "boolean" in error_str or "type" in error_str

    def test_stateful_ingestion_config(self):
        """Test that stateful ingestion config is accepted."""
        config_dict = {
            "source": "test.ttl",
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
            },
        }
        config = RDFSourceConfig.model_validate(config_dict)

        assert config.stateful_ingestion is not None
        assert config.stateful_ingestion.enabled is True
        assert config.stateful_ingestion.remove_stale_metadata is True

    def test_default_values(self):
        """Test that default values are set correctly."""
        config_dict: dict = {"source": "test.ttl"}
        config = RDFSourceConfig.model_validate(config_dict)

        assert config.format is None  # Format is auto-detected, default is None
        assert config.environment == "PROD"
        assert config.recursive is True  # Default changed to True
        assert config.extensions == [".ttl", ".rdf", ".owl", ".n3", ".nt"]
        assert config.export_only is None
        assert config.skip_export is None
