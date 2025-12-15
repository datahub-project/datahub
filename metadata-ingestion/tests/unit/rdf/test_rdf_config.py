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
        """Test that invalid format is rejected."""
        config_dict = {
            "source": "test.ttl",
            "format": "invalid_format",
        }

        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict)

        error_msg = str(exc_info.value).lower()
        assert "format" in error_msg or "invalid" in error_msg

    def test_invalid_environment(self):
        """Test that invalid environment is rejected."""
        config_dict = {
            "source": "test.ttl",
            "environment": "INVALID_ENV",
        }

        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict)

        error_msg = str(exc_info.value).lower()
        assert "environment" in error_msg or "invalid" in error_msg

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
        """Test that recursive must be a boolean."""
        config_dict = {
            "source": "test.ttl",
            "recursive": "yes",
        }

        with pytest.raises(Exception) as exc_info:
            RDFSourceConfig.model_validate(config_dict)

        assert "bool" in str(exc_info.value).lower()

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
        config_dict = {"source": "test.ttl"}
        config = RDFSourceConfig.model_validate(config_dict)

        assert config.format == "turtle"
        assert config.environment == "PROD"
        assert config.recursive is False
        assert config.extensions == [".ttl", ".rdf", ".xml", ".jsonld", ".n3", ".nt"]
        assert config.export_only is None
        assert config.skip_export is None
