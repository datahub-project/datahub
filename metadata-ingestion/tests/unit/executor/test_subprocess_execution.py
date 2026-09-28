import json
import os
import uuid
from unittest.mock import patch

import pytest

from datahub.executor.execution.sub_process_ingestion_task import (
    SubProcessIngestionTaskArgs,
)


def test_parse_args():
    exec_id = str(uuid.uuid4())
    exec_urn = f"urn:li:dataHubExecutionRequest:{exec_id}"
    ingestion_source = (
        "urn:li:dataHubIngestionSource:96980632-7eb2-4185-8923-e4dab4f5f153"
    )
    recipe = json.dumps(
        {
            "run_id": exec_urn,
            "source": {"type": "demo-data", "config": {}},
            "pipeline_name": ingestion_source,
        },
        separators=(",", ":"),
    )

    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
        }
    )
    assert args.recipe == recipe
    assert args.version == "latest"

    # Test snake_case (debug_mode) with lowercase "true"
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "https://datahub-docs.vercel.app/",
            "debug_mode": "true",
        }
    )
    assert args.version == "https://datahub-docs.vercel.app/"
    assert args.debug_mode == "true"

    # Test camelCase (debugMode) with lowercase "true" - as sent by the UI
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "https://datahub-docs.vercel.app/",
            "debugMode": "true",
        }
    )
    assert args.version == "https://datahub-docs.vercel.app/"
    assert args.debug_mode == "true"  # Should be accessible via debug_mode field

    # Test camelCase with uppercase "True" (should be normalized to lowercase)
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "debugMode": "True",
        }
    )
    assert args.debug_mode == "true"  # Normalized to lowercase

    # Test camelCase with uppercase "False" (should be normalized to lowercase)
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "debugMode": "False",
        }
    )
    assert args.debug_mode == "false"  # Normalized to lowercase

    # Test snake_case with uppercase "TRUE" (should be normalized)
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "debug_mode": "TRUE",
        }
    )
    assert args.debug_mode == "true"  # Normalized to lowercase

    # Test mixed case like "TrUe" (should be normalized)
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "debugMode": "TrUe",
        }
    )
    assert args.debug_mode == "true"  # Normalized to lowercase

    # Test with lowercase "false" explicitly set via snake_case
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "debug_mode": "false",
        }
    )
    assert args.debug_mode == "false"

    # Test when debug_mode is not provided at all (should default to "false")
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
        }
    )
    assert args.debug_mode == "false"  # Default value

    # Test when debugMode is not provided (should also default to "false")
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "latest",
        }
    )
    assert args.debug_mode == "false"  # Default value

    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "https://datahub-docs.vercel.app/",
            "extra_pip_plugins": json.dumps(["bigquery"]),
            "extra_pip_requirements": json.dumps(["sqlparse==0.4.3"]),
            "extra_env_vars": json.dumps({"MY_CUSTOM_ENV": "my_custom_value2"}),
        }
    )
    assert args.version == "https://datahub-docs.vercel.app/"
    assert args.extra_pip_plugins == ["bigquery"]
    assert args.extra_pip_requirements == ["sqlparse==0.4.3"]
    assert args.extra_env_vars == {"MY_CUSTOM_ENV": "my_custom_value2"}
    assert args.debug_mode != "true"

    with pytest.raises(ValueError):
        SubProcessIngestionTaskArgs.model_validate(
            {
                "version": "https://datahub-docs.vercel.app/",
                "debug_mode": "true",
            }
        )


def test_bundled_version_behavior():
    """Test the behavior with bundled version."""
    exec_id = str(uuid.uuid4())
    exec_urn = f"urn:li:dataHubExecutionRequest:{exec_id}"
    ingestion_source = (
        "urn:li:dataHubIngestionSource:96980632-7eb2-4185-8923-e4dab4f5f153"
    )
    recipe = json.dumps(
        {
            "run_id": exec_urn,
            "source": {"type": "demo-data", "config": {}},
            "pipeline_name": ingestion_source,
        },
        separators=(",", ":"),
    )

    # Test with bundled version
    args = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "bundled",
        }
    )

    assert args.version == "bundled"
    assert args.should_use_bundled_venv()


def test_dependency_resolution_disabled():
    """Test behavior when dependency resolution is globally disabled."""
    exec_id = str(uuid.uuid4())
    exec_urn = f"urn:li:dataHubExecutionRequest:{exec_id}"
    ingestion_source = (
        "urn:li:dataHubIngestionSource:96980632-7eb2-4185-8923-e4dab4f5f153"
    )
    recipe = json.dumps(
        {
            "run_id": exec_urn,
            "source": {"type": "demo-data", "config": {}},
            "pipeline_name": ingestion_source,
        },
        separators=(",", ":"),
    )

    with patch.dict(os.environ, {"INGESTION_DEPENDENCY_RESOLUTION_ENABLED": "false"}):
        # Should work with bundled version
        args_bundled = SubProcessIngestionTaskArgs.model_validate(
            {
                "recipe": recipe,
                "version": "bundled",
            }
        )

        assert args_bundled.should_use_bundled_venv()

        # Non-bundled versions should NOT automatically raise in should_use_bundled_venv()
        # They should work for backwards compatibility, but validation can be done explicitly
        args_latest = SubProcessIngestionTaskArgs.model_validate(
            {
                "recipe": recipe,
                "version": "latest",
            }
        )

        # should_use_bundled_venv returns False for non-bundled versions (backwards compatible)
        assert not args_latest.should_use_bundled_venv()
