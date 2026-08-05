import json
import os
import uuid
from unittest.mock import patch

import pytest

from datahub.executor.execution import venv_utils
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

    # Note: The hash may be different due to the new VenvResolver implementation
    # This test verifies the venv name format is still consistent
    venv_name = args.get_venv_name(plugin="demo-data")
    assert venv_name.startswith("demo-data-")
    # For plugin names with hyphens, we need to get the last part after splitting
    hash_part = venv_name[len("demo-data-") :]  # Get everything after "demo-data-"
    assert len(hash_part) == 16  # Hash length should still be 16

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

    # Bundled version should ignore extra requirements/plugins in venv name
    venv_name_1 = args.get_venv_name(plugin="demo-data")

    args_with_extras = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "bundled",
            "extra_pip_plugins": json.dumps(["bigquery"]),
            "extra_pip_requirements": json.dumps(["sqlparse==0.4.3"]),
        }
    )

    venv_name_2 = args_with_extras.get_venv_name(plugin="demo-data")

    # Should be the same venv name despite different extra requirements
    assert venv_name_1 == venv_name_2

    # Should use simple naming for bundled version
    assert venv_name_1 == "demo-data-bundled"


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


def test_venv_path_resolution():
    """Test venv path resolution using venv utilities."""
    # Test dynamic venv path (regular version)
    dynamic_venv_name = venv_utils.get_venv_name("demo-data", "v0.12.1")
    dynamic_path = venv_utils.get_venv_path(dynamic_venv_name, "/tmp/test")
    assert dynamic_path == f"/tmp/test/venv-{dynamic_venv_name}"

    # Test bundled venv path
    bundled_venv_name = venv_utils.get_venv_name("demo-data", "bundled")
    bundled_path = venv_utils.get_venv_path(bundled_venv_name, "/tmp/test")
    assert bundled_path == f"/opt/datahub/venvs/{bundled_venv_name}"


def test_venv_name_deterministic():
    """Test that venv names are deterministic for the same inputs."""
    recipe = json.dumps(
        {
            "source": {"type": "demo-data", "config": {}},
            "pipeline_name": "test-pipeline",
        }
    )

    # Create multiple instances with same configuration
    args1 = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "v0.12.1",
            "extra_pip_requirements": json.dumps(["pkg1", "pkg2"]),
        }
    )

    args2 = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "v0.12.1",
            "extra_pip_requirements": json.dumps(["pkg1", "pkg2"]),
        }
    )

    # Should generate the same venv name
    assert args1.get_venv_name("demo-data") == args2.get_venv_name("demo-data")

    # Different configurations should generate different names
    args3 = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "v0.12.1",
            "extra_pip_requirements": json.dumps(["pkg1", "pkg3"]),  # Different package
        }
    )

    assert args1.get_venv_name("demo-data") != args3.get_venv_name("demo-data")


def test_venv_name_generation_with_different_configs():
    """Test that different configs result in different venv names and same configs result in same name."""
    recipe = json.dumps(
        {
            "source": {"type": "demo-data", "config": {}},
            "pipeline_name": "test-pipeline",
        }
    )

    # Two identical configurations should generate the same venv name
    args1 = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "v0.12.1",
            "extra_pip_requirements": json.dumps(["pkg1", "pkg2"]),
        }
    )

    args2 = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "v0.12.1",
            "extra_pip_requirements": json.dumps(["pkg1", "pkg2"]),
        }
    )

    # Should generate the same venv name
    assert args1.get_venv_name("demo-data") == args2.get_venv_name("demo-data")

    # Different configurations should generate different names
    args3 = SubProcessIngestionTaskArgs.model_validate(
        {
            "recipe": recipe,
            "version": "v0.12.1",
            "extra_pip_requirements": json.dumps(["pkg1", "pkg3"]),  # Different package
        }
    )

    assert args1.get_venv_name("demo-data") != args3.get_venv_name("demo-data")
