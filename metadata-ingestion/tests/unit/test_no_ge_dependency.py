"""Verify profiler modules can be imported in an environment without great_expectations."""

import subprocess
import sys
import textwrap

import pytest

_MODULES_TO_TEST = [
    "datahub.ingestion.source.sql.sql_generic_profiler",
    "datahub.ingestion.source.snowflake.snowflake_profiler",
    "datahub.ingestion.source.bigquery_v2.profiler",
]


@pytest.mark.parametrize("module_path", _MODULES_TO_TEST)
def test_profiler_module_imports_without_great_expectations(module_path: str) -> None:
    """Importing the module must not require great_expectations to be installed.

    Runs the import in a subprocess with a meta_path finder that blocks any
    great_expectations.* import, simulating an environment where the user
    installed acryl-datahub[snowflake] but not acryl-datahub[profiling-ge].
    """
    script = textwrap.dedent(
        f"""
        import sys


        class GreatExpectationsBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "great_expectations" or name.startswith("great_expectations."):
                    raise ImportError(f"GE blocked by test: {{name}}")
                return None


        sys.meta_path.insert(0, GreatExpectationsBlocker())

        import {module_path}  # noqa: F401

        print("IMPORT_OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0 and "IMPORT_OK" in result.stdout, (
        f"Module {module_path} requires great_expectations at import time.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )


_SOURCE_MODULES = [
    "datahub.ingestion.source.snowflake.snowflake_v2",
    "datahub.ingestion.source.bigquery_v2.bigquery",
    "datahub.ingestion.source.redshift.redshift",
    "datahub.ingestion.source.sql.postgres",
    "datahub.ingestion.source.sql.mysql",
    "datahub.ingestion.source.sql.mssql",
    "datahub.ingestion.source.sql.athena",
    "datahub.ingestion.source.unity.source",
]


@pytest.mark.parametrize("module_path", _SOURCE_MODULES)
def test_source_module_imports_without_great_expectations(module_path: str) -> None:
    """End-to-end check: an SQL source module must import in a GE-less environment."""
    script = textwrap.dedent(
        f"""
        import sys


        class GreatExpectationsBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "great_expectations" or name.startswith("great_expectations."):
                    raise ImportError(f"GE blocked by test: {{name}}")
                return None


        sys.meta_path.insert(0, GreatExpectationsBlocker())

        import {module_path}  # noqa: F401

        print("IMPORT_OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0 and "IMPORT_OK" in result.stdout, (
        f"Source {module_path} requires great_expectations at import time.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
