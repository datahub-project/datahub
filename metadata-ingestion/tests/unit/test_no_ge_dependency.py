"""Verify DataHub modules can be imported in an environment without great_expectations."""

import subprocess
import sys
import textwrap

import pytest

_MODULES_TO_TEST = [
    "datahub.ingestion.source.sql.sql_generic_profiler",
    "datahub.ingestion.source.snowflake.snowflake_profiler",
    "datahub.ingestion.source.bigquery_v2.profiler",
]


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


def _assert_module_imports_without_great_expectations(module_path: str) -> None:
    """Spawn a subprocess that blocks great_expectations imports and asserts module_path loads.

    Used by both the profiler-module test and the source-module test.
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


@pytest.mark.parametrize("module_path", _MODULES_TO_TEST)
def test_profiler_module_imports_without_great_expectations(module_path: str) -> None:
    """Importing a profiler module must not require great_expectations to be installed."""
    _assert_module_imports_without_great_expectations(module_path)


@pytest.mark.parametrize("module_path", _SOURCE_MODULES)
def test_source_module_imports_without_great_expectations(module_path: str) -> None:
    """End-to-end check: a SQL source module must import in a GE-less environment."""
    _assert_module_imports_without_great_expectations(module_path)


def test_ge_method_raises_helpful_error_when_ge_missing() -> None:
    """When profiling.method=ge but great_expectations is not installed, a clear ConfigurationError must point at the fix."""
    script = textwrap.dedent(
        """
        import sys


        class GreatExpectationsBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "great_expectations" or name.startswith("great_expectations."):
                    raise ImportError(f"GE blocked by test: {name}")
                return None


        sys.meta_path.insert(0, GreatExpectationsBlocker())

        # Replicates the lazy-import-and-raise flow used by sql_generic_profiler.get_profiler_instance()
        # when profiling.method=ge. Confirms ConfigurationError carries the expected guidance.
        from datahub.configuration.common import ConfigurationError

        try:
            from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler  # noqa: F401
        except ImportError as e:
            from datahub.ingestion.source.profiling.common import (
                GE_PROFILER_MISSING_MESSAGE,
            )
            try:
                raise ConfigurationError(GE_PROFILER_MISSING_MESSAGE) from e
            except ConfigurationError as ce:
                print(f"CAUGHT:{ce}")
                sys.exit(0)

        sys.exit(2)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"Expected the lazy GE import to fail and ConfigurationError to be raised.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "CAUGHT:" in result.stdout, (
        f"ConfigurationError not raised. stdout: {result.stdout}"
    )
    assert "acryl-datahub[profiling-ge]" in result.stdout, (
        f"Error message missing install hint. stdout: {result.stdout}"
    )
    assert "profiling.method: sqlalchemy" in result.stdout, (
        f"Error message missing method-switch hint. stdout: {result.stdout}"
    )


def test_sql_common_lazy_imports_are_ge_free() -> None:
    """sql_common.py's lazy imports inside SQLAlchemySource.loop_profiler_requests
    and SQLAlchemySource.get_profiler_instance must not require great_expectations
    when profiling.method=sqlalchemy.

    Regression test for a bug discovered in the final review where these
    imports pulled GE via aliased re-exports from ge_data_profiler.py.
    """
    script = textwrap.dedent(
        """
        import sys


        class GreatExpectationsBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "great_expectations" or name.startswith("great_expectations."):
                    raise ImportError(f"GE blocked by test: {name}")
                return None


        sys.meta_path.insert(0, GreatExpectationsBlocker())

        # Simulate the exact lazy import that loop_profiler_requests does (post-fix).
        from datahub.ingestion.source.profiling.common import (
            ProfilerRequest as GEProfilerRequest,
        )
        assert GEProfilerRequest.__name__ == "ProfilerRequest"

        # Simulate the exact lazy import that get_profiler_instance does (post-fix).
        try:
            from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler  # noqa: F401
        except ImportError:
            pass  # Expected when GE is blocked - that's the whole point.
        else:
            print("FAIL: DatahubGEProfiler unexpectedly imported")
            sys.exit(1)

        print("IMPORT_OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0 and "IMPORT_OK" in result.stdout, (
        f"sql_common lazy-import path requires great_expectations.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
