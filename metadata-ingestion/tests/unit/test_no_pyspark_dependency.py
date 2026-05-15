"""Verify DataHub modules can be imported in an environment without pyspark."""

import subprocess
import sys
import textwrap

import pytest

_SOURCE_MODULES = [
    "datahub.ingestion.source.unity.source",
    "datahub.ingestion.source.s3.source",
    "datahub.ingestion.source.abs.source",
]


def _assert_module_imports_without_pyspark(module_path: str) -> None:
    script = textwrap.dedent(
        f"""
        import sys


        class PySparkBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError(f"pyspark blocked by test: {{name}}")
                return None


        sys.meta_path.insert(0, PySparkBlocker())

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
        f"Module {module_path} requires pyspark at import time.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )


@pytest.mark.parametrize("module_path", _SOURCE_MODULES)
def test_source_module_imports_without_pyspark(module_path: str) -> None:
    _assert_module_imports_without_pyspark(module_path)


def test_s3_profiling_raises_helpful_error_when_pyspark_missing() -> None:
    script = textwrap.dedent(
        """
        import sys


        class PySparkBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError(f"pyspark blocked by test: {name}")
                if name == "pydeequ" or name.startswith("pydeequ."):
                    raise ImportError(f"pydeequ blocked by test: {name}")
                return None


        sys.meta_path.insert(0, PySparkBlocker())

        from datahub.configuration.common import ConfigurationError
        from datahub.ingestion.source.s3.config import DataLakeSourceConfig
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        config = DataLakeSourceConfig.model_validate(
            {"path_specs": [{"include": "s3://bucket/{table}"}], "profiling": {"enabled": True}}
        )
        ctx = PipelineContext(run_id="test")

        try:
            S3Source(config, ctx)
        except ConfigurationError as e:
            print(f"CAUGHT:{e}")
            sys.exit(0)
        except Exception as e:
            print(f"WRONG_EXCEPTION:{type(e).__name__}:{e}")
            sys.exit(1)

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
        f"Expected ConfigurationError.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "CAUGHT:" in result.stdout
    assert "acryl-datahub[s3,pyspark]" in result.stdout


def test_abs_profiling_raises_helpful_error_when_pyspark_missing() -> None:
    script = textwrap.dedent(
        """
        import sys


        class PySparkBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError(f"pyspark blocked by test: {name}")
                if name == "pydeequ" or name.startswith("pydeequ."):
                    raise ImportError(f"pydeequ blocked by test: {name}")
                return None


        sys.meta_path.insert(0, PySparkBlocker())

        from datahub.configuration.common import ConfigurationError
        from datahub.ingestion.source.abs.config import DataLakeSourceConfig
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.abs.source import ABSSource

        config = DataLakeSourceConfig.model_validate(
            {"path_specs": [{"include": "abfss://container@account.dfs.core.windows.net/{table}"}], "profiling": {"enabled": True}}
        )
        ctx = PipelineContext(run_id="test")

        try:
            ABSSource(config, ctx)
        except ConfigurationError as e:
            print(f"CAUGHT:{e}")
            sys.exit(0)
        except Exception as e:
            print(f"WRONG_EXCEPTION:{type(e).__name__}:{e}")
            sys.exit(1)

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
        f"Expected ConfigurationError.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert "CAUGHT:" in result.stdout
    assert "acryl-datahub[abs,pyspark]" in result.stdout


def test_unity_usage_drops_to_dropped_metric_when_pyspark_missing() -> None:
    script = textwrap.dedent(
        """
        import sys


        class PySparkBlocker:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError(f"pyspark blocked by test: {name}")
                return None


        sys.meta_path.insert(0, PySparkBlocker())

        from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor

        result = UnityCatalogUsageExtractor._parse_query_via_spark_sql_plan.__doc__
        # Instantiating just to call the method directly — we only need the parser stub.
        import unittest.mock as mock

        extractor = mock.MagicMock(spec=UnityCatalogUsageExtractor)
        extractor._spark_sql_parser = None
        extractor._spark_sql_parser_init_attempted = False
        # Call the real property getter via the class
        parser = UnityCatalogUsageExtractor.spark_sql_parser.fget(extractor)
        assert parser is None, f"Expected None, got {parser!r}"
        assert extractor._spark_sql_parser_init_attempted is True

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
        f"Unity spark_sql_parser did not return None when pyspark missing.\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
