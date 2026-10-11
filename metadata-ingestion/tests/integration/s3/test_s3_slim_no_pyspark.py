"""
Integration test to validate s3/s3-slim installation works without PySpark.

S3 profiling no longer depends on PySpark/Deequ at all (see FileProfiler), so
neither the "slim" nor the "full" `s3` extra installs PySpark anymore. This
test ensures profiling works even when PySpark/Deequ are absent, and that
neither extra pulls them in.

"""

import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Set
from unittest.mock import patch

import pytest


def _collect_aspect_names(workunits: Iterable) -> Set[str]:
    """Extract the set of aspect names emitted by an ingestion run."""
    aspect_names: Set[str] = set()
    for wu in workunits:
        metadata = getattr(wu, "metadata", None)
        aspect_name = getattr(metadata, "aspectName", None)
        if aspect_name:
            aspect_names.add(aspect_name)
    return aspect_names


@pytest.mark.integration
class TestS3SlimNoPySpark:
    """Integration tests for s3-slim without PySpark dependencies."""

    @pytest.fixture(autouse=True)
    def mock_missing_pyspark(self):
        """Automatically mock missing pyspark/pydeequ for all tests in this class."""
        with patch.dict(
            sys.modules,
            {
                "pyspark": None,
                "pyspark.sql": None,
                "pyspark.sql.dataframe": None,
                "pyspark.sql.types": None,
                "pydeequ": None,
                "pydeequ.analyzers": None,
            },
        ):
            yield

    def test_s3_slim_pyspark_not_installed(self):
        with pytest.raises(ImportError):
            import pyspark

            print(pyspark)  # to avoid ruff removing this import as noop

    def test_s3_slim_pydeequ_not_installed(self):
        with pytest.raises(ImportError):
            import pydeequ

            print(pydeequ)  # to avoid ruff removing this import as noop

    def test_s3_source_loads_as_plugin(self):
        from datahub.ingestion.source.source_registry import source_registry

        s3_class = source_registry.get("s3")
        assert s3_class is not None

        from datahub.ingestion.source.s3.source import S3Source

        assert s3_class == S3Source

    def test_s3_config_without_profiling(self):
        from datahub.ingestion.source.s3.config import DataLakeSourceConfig

        config_dict = {
            "path_specs": [
                {
                    "include": "s3://test-bucket/data/*.csv",
                }
            ],
            "profiling": {"enabled": False},
        }

        config = DataLakeSourceConfig.parse_obj(config_dict)
        assert config is not None
        assert config.profiling.enabled is False

    def test_s3_source_creation_succeeds_with_profiling_and_no_pyspark(
        self, tmp_path: Path
    ) -> None:
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        test_file = tmp_path / "test.csv"
        test_file.write_text("id,name,value\n1,test,100\n2,sample,200\n")

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {"enabled": True},
        }

        ctx = PipelineContext(run_id="test-s3-slim-profiling")

        # FileProfiler (pyarrow/fastavro/datasketches) replaced the old
        # Spark+Deequ profiler, so this must succeed even with pyspark/pydeequ
        # mocked missing.
        source = S3Source.create(config_dict, ctx)
        workunits = list(source.get_workunits())
        assert len(workunits) > 0

    def test_s3_source_works_without_profiling(self, tmp_path: Path) -> None:
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        test_file = tmp_path / "test.csv"
        test_file.write_text("id,name,value\n1,test,100\n2,sample,200\n")

        config_dict = {
            "path_specs": [
                {
                    "include": f"{tmp_path}/*.csv",
                }
            ],
            "profiling": {"enabled": False},
        }

        ctx = PipelineContext(run_id="test-s3-slim-ingestion")

        source = S3Source.create(config_dict, ctx)
        assert source is not None

        workunits = list(source.get_workunits())
        assert len(workunits) > 0

    def test_schema_inference_enabled_emits_schema_metadata(
        self, tmp_path: Path
    ) -> None:
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        test_file = tmp_path / "test.csv"
        test_file.write_text("id,name,value\n1,test,100\n2,sample,200\n")

        ctx = PipelineContext(run_id="test-s3-infer-schema-on")
        source = S3Source.create(
            {
                "path_specs": [{"include": f"{tmp_path}/*.csv"}],
                "profiling": {"enabled": False},
            },
            ctx,
        )

        workunits = list(source.get_workunits())
        aspect_names = _collect_aspect_names(workunits)
        assert "schemaMetadata" in aspect_names

        dataset_props = next(
            wu.metadata.aspect
            for wu in workunits
            if getattr(wu.metadata, "aspectName", None) == "datasetProperties"
        )
        assert "schema_inferred_from" in dataset_props.customProperties

    def test_schema_inference_disabled_skips_schema_metadata(
        self, tmp_path: Path
    ) -> None:
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        test_file = tmp_path / "test.csv"
        test_file.write_text("id,name,value\n1,test,100\n2,sample,200\n")

        ctx = PipelineContext(run_id="test-s3-infer-schema-off")
        source = S3Source.create(
            {
                "path_specs": [{"include": f"{tmp_path}/*.csv"}],
                "profiling": {"enabled": False},
                "enable_schema_inference": False,
            },
            ctx,
        )

        workunits = list(source.get_workunits())
        aspect_names = _collect_aspect_names(workunits)
        # Schema must not be emitted, but the rest of the dataset metadata still is.
        assert "schemaMetadata" not in aspect_names
        assert "datasetProperties" in aspect_names

        # With inference off, no file is opened, so the dataset must not name a
        # source file it never read.
        dataset_props = next(
            wu.metadata.aspect
            for wu in workunits
            if getattr(wu.metadata, "aspectName", None) == "datasetProperties"
        )
        assert "schema_inferred_from" not in dataset_props.customProperties

    def test_schema_inference_enabled_empty_file_skips_schema_metadata(
        self, tmp_path: Path
    ) -> None:
        """An empty file never reaches schema inference (the schema block is
        guarded by size_in_bytes > 0), so even with inference enabled no
        schemaMetadata is emitted and schema_inferred_from is not set."""
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")

        ctx = PipelineContext(run_id="test-s3-infer-schema-empty")
        source = S3Source.create(
            {
                "path_specs": [{"include": f"{tmp_path}/*.csv"}],
                "profiling": {"enabled": False},
                "enable_schema_inference": True,
            },
            ctx,
        )

        workunits = list(source.get_workunits())
        aspect_names = _collect_aspect_names(workunits)
        assert "schemaMetadata" not in aspect_names
        assert "datasetProperties" in aspect_names

        dataset_props = next(
            wu.metadata.aspect
            for wu in workunits
            if getattr(wu.metadata, "aspectName", None) == "datasetProperties"
        )
        assert "schema_inferred_from" not in dataset_props.customProperties


@pytest.mark.integration
class TestS3SlimInstallation:
    def test_s3_slim_install_excludes_pyspark(self):
        """Test that installing acryl-datahub[s3-slim] does not install PySpark.

        This test creates a fresh venv and verifies the installation.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            venv_path = Path(tmpdir) / "test_venv"

            # Create venv
            result = subprocess.run(
                [sys.executable, "-m", "venv", str(venv_path)],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, f"Failed to create venv: {result.stderr}"

            # Install s3-slim
            pip_path = venv_path / "bin" / "pip"
            metadata_ingestion_path = Path(__file__).parent.parent.parent.parent

            result = subprocess.run(
                [
                    str(pip_path),
                    "install",
                    "-e",
                    f"{metadata_ingestion_path}[s3-slim]",
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
            assert result.returncode == 0, f"Failed to install s3-slim: {result.stderr}"

            # Verify PySpark is NOT installed
            python_path = venv_path / "bin" / "python"
            result = subprocess.run(
                [
                    str(python_path),
                    "-c",
                    "import pyspark; print('FAIL: pyspark found')",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode != 0, (
                "PySpark should NOT be installed with s3-slim extra. "
                f"Output: {result.stdout}"
            )
            assert (
                "ModuleNotFoundError" in result.stderr
                or "No module named" in result.stderr
            )

            # Verify s3 source loads
            result = subprocess.run(
                [
                    str(python_path),
                    "-c",
                    "from datahub.ingestion.source.s3.source import S3Source; print('SUCCESS')",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, f"S3 source failed to load: {result.stderr}"
            assert "SUCCESS" in result.stdout

    def test_s3_full_install_excludes_pyspark(self):
        """Test that installing acryl-datahub[s3] does NOT install PySpark.

        The `s3` extra (with profiling) used to require PySpark/Deequ; it now
        uses FileProfiler (pyarrow/fastavro/datasketches) instead, so PySpark
        should not be pulled in even by the full (non-slim) extra.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            venv_path = Path(tmpdir) / "test_venv"

            # Create venv
            result = subprocess.run(
                [sys.executable, "-m", "venv", str(venv_path)],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0

            # Install s3 (full, with profiling)
            pip_path = venv_path / "bin" / "pip"
            metadata_ingestion_path = Path(__file__).parent.parent.parent.parent

            result = subprocess.run(
                [
                    str(pip_path),
                    "install",
                    "-e",
                    f"{metadata_ingestion_path}[s3]",
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
            assert result.returncode == 0

            # Verify PySpark is NOT installed
            python_path = venv_path / "bin" / "python"
            result = subprocess.run(
                [
                    str(python_path),
                    "-c",
                    "import pyspark; print('FAIL: pyspark found')",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode != 0, (
                "PySpark should NOT be installed with the s3 extra anymore. "
                f"Output: {result.stdout}"
            )

            # Verify profiling dependencies (datasketches) ARE installed
            result = subprocess.run(
                [
                    str(python_path),
                    "-c",
                    "import datasketches; print('SUCCESS: datasketches found')",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, (
                f"datasketches should be installed with s3 extra: {result.stderr}"
            )
            assert "SUCCESS" in result.stdout
