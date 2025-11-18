"""
Integration test to validate s3-slim installation works without PySpark.

This test ensures that the s3-slim pip extra can be installed and used
without PySpark dependencies, which is critical for lightweight deployments.

"""

import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


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
            pass

    def test_s3_slim_pydeequ_not_installed(self):
        with pytest.raises(ImportError):
            pass

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

    def test_s3_source_creation_fails_with_profiling_no_pyspark(self):
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        config_dict = {
            "path_specs": [
                {
                    "include": "s3://test-bucket/data/*.csv",
                }
            ],
            "profiling": {"enabled": True},
        }

        ctx = PipelineContext(run_id="test-s3-slim")

        with pytest.raises(RuntimeError) as exc_info:
            S3Source.create(config_dict, ctx)

        error_msg = str(exc_info.value)
        assert "PySpark is not installed" in error_msg
        assert "S3 profiling" in error_msg
        assert "acryl-datahub[data-lake-profiling]" in error_msg

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

    def test_s3_full_install_includes_pyspark(self):
        """Test that installing acryl-datahub[s3] DOES install PySpark.

        Standard s3 extra includes PySpark.
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

            # Install s3 (full, with PySpark)
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

            # Verify PySpark IS installed
            python_path = venv_path / "bin" / "python"
            result = subprocess.run(
                [
                    str(python_path),
                    "-c",
                    "import pyspark; print('SUCCESS: pyspark found')",
                ],
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, "PySpark should be installed with s3 extra"
            assert "SUCCESS" in result.stdout
