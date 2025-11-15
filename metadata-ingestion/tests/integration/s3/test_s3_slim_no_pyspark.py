"""
Integration test to validate s3-slim installation works without PySpark.

This test ensures that the s3-slim pip extra can be installed and used
without PySpark dependencies, which is critical for lightweight deployments.

NOTE: Most tests in this file are designed to run in s3-slim environments
and will be skipped if PySpark is installed (e.g., in dev environments).
"""

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

# Check if PySpark is available for test skipping
try:
    import pyspark  # noqa: F401

    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

# Skip marker for tests that should only run without PySpark
requires_no_pyspark = pytest.mark.skipif(
    _PYSPARK_AVAILABLE,
    reason="Test only runs in s3-slim environments without PySpark",
)


@pytest.mark.integration
class TestS3SlimNoPySpark:
    """Integration tests for s3-slim without PySpark dependencies."""

    @requires_no_pyspark
    def test_s3_slim_pyspark_not_installed(self):
        """Verify that s3-slim installation does not include PySpark."""
        try:
            import pyspark

            pytest.fail(
                "PySpark should NOT be installed when using s3-slim extra. "
                f"Found pyspark at: {pyspark.__file__}"
            )
        except ImportError:
            # This is expected - PySpark should not be available
            pass

    @requires_no_pyspark
    def test_s3_slim_pydeequ_not_installed(self):
        """Verify that s3-slim installation does not include PyDeequ."""
        try:
            import pydeequ

            pytest.fail(
                "PyDeequ should NOT be installed when using s3-slim extra. "
                f"Found pydeequ at: {pydeequ.__file__}"
            )
        except ImportError:
            # This is expected - PyDeequ should not be available
            pass

    @requires_no_pyspark
    def test_s3_source_imports_successfully(self):
        """Verify that S3 source can be imported without PySpark."""
        from datahub.ingestion.source.s3.source import S3Source

        assert S3Source is not None

    @requires_no_pyspark
    def test_s3_source_loads_as_plugin(self):
        """Verify that S3 source is registered and loadable as a plugin."""
        from datahub.ingestion.api.registry import PluginRegistry

        # Get the source registry
        registry = PluginRegistry[type]()

        # The s3 source should be available
        s3_class = registry.get("s3")
        assert s3_class is not None

        # Verify it's the right class
        from datahub.ingestion.source.s3.source import S3Source

        assert s3_class == S3Source

    @requires_no_pyspark
    def test_s3_config_without_profiling(self):
        """Verify S3 config can be created without profiling."""
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

    @requires_no_pyspark
    def test_s3_config_profiling_enabled_accepted(self):
        """Verify S3 config accepts profiling=True even without PySpark.

        The config should accept profiling=True for backward compatibility.
        The actual error will occur when the source tries to initialize profiling.
        """
        from datahub.ingestion.source.s3.config import DataLakeSourceConfig

        config_dict = {
            "path_specs": [
                {
                    "include": "s3://test-bucket/data/*.csv",
                }
            ],
            "profiling": {"enabled": True},
        }

        # Config creation should succeed
        config = DataLakeSourceConfig.parse_obj(config_dict)
        assert config is not None
        assert config.profiling.enabled is True

    @requires_no_pyspark
    def test_s3_source_creation_fails_with_profiling_no_pyspark(self):
        """Verify S3 source creation fails with clear error when profiling enabled without PySpark."""
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

        # Creating the source with profiling enabled should fail
        with pytest.raises(RuntimeError) as exc_info:
            S3Source.create(config_dict, ctx)

        error_msg = str(exc_info.value)
        assert "PySpark is not installed" in error_msg
        assert "S3 profiling" in error_msg
        assert "acryl-datahub[data-lake-profiling]" in error_msg

    @requires_no_pyspark
    def test_s3_source_works_without_profiling(self, tmp_path: Path) -> None:
        """Verify S3 source can run ingestion without profiling."""
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.source.s3.source import S3Source

        # Create test CSV file
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

        # Creating and running the source should work
        source = S3Source.create(config_dict, ctx)
        assert source is not None

        # Get workunits - should not raise any PySpark-related errors
        workunits = list(source.get_workunits())
        assert len(workunits) > 0


@pytest.mark.integration
class TestS3SlimInstallation:
    """Tests that validate s3-slim can be installed in isolated environments."""

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

        This ensures backward compatibility - standard s3 extra includes PySpark.
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
