import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from datahub.cli.specific.dataset_cli import dataset

TEST_RESOURCES_DIR = Path(__file__).parent / "test_resources"


@pytest.fixture
def test_yaml_file():
    """Creates a temporary test yaml file for testing."""
    # Define test data path
    test_file = TEST_RESOURCES_DIR / "dataset.yaml"

    # Create a temporary copy to work with
    temp_file = Path(f"{test_file}.tmp")
    shutil.copyfile(test_file, temp_file)

    yield temp_file

    # Clean up
    if temp_file.exists():
        temp_file.unlink()


@pytest.fixture
def invalid_value_yaml_file():
    """Creates a temporary  yaml file - correctly formatted but bad datatype for testing."""
    invalid_content = """
## This file is intentionally malformed
- id: user.badformat
  platform: hive
  schema:
    fields:
    - id: ip
      type: bad_type
      description: The IP address
    """

    # Create a temporary file
    temp_file = TEST_RESOURCES_DIR / "invalid_dataset.yaml.tmp"
    with open(temp_file, "w") as f:
        f.write(invalid_content)

    yield temp_file

    # Clean up
    if temp_file.exists():
        temp_file.unlink()


@pytest.fixture
def malformed_yaml_file():
    """Creates a temporary malformed yaml file for testing."""
    malformed_content = """
## This file is intentionally malformed
- id: user.badformat
  platform: hive
  schema:
    fields:
    - id: ip
      type string  # Missing colon here
      description: The IP address
    """

    # Create a temporary file
    temp_file = TEST_RESOURCES_DIR / "malformed_dataset.yaml.tmp"
    with open(temp_file, "w") as f:
        f.write(malformed_content)

    yield temp_file

    # Clean up
    if temp_file.exists():
        temp_file.unlink()


@pytest.fixture
def fixable_yaml_file():
    """Creates a temporary yaml file with fixable formatting issues."""
    fixable_content = """
## This file has fixable formatting issues
- id: user.fixable
  platform: hive
  schema:
    fields:
    - id: ip
      type: string
      description: The IP address
    - id:  user_id     # Extra spaces
      type: string
      description:    The user ID   # Extra spaces
    """

    temp_file = TEST_RESOURCES_DIR / "fixable_dataset.yaml.tmp"
    with open(temp_file, "w") as f:
        f.write(fixable_content)

    yield temp_file

    # Clean up
    if temp_file.exists():
        temp_file.unlink()


class TestDatasetCli:
    def test_dataset_file_command_exists(self):
        """Test that the dataset file command exists."""
        runner = CliRunner()
        result = runner.invoke(dataset, ["--help"])
        assert result.exit_code == 0
        assert "file" in result.output

    @patch("datahub.cli.specific.dataset_cli.Dataset")
    def test_lint_check_no_issues(self, mock_dataset, test_yaml_file):
        """Test the lintCheck option when no issues are found."""
        # Setup mocks
        mock_dataset_instance = MagicMock()
        mock_dataset.from_yaml.return_value = [mock_dataset_instance]
        mock_dataset_instance.to_yaml.return_value = None

        # Mock filecmp.cmp to return True (files match)
        with patch("filecmp.cmp", return_value=True):
            runner = CliRunner()
            result = runner.invoke(
                dataset, ["file", "--lintCheck", str(test_yaml_file)]
            )

            # Verify
            assert result.exit_code == 0
            assert "No differences found" in result.output
            mock_dataset.from_yaml.assert_called_once()
            mock_dataset_instance.to_yaml.assert_called_once()

    @patch("datahub.cli.specific.dataset_cli.Dataset")
    @patch("os.system")
    def test_lint_check_with_issues(self, mock_system, mock_dataset, fixable_yaml_file):
        """Test the lintCheck option when issues are found."""
        # Setup mocks
        mock_dataset_instance = MagicMock()
        mock_dataset.from_yaml.return_value = [mock_dataset_instance]

        # Mock filecmp.cmp to return False (files don't match)
        with patch("filecmp.cmp", return_value=False):
            runner = CliRunner()
            result = runner.invoke(
                dataset, ["file", "--lintCheck", str(fixable_yaml_file)]
            )

            # Verify
            assert result.exit_code == 0
            assert "To fix these differences" in result.output
            mock_dataset.from_yaml.assert_called_once()
            mock_dataset_instance.to_yaml.assert_called_once()
            mock_system.assert_called_once()  # Should call diff

    @patch("datahub.cli.specific.dataset_cli.Dataset")
    @patch("os.system")
    @patch("shutil.copyfile")
    def test_lint_fix(
        self, mock_copyfile, mock_system, mock_dataset, fixable_yaml_file
    ):
        """Test the lintFix option."""
        # Setup mocks
        mock_dataset_instance = MagicMock()
        mock_dataset.from_yaml.return_value = [mock_dataset_instance]

        # Mock filecmp.cmp to return False (files don't match)
        with patch("filecmp.cmp", return_value=False):
            runner = CliRunner()
            result = runner.invoke(
                dataset, ["file", "--lintCheck", "--lintFix", str(fixable_yaml_file)]
            )

            # Verify
            assert result.exit_code == 0
            assert "Fixed linting issues" in result.output

            # Check that copyfile was called twice:
            # 1. To copy the original file to the temp file
            # 2. To copy the fixed temp file back to the original
            assert mock_copyfile.call_count == 2

            # The second call should copy from temp file to the original
            mock_copyfile.call_args_list[1][0][0]  # Source of second call
            assert mock_copyfile.call_args_list[1][0][1] == str(
                fixable_yaml_file
            )  # Destination

    @patch("datahub.cli.specific.dataset_cli.Dataset")
    def test_error_handling(self, mock_dataset, malformed_yaml_file):
        """Test error handling when processing a malformed yaml file."""
        # Setup mock to raise an exception
        mock_dataset.from_yaml.side_effect = Exception("YAML parsing error")

        runner = CliRunner()
        result = runner.invoke(
            dataset, ["file", "--lintCheck", str(malformed_yaml_file)]
        )

        # Verify exception is properly handled
        assert result.exit_code != 0
        mock_dataset.from_yaml.assert_called_once()

    def test_temporary_file_cleanup(self, test_yaml_file):
        """Test that temporary files are properly cleaned up."""
        # Count files in the directory before
        files_before = len(list(TEST_RESOURCES_DIR.glob("*.tmp")))

        runner = CliRunner()
        with patch("datahub.cli.specific.dataset_cli.Dataset"):
            with patch("filecmp.cmp", return_value=True):
                runner.invoke(dataset, ["file", "--lintCheck", str(test_yaml_file)])

        # Count files after
        files_after = len(list(TEST_RESOURCES_DIR.glob("*.tmp")))

        # Should be same count (our fixture creates one tmp file)
        assert files_before == files_after

    @patch("datahub.cli.specific.dataset_cli.Dataset")
    def test_multiple_datasets_in_file(self, mock_dataset, test_yaml_file):
        """Test handling of multiple datasets defined in a single file."""
        # Create mock dataset instances
        mock_dataset1 = MagicMock()
        mock_dataset2 = MagicMock()
        mock_dataset.from_yaml.return_value = [mock_dataset1, mock_dataset2]

        with patch("filecmp.cmp", return_value=True):
            runner = CliRunner()
            result = runner.invoke(
                dataset, ["file", "--lintCheck", str(test_yaml_file)]
            )

            # Verify
            assert result.exit_code == 0
            assert "No differences found" in result.output

            # Verify both dataset instances had to_yaml called
            mock_dataset1.to_yaml.assert_called_once()
            mock_dataset2.to_yaml.assert_called_once()

    @patch("datahub.cli.specific.dataset_cli.get_default_graph")
    def test_dry_run_sync(self, mock_get_default_graph, test_yaml_file):
        mock_graph = MagicMock()
        mock_graph.exists.return_value = True
        mock_get_default_graph.return_value.__enter__.return_value = mock_graph

        runner = CliRunner()
        result = runner.invoke(
            dataset, ["sync", "--dry-run", "--to-datahub", "-f", str(test_yaml_file)]
        )

        # Verify
        assert result.exit_code == 0
        assert not mock_get_default_graph.emit.called

    @patch("datahub.cli.specific.dataset_cli.get_default_graph")
    def test_dry_run_sync_fail_bad_type(
        self, mock_get_default_graph, invalid_value_yaml_file
    ):
        mock_graph = MagicMock()
        mock_graph.exists.return_value = True
        mock_get_default_graph.return_value.__enter__.return_value = mock_graph

        runner = CliRunner()
        result = runner.invoke(
            dataset,
            ["sync", "--dry-run", "--to-datahub", "-f", str(invalid_value_yaml_file)],
        )

        # Verify
        assert result.exit_code != 0
        assert not mock_get_default_graph.emit.called
        assert "Type bad_type is not a valid primitive type" in result.output

    @patch("datahub.cli.specific.dataset_cli.get_default_graph")
    def test_dry_run_sync_fail_missing_ref(
        self, mock_get_default_graph, test_yaml_file
    ):
        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        mock_get_default_graph.return_value.__enter__.return_value = mock_graph

        runner = CliRunner()
        result = runner.invoke(
            dataset, ["sync", "--dry-run", "--to-datahub", "-f", str(test_yaml_file)]
        )

        # Verify
        assert result.exit_code != 0
        assert not mock_get_default_graph.emit.called
        assert "missing entity reference" in result.output

    @patch("datahub.cli.specific.dataset_cli.get_default_graph")
    def test_run_sync(self, mock_get_default_graph, test_yaml_file):
        mock_graph = MagicMock()
        mock_graph.exists.return_value = True
        mock_get_default_graph.return_value.__enter__.return_value = mock_graph

        runner = CliRunner()
        result = runner.invoke(
            dataset, ["sync", "--to-datahub", "-f", str(test_yaml_file)]
        )

        # Verify
        assert result.exit_code == 0
        assert mock_graph.emit.called

    @patch("datahub.cli.specific.dataset_cli.get_default_graph")
    def test_run_sync_fail(self, mock_get_default_graph, invalid_value_yaml_file):
        mock_graph = MagicMock()
        mock_graph.exists.return_value = True
        mock_get_default_graph.return_value.__enter__.return_value = mock_graph

        runner = CliRunner()
        result = runner.invoke(
            dataset, ["sync", "--to-datahub", "-f", str(invalid_value_yaml_file)]
        )

        # Verify
        assert result.exit_code != 0
        assert not mock_get_default_graph.emit.called
        assert "is not a valid primitive type" in result.output

    @patch("datahub.cli.specific.dataset_cli.get_default_graph")
    def test_run_upsert_fail(self, mock_get_default_graph, invalid_value_yaml_file):
        mock_graph = MagicMock()
        mock_graph.exists.return_value = True
        mock_get_default_graph.return_value.__enter__.return_value = mock_graph

        runner = CliRunner()
        result = runner.invoke(dataset, ["upsert", "-f", str(invalid_value_yaml_file)])

        # Verify
        assert result.exit_code != 0
        assert not mock_get_default_graph.emit.called
        assert "is not a valid primitive type" in result.output

    @patch("datahub.cli.specific.dataset_cli.get_default_graph")
    def test_sync_from_datahub_fail(self, mock_get_default_graph, test_yaml_file):
        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        mock_get_default_graph.return_value.__enter__.return_value = mock_graph

        runner = CliRunner()
        result = runner.invoke(
            dataset, ["sync", "--dry-run", "--from-datahub", "-f", str(test_yaml_file)]
        )

        # Verify
        assert result.exit_code != 0
        assert "does not exist" in result.output
