import json
import tempfile
from pathlib import Path

from utils import should_write_json_file


class TestShouldWriteJsonFile:
    """Test the should_write_json_file utility function."""

    def test_should_write_when_file_does_not_exist(self):
        """Test that function returns True when output file doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "nonexistent.json"
            new_data = {"key": "value", "generated_at": "2023-01-01T00:00:00Z"}

            result = should_write_json_file(output_path, new_data)

            assert result is True

    def test_should_write_when_content_changed(self):
        """Test that function returns True when content has actually changed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "test.json"

            # Create existing file with different content
            existing_data = {"key": "old_value", "generated_at": "2023-01-01T00:00:00Z"}
            with open(output_path, "w") as f:
                json.dump(existing_data, f)

            # New data with different content
            new_data = {"key": "new_value", "generated_at": "2023-01-02T00:00:00Z"}

            result = should_write_json_file(output_path, new_data)

            assert result is True

    def test_should_not_write_when_only_timestamp_changed(self):
        """Test that function returns False when only generated_at timestamp changed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "test.json"

            # Create existing file
            existing_data = {"key": "value", "generated_at": "2023-01-01T00:00:00Z"}
            with open(output_path, "w") as f:
                json.dump(existing_data, f)

            # New data with same content but different timestamp
            new_data = {"key": "value", "generated_at": "2023-01-02T00:00:00Z"}

            result = should_write_json_file(output_path, new_data)

            assert result is False
