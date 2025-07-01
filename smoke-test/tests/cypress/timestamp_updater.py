import json
import time
from pathlib import Path
from typing import Any, List


class TimestampUpdater:
    """Updates timestamps in JSON fixture files based on configuration."""

    def __init__(self, timestamp_config):
        self.timestamp_config = timestamp_config

    def get_current_timestamp_ms(self) -> int:
        """Returns current epoch timestamp in milliseconds."""
        return int(time.time() * 1000)

    def update_value_at_path(self, data: Any, path: str, value: Any) -> bool:
        """
        Updates a value in nested data structure using a simplified JSONPath.

        Args:
            data: The data structure to update
            path: Simplified JSONPath (e.g., "$.*.created.time")
            value: The new value to set

        Returns:
            True if any updates were made, False otherwise
        """
        parts = path.strip("$").strip(".").split(".")

        def recursive_update(obj: Any, remaining_parts: List[str]) -> bool:
            if not remaining_parts:
                return False

            current_part = remaining_parts[0]
            remaining = remaining_parts[1:]

            if current_part == "*":
                # Handle wildcard - iterate through all items
                if isinstance(obj, list):
                    any_updated = False
                    for item in obj:
                        if recursive_update(item, remaining):
                            any_updated = True
                    return any_updated
                elif isinstance(obj, dict):
                    any_updated = False
                    for key in obj:
                        if recursive_update(obj[key], remaining):
                            any_updated = True
                    return any_updated
            else:
                # Handle specific key
                if isinstance(obj, dict) and current_part in obj:
                    if not remaining:
                        # We've reached the target
                        obj[current_part] = value
                        return True
                    else:
                        # Continue traversing
                        return recursive_update(obj[current_part], remaining)

            return False

        return recursive_update(data, parts)

    def update_timestamps_in_file(self, filepath: str) -> bool:
        """
        Updates timestamps in a specific file based on configuration.

        Args:
            filepath: Path to the JSON file

        Returns:
            True if file was updated, False otherwise
        """
        filename = Path(filepath).name

        # Check if this file needs timestamp updates
        if filename not in self.timestamp_config:
            return False

        try:
            # Read the file
            with open(filepath, "r") as f:
                data = json.load(f)

            # Get current timestamp
            current_timestamp = self.get_current_timestamp_ms()

            # Update timestamps based on configuration
            any_updated = False
            for path_expression in self.timestamp_config[filename]:
                if self.update_value_at_path(data, path_expression, current_timestamp):
                    any_updated = True

            # Write back if any updates were made
            if any_updated:
                with open(filepath, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"Updated timestamps in {filename}")
                return True

            return False

        except Exception as e:
            print(f"Error updating timestamps in {filepath}: {e}")
            return False

    def update_all_configured_files(self, base_dir: str) -> None:
        """
        Updates timestamps in all configured files within the base directory.

        Args:
            base_dir: Base directory containing the fixture files
        """
        for filename in self.timestamp_config.keys():
            filepath = Path(base_dir) / filename
            if filepath.exists():
                self.update_timestamps_in_file(str(filepath))
            else:
                print(f"Warning: Configured file {filename} not found in {base_dir}")
