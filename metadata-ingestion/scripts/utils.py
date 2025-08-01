import json
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def should_write_json_file(
    output_path: Path, 
    new_json_data: Dict[str, Any], 
    file_description: str = "JSON file"
) -> bool:
    """
    Check if a JSON file should be written by comparing content with existing file.
    
    This function compares the new JSON data with existing file content, excluding
    the 'generated_at' field from comparison since it changes on every generation.
    
    Args:
        output_path: Path to the output file
        new_json_data: The new JSON data to potentially write
        file_description: Description of the file for logging purposes
        
    Returns:
        True if the file should be written, False if content is unchanged
    """
    write_file = True
    
    if output_path.exists():
        try:
            with open(output_path, "r") as f:
                existing_data = json.load(f)

            # Create copies without generated_at for comparison
            existing_for_comparison = existing_data.copy()
            new_for_comparison = new_json_data.copy()
            existing_for_comparison.pop("generated_at", None)
            new_for_comparison.pop("generated_at", None)

            if json.dumps(
                existing_for_comparison, indent=2, sort_keys=True
            ) == json.dumps(new_for_comparison, indent=2, sort_keys=True):
                logger.info(f"No changes detected in {output_path}, skipping write.")
                write_file = False
        except Exception as e:
            logger.warning(f"Could not read existing {file_description}: {e}")
    
    return write_file 