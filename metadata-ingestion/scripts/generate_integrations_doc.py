from typing import Dict
from docgen_types import Platform
import json
import os


def generate(platforms: Dict[str, Platform],
                               origin_path: str = "../docs-website/filterTagIndexes.json",
                               output_path: str = "../docs-website/filterTagIndexesGenerated.json") -> None:
    """
    Generate and write a JSON file containing integration source information.
    Reads from original file and writes to new file, preserving the original.
    """

    # First read existing file if it exists
    existing_sources = {}
    if os.path.exists(origin_path):
        try:
            with open(origin_path, 'r') as f:
                data = json.load(f)
                existing_sources = {
                    source["Title"].lower(): source
                    for source in data.get("ingestionSources", [])
                }
        except json.JSONDecodeError:
            print(f"Warning: Could not parse existing file {origin_path}")

    def get_platform_image_path(platform_id: str) -> str:
        """Get platform image path trying different extensions and falling back to default."""
        base_path = "../docs-website/static/img/logos/platforms"
        if os.path.exists(f"{base_path}/{platform_id}.svg"):
            return f"img/logos/platforms/{platform_id}.svg"
        elif os.path.exists(f"{base_path}/{platform_id}.png"):
            return f"img/logos/platforms/{platform_id}.png"
        return "img/datahub-logo-color-mark.svg"

    # Use dictionary to prevent duplicates
    processed_sources = {}

    for platform_id, platform in platforms.items():
        if not platform.plugins:
            continue

        # Check if we already have this source
        existing_source = existing_sources.get(platform.name.lower())

        if existing_source:
            source = existing_source.copy()
        else:
            # Create new source object with minimal default values
            source = {
                "Path": f"docs/generated/ingestion/sources/{platform_id}",
                "imgPath": get_platform_image_path(platform_id),
                "Title": platform.name,
                "Description": f"DataHub supports {platform.name} Integration.",
                "tags": {
                    "Platform Type": "",
                    "Connection Type": "Pull",
                    "Features": "UI Ingestion"
                }
            }
            print(platform.name)

        # Store in processed_sources using lowercase title as key to prevent duplicates
        processed_sources[platform.name.lower()] = source

    # Convert back to list and sort
    integration_sources = sorted(processed_sources.values(), key=lambda x: x["Title"])

    # Write to output file
    with open(output_path, 'w') as f:
        json.dump({"ingestionSources": integration_sources}, f, indent=2)