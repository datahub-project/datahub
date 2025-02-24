import os
import json
from typing import Dict
from docgen_types import Platform
def generate(platforms: Dict[str, Platform],
                               origin_path: str = "../docs-website/filterTagIndexes.json",
                               output_path: str = "../docs-website/filterTagIndexesGenerated.json") -> None:
    """
    Generate and write a JSON file containing integration source information.
    Reads from original file and writes to new file, preserving the original.
    """

    # Read existing file if it exists
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

    integration_sources = []
    for platform_id, platform in platforms.items():
        if not platform.plugins:
            continue

        for plugin in sorted(
                platform.plugins.values(),
                key=lambda x: str(x.doc_order) if x.doc_order else x.name
        ):
            source = existing_sources.get(platform.name.lower(), {
                "Path": f"docs/generated/ingestion/sources/{platform_id}",
                "imgPath": get_platform_image_path(platform_id),
                "Title": platform.name,
                "Description": f"DataHub supports {platform.name} Integration.",
                "tags": {
                    "Platform Type": "",
                    "Connection Type": "Pull",
                    "Features": "UI Ingestion"
                }
            }).copy()

            integration_sources.append(source)
            # print(f"Adding {platform.name} to integration sources doc -- needs review")

    with open(output_path, 'w') as f:
        json.dump({"ingestionSources": sorted(integration_sources, key=lambda x: x["Title"])}, f, indent=2)

