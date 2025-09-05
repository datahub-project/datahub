import json
from typing import Any, Dict, Optional

try:
    from PIL import Image, ImageDraw, ImageFont

    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    # Create dummy objects to prevent import errors
    Image = None  # type: ignore
    ImageDraw = None  # type: ignore
    ImageFont = None  # type: ignore


def _create_fallback_icons(color_icon_path: str, outline_icon_path: str) -> None:
    """Create minimal Teams-compliant icons as fallback."""
    if (
        PIL_AVAILABLE
        and Image is not None
        and ImageDraw is not None
        and ImageFont is not None
    ):
        # Create color icon (192x192, blue DataHub-style)
        color_icon = Image.new("RGBA", (192, 192), (0, 0, 0, 0))
        draw = ImageDraw.Draw(color_icon)

        # Draw a simple DataHub-style icon (circle with "DH")
        draw.ellipse([20, 20, 172, 172], fill=(23, 167, 255, 255))

        # Try to add text if font is available
        try:
            font = ImageFont.truetype("Arial", 60)
        except (OSError, ImportError):
            font = ImageFont.load_default()  # type: ignore

        # Get text dimensions
        bbox = draw.textbbox((0, 0), "DH", font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        x = (192 - text_width) // 2
        y = (192 - text_height) // 2

        draw.text((x, y), "DH", fill=(255, 255, 255, 255), font=font)
        color_icon.save(color_icon_path, "PNG")

        # Create outline icon (32x32, white on fully transparent)
        outline_icon = Image.new("RGBA", (32, 32), (0, 0, 0, 0))
        draw = ImageDraw.Draw(outline_icon)

        # Draw white circle outline only (no fill, transparent background)
        draw.ellipse([3, 3, 29, 29], outline=(255, 255, 255, 255), width=1)

        # Add "DH" text in white
        try:
            small_font = ImageFont.truetype("Arial", 10)
        except (OSError, ImportError):
            small_font = ImageFont.load_default()  # type: ignore

        bbox = draw.textbbox((0, 0), "DH", font=small_font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        x = (32 - text_width) // 2
        y = (32 - text_height) // 2

        draw.text((x, y), "DH", fill=(255, 255, 255, 255), font=small_font)

        # Force save with transparency
        outline_icon.save(outline_icon_path, "PNG", transparency=0)

    else:
        # PIL not available, create minimal PNG files
        # 192x192 blue square for color icon
        color_png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\xc0\x00\x00\x00\xc0\x08\x06\x00\x00\x00R\xdc\x9d\x14\x00\x00\x00\x19tEXtSoftware\x00Adobe ImageReadyq\xc9e<\x00\x00\x00\x18IDATx\xdab\x00\x02\x00\x00\x05\x00\x01\xe2&\x05[B\x00\x00\x00\x00IEND\xaeB`\x82"
        with open(color_icon_path, "wb") as f:
            f.write(color_png)

        # Create a completely transparent 32x32 PNG for outline icon
        # This is a minimal transparent PNG
        outline_png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00 \x00\x00\x00 \x08\x06\x00\x00\x00szz\xf4\x00\x00\x00\x0bIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xdb\x00\x00\x00\x00IEND\xaeB`\x82"
        with open(outline_icon_path, "wb") as f:
            f.write(outline_png)


def get_teams_app_manifest(
    app_id: str, webhook_url: str, icon_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generate the Teams app manifest.

    Args:
        app_id: The Microsoft Teams app ID
        webhook_url: The webhook URL for receiving Teams messages
        icon_url: Optional icon URL for the app

    Returns:
        The Teams app manifest as a dictionary
    """

    manifest = {
        "$schema": "https://developer.microsoft.com/en-us/json-schemas/teams/v1.16/MicrosoftTeams.schema.json",
        "manifestVersion": "1.16",
        "version": "1.3.5",
        "id": app_id,
        "developer": {
            "name": "DataHub",
            "websiteUrl": "https://datahub.com",
            "privacyUrl": "https://datahub.com/privacy",
            "termsOfUseUrl": "https://datahub.com/terms",
        },
        "icons": {
            "color": "color.png",
            "outline": "outline.png",
        },
        "name": {"short": "DataHub", "full": "DataHub - Modern Data Discovery"},
        "description": {
            "short": "Discover and understand your data",
            "full": "DataHub integration for Microsoft Teams allows you to search, discover, and get insights about your data directly from Teams. Search across datasets, charts, dashboards, and more from your DataHub instance.",
        },
        "accentColor": "#142f39",
        "bots": [
            {
                "botId": app_id,
                "scopes": ["personal", "team", "groupchat"],
                "commandLists": [
                    {
                        "scopes": ["personal", "team", "groupchat"],
                        "commands": [
                            {
                                "title": "Ask Question",
                                "description": "Ask a question about your data",
                            },
                            {
                                "title": "Get Entity",
                                "description": "Get details for a specific entity by URN",
                            },
                            {
                                "title": "Search",
                                "description": "Search across your DataHub instance",
                            },
                            {
                                "title": "Help",
                                "description": "Get help with DataHub commands",
                            },
                        ],
                    }
                ],
                "supportsFiles": False,
                "isNotificationOnly": False,
            }
        ],
        "composeExtensions": [
            {
                "botId": app_id,
                "commands": [
                    {
                        "id": "searchDataHub",
                        "context": ["compose"],
                        "description": "Search your DataHub instance",
                        "title": "Search DataHub",
                        "type": "query",
                        "parameters": [
                            {
                                "name": "query",
                                "title": "Search Query",
                                "description": "Enter your search query",
                                "inputType": "text",
                            }
                        ],
                    }
                ],
                "messageHandlers": [
                    {"type": "link", "value": {"domains": ["*.acryl.io"]}}
                ],
            }
        ],
        "activities": {
            "activityTypes": [
                {
                    "type": "datasetUpdated",
                    "description": "Dataset has been updated",
                    "templateText": "Dataset {datasetName} has been updated",
                },
                {
                    "type": "entityChanged",
                    "description": "Entity has been changed",
                    "templateText": "{entityName} has been changed",
                },
                {
                    "type": "incidentNotification",
                    "description": "Incident notification",
                    "templateText": "Incident: {incidentTitle}",
                },
                {
                    "type": "complianceNotification",
                    "description": "Compliance notification",
                    "templateText": "Compliance: {formName}",
                },
                {
                    "type": "generalNotification",
                    "description": "General DataHub notification",
                    "templateText": "DataHub notification: {message}",
                },
            ]
        },
        "permissions": ["identity", "messageTeamMembers"],
        "validDomains": ["*.datahub.com", "*.acryl.io"],
        "webApplicationInfo": {
            "id": app_id,
            "resource": "https://graph.microsoft.com/",
        },
    }

    return manifest


def generate_teams_app_package(
    app_id: str, webhook_url: str, icon_url: Optional[str] = None, output_dir: str = "."
) -> str:
    """
    Generate a Teams app package as a ZIP file.

    Args:
        app_id: The Microsoft Teams app ID
        webhook_url: The webhook URL for receiving Teams messages
        icon_url: Optional icon URL for the app
        output_dir: Directory to save the package

    Returns:
        Path to the generated ZIP file
    """

    import os
    import tempfile
    import zipfile

    manifest = get_teams_app_manifest(app_id, webhook_url, icon_url)

    # Create temporary directory for package files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Write manifest.json
        manifest_path = os.path.join(temp_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        # Create icon files with proper DataHub branding
        color_icon_path = os.path.join(temp_dir, "color.png")
        outline_icon_path = os.path.join(temp_dir, "outline.png")

        # Try to find DataHub logo in the repository using relative paths
        datahub_logo_paths = [
            # Relative paths from repository root
            "datahub-web-react/src/images/datahublogo.png",
            "datahub-web-react/public/assets/logos/datahub-logo.png",
            # Alternative relative paths
            "../datahub-web-react/src/images/datahublogo.png",
            "../datahub-web-react/public/assets/logos/datahub-logo.png",
        ]

        datahub_icon_data = None
        for logo_path in datahub_logo_paths:
            try:
                if os.path.exists(logo_path):
                    with open(logo_path, "rb") as f:
                        datahub_icon_data = f.read()
                    break
            except Exception:
                continue

        if datahub_icon_data:
            # Create properly sized Teams icons
            if PIL_AVAILABLE and Image is not None:
                import io

                # Load the original DataHub logo
                original_image = Image.open(io.BytesIO(datahub_icon_data))

                # Create color icon (192x192)
                color_icon = original_image.resize((192, 192), Image.Resampling.LANCZOS)
                color_icon.save(color_icon_path, "PNG")

                # Create outline icon (32x32, white on transparent)
                outline_icon = original_image.resize((32, 32), Image.Resampling.LANCZOS)

                # Convert to RGBA if needed
                if outline_icon.mode != "RGBA":
                    outline_icon = outline_icon.convert("RGBA")

                # Create white outline version while preserving transparency
                data = outline_icon.getdata()
                new_data = []
                for item in data:  # type: ignore
                    # If pixel is not transparent, make it white but keep original alpha
                    if (
                        len(item) == 4 and item[3] > 0
                    ):  # Has alpha channel and not fully transparent
                        new_data.append(
                            (255, 255, 255, item[3])
                        )  # White with original alpha
                    else:
                        new_data.append((0, 0, 0, 0))  # Fully transparent

                outline_icon.putdata(new_data)

                # Save with explicit transparency preservation
                outline_icon.save(outline_icon_path, "PNG", pnginfo=None)

            else:
                # PIL not available, use original icons (might not meet Teams requirements)
                with open(color_icon_path, "wb") as f:
                    f.write(datahub_icon_data)
                with open(outline_icon_path, "wb") as f:
                    f.write(datahub_icon_data)
        else:
            # Create minimal valid icons as fallback
            _create_fallback_icons(color_icon_path, outline_icon_path)

        # Create ZIP package with version in filename
        version = manifest.get("version", "1.0.0")
        package_path = os.path.join(
            output_dir, f"datahub-teams-app-{app_id}-v{version}.zip"
        )
        with zipfile.ZipFile(package_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(manifest_path, "manifest.json")
            zf.write(color_icon_path, "color.png")
            zf.write(outline_icon_path, "outline.png")

    return package_path
