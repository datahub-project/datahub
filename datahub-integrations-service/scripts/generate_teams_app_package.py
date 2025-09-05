#!/usr/bin/env python3
"""
Script to generate Teams app package with updated permissions.

This script creates a Teams app package (ZIP file) that can be uploaded to Microsoft Teams.
It includes the updated manifest with hybrid permission model supporting both Application
and Delegated permissions for maximum enterprise compatibility.

Usage:
    python scripts/generate_teams_app_package.py --app-id YOUR_APP_ID --webhook-url YOUR_WEBHOOK_URL

    # With custom output directory
    python scripts/generate_teams_app_package.py --app-id YOUR_APP_ID --webhook-url YOUR_WEBHOOK_URL --output-dir ./packages

    # Using environment variables
    export TEAMS_APP_ID="your-app-id"
    export TEAMS_WEBHOOK_URL="https://your-domain.com/public/teams/webhook"
    python scripts/generate_teams_app_package.py
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add the src directory to Python path so we can import our modules
script_dir = Path(__file__).parent
src_dir = script_dir.parent / "src"
sys.path.insert(0, str(src_dir))

from datahub_integrations.teams.app_manifest import (  # noqa: E402
    generate_teams_app_package,
    get_teams_app_manifest,
)


def load_env_file(env_file_path):
    """Load environment variables from a .env file."""
    if not os.path.exists(env_file_path):
        return

    with open(env_file_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                # Don't override existing environment variables
                if key not in os.environ:
                    os.environ[key] = value


def bump_version(current_version: str, bump_type: str) -> str:
    """
    Bump a semantic version.

    Args:
        current_version: Current version (e.g., "1.2.0")
        bump_type: Type of bump ("major", "minor", "patch")

    Returns:
        New version string
    """
    try:
        parts = current_version.split(".")
        if len(parts) != 3:
            raise ValueError("Version must be in format major.minor.patch")

        major, minor, patch = map(int, parts)

        if bump_type == "major":
            major += 1
            minor = 0
            patch = 0
        elif bump_type == "minor":
            minor += 1
            patch = 0
        elif bump_type == "patch":
            patch += 1
        else:
            raise ValueError(f"Invalid bump type: {bump_type}")

        return f"{major}.{minor}.{patch}"
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid version format '{current_version}': {e}") from e


def get_version_with_context(
    current_version: str, bump_type: str = None
) -> tuple[str, str]:
    """
    Get version with context about what changed.

    Returns:
        Tuple of (new_version, change_description)
    """
    if bump_type:
        old_version = current_version
        new_version = bump_version(current_version, bump_type)

        change_descriptions = {
            "major": "Breaking changes - incompatible API changes",
            "minor": "New features - backward compatible functionality added",
            "patch": "Bug fixes - backward compatible bug fixes",
        }

        change_desc = change_descriptions[bump_type]
        return new_version, f"Bumped from {old_version} → {new_version} ({change_desc})"
    else:
        return current_version, f"Using version {current_version}"


def main():
    """Generate Teams app package with updated permissions."""

    # Load environment variables from .env.teams file if it exists
    script_dir = Path(__file__).parent
    env_file = script_dir / ".env.teams"
    load_env_file(env_file)

    parser = argparse.ArgumentParser(
        description="Generate Microsoft Teams app package for DataHub integration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--app-id",
        help="Microsoft Teams App ID (can also use DATAHUB_TEAMS_APP_ID or TEAMS_APP_ID env var)",
        default=os.environ.get("DATAHUB_TEAMS_APP_ID")
        or os.environ.get("TEAMS_APP_ID"),
    )

    parser.add_argument(
        "--webhook-url",
        help="Webhook URL for Teams integration (can also use DATAHUB_TEAMS_WEBHOOK_URL or TEAMS_WEBHOOK_URL env var)",
        default=os.environ.get("DATAHUB_TEAMS_WEBHOOK_URL")
        or os.environ.get("TEAMS_WEBHOOK_URL"),
    )

    parser.add_argument(
        "--icon-url",
        help="Optional custom icon URL (uses default DataHub icons if not provided)",
        default=None,
    )

    parser.add_argument(
        "--version",
        help="App version following semantic versioning (default: 1.2.0)",
        default="1.2.0",
    )

    parser.add_argument(
        "--bump",
        choices=["major", "minor", "patch"],
        help="Bump version automatically (major, minor, or patch)",
    )

    parser.add_argument(
        "--output-dir",
        help="Directory to save the generated package (default: current directory)",
        default=".",
    )

    parser.add_argument(
        "--show-manifest",
        action="store_true",
        help="Display the generated manifest JSON before creating package",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate inputs and show manifest, don't create package",
    )

    args = parser.parse_args()

    # Handle version bumping
    try:
        final_version, version_context = get_version_with_context(
            args.version, args.bump
        )
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

    # Show what was loaded from .env.teams
    if env_file.exists():
        print(f"✅ Loaded configuration from {env_file}")
        print(f"   App ID: {args.app_id or 'Not set'}")
        print(f"   Webhook URL: {args.webhook_url or 'Not set'}")

    print(f"📋 Version: {version_context}")

    # Validate required arguments
    if not args.app_id:
        print("❌ Error: App ID is required.")
        print("\nOptions to provide App ID:")
        print("1. Command line: --app-id YOUR_APP_ID")
        print("2. Environment variable: DATAHUB_TEAMS_APP_ID or TEAMS_APP_ID")
        print(f"3. Add to {env_file}: DATAHUB_TEAMS_APP_ID=your-app-id")
        print("\nTo get your App ID:")
        print("1. Go to Azure Portal → App Registrations")
        print("2. Find your DataHub Teams app")
        print("3. Copy the 'Application (client) ID'")
        sys.exit(1)

    if not args.webhook_url:
        print("❌ Error: Webhook URL is required.")
        print("\nOptions to provide Webhook URL:")
        print("1. Command line: --webhook-url YOUR_WEBHOOK_URL")
        print("2. Environment variable: DATAHUB_TEAMS_WEBHOOK_URL or TEAMS_WEBHOOK_URL")
        print(
            f"3. Add to {env_file}: DATAHUB_TEAMS_WEBHOOK_URL=https://your-domain.com/public/teams/webhook"
        )
        print(
            "\nExample webhook URL: https://your-datahub-domain.com/public/teams/webhook"
        )
        sys.exit(1)

    # Validate webhook URL format
    if not args.webhook_url.startswith(("http://", "https://")):
        print("❌ Error: Webhook URL must start with http:// or https://")
        print(f"   Provided: {args.webhook_url}")
        sys.exit(1)

    if not args.webhook_url.endswith("/public/teams/webhook"):
        print("⚠️  Warning: Webhook URL should end with '/public/teams/webhook'")
        print(f"   Provided: {args.webhook_url}")
        print(f"   Expected: {args.webhook_url.rstrip('/')}/public/teams/webhook")

        response = input("Continue anyway? (y/N): ")
        if response.lower() != "y":
            sys.exit(1)

    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("🚀 Generating Teams app package...")
    print(f"   App ID: {args.app_id}")
    print(f"   Webhook URL: {args.webhook_url}")
    print(f"   Version: {final_version}")
    print(f"   Output Directory: {output_dir.absolute()}")

    try:
        # Generate manifest to show/validate
        manifest = get_teams_app_manifest(
            app_id=args.app_id,
            webhook_url=args.webhook_url,
            icon_url=args.icon_url,
            version=final_version,
        )

        if args.show_manifest or args.validate_only:
            print("\n📋 Generated Manifest:")
            print("=" * 50)
            print(json.dumps(manifest, indent=2))
            print("=" * 50)

            # Show permission summary
            permissions = (
                manifest.get("authorization", {})
                .get("permissions", {})
                .get("resourceSpecific", [])
            )
            app_permissions = [p for p in permissions if p.get("type") == "Application"]
            delegated_permissions = [
                p for p in permissions if p.get("type") == "Delegated"
            ]

            print("\n🔐 Permission Summary:")
            print(f"   Application Permissions: {len(app_permissions)}")
            for perm in app_permissions:
                print(f"     - {perm['name']}")
            print(f"   Delegated Permissions: {len(delegated_permissions)}")
            for perm in delegated_permissions:
                print(f"     - {perm['name']}")

        if args.validate_only:
            print("\n✅ Validation complete. Use --show-manifest to see full manifest.")
            return

        # Generate the package
        package_path = generate_teams_app_package(
            app_id=args.app_id,
            webhook_url=args.webhook_url,
            icon_url=args.icon_url,
            output_dir=str(output_dir),
            version=final_version,
        )

        package_file = Path(package_path)
        file_size = package_file.stat().st_size

        print("\n✅ Teams app package generated successfully!")
        print(f"   📦 Package: {package_file.absolute()}")
        print(f"   📏 Size: {file_size:,} bytes")

        print("\n📋 Next Steps:")
        print("1. Go to Microsoft Teams Admin Center")
        print("2. Navigate to Teams apps → Manage apps")
        print("3. Click 'Upload new app' → 'Upload'")
        print(f"4. Select the generated file: {package_file.name}")
        print("5. Review and approve the app permissions")

        print("\n🔗 Useful Links:")
        print(
            "   Teams Admin Center: https://admin.teams.microsoft.com/policies/manage-apps"
        )
        print(
            "   Azure App Registrations: https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade"
        )

        # Show permission requirements
        print("\n⚠️  Permission Requirements:")
        print("   This app requests both Application and Delegated permissions.")
        print("   You may need Global Admin approval for Application permissions.")
        print(
            "   If blocked, the app can fall back to Delegated permissions with user consent."
        )

    except Exception as e:
        print(f"❌ Error generating Teams app package: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
