#!/usr/bin/env python3
"""
Teams App Manifest Generator

Generates a Microsoft Teams app manifest package for testing bot installations.
"""

import json
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class TeamsManifestGenerator:
    """Generates Teams app manifests for testing"""

    def __init__(self):
        self.script_dir = Path(__file__).parent
        self.output_dir = self.script_dir / "manifests"
        self.output_dir.mkdir(exist_ok=True)

    def load_bot_config(self) -> Dict:
        """Load bot configuration from environment variables"""
        import os

        config = {
            "bot_id": os.environ.get("DATAHUB_TEAMS_APP_ID"),
            "webhook_url": os.environ.get("DATAHUB_TEAMS_WEBHOOK_URL"),
        }

        return config

    def create_manifest(
        self,
        bot_id: str,
        app_name: str = "DataHub Test 1",
        app_version: str = "1.0.0",
        scopes: List[str] = None,
        supports_files: bool = False,
        test_mode: bool = False,
    ) -> Dict:
        """Create Teams app manifest"""

        if scopes is None:
            scopes = ["personal", "team", "groupchat"]

        # App ID logic:
        # - Test mode: Generate new UUID each time for separate test installations
        # - Production mode: Use bot_id to maintain single app identity
        if test_mode:
            app_id = str(uuid.uuid4())  # New UUID for each test
        else:
            app_id = bot_id  # Use bot_id for production consistency

        manifest = {
            "$schema": "https://developer.microsoft.com/en-us/json-schemas/teams/v1.16/MicrosoftTeams.schema.json",
            "manifestVersion": "1.16",
            "version": app_version,
            "id": app_id,
            "packageName": f"com.datahub.test.{app_id}",
            "developer": {
                "name": "DataHub Test Environment",
                "websiteUrl": "https://datahubproject.io",
                "privacyUrl": "https://datahubproject.io/privacy",
                "termsOfUseUrl": "https://datahubproject.io/terms",
            },
            "icons": {"color": "color.png", "outline": "outline.png"},
            "name": {"short": app_name, "full": f"{app_name} - Installation Testing"},
            "description": {
                "short": "⚠️ TEST APP - DataHub Teams bot installation test",
                "full": f"🧪 TEST APPLICATION - This is a test version of the DataHub Teams bot used for installation testing. Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. Do not use in production.",
            },
            "accentColor": "#5558AF",
            "bots": [
                {
                    "botId": bot_id,
                    "scopes": scopes,
                    "supportsFiles": supports_files,
                    "isNotificationOnly": False,
                    "supportsCalling": False,
                    "supportsVideo": False,
                }
            ],
            "activities": {
                "activityTypes": [
                    {
                        "type": "systemDefault",
                        "description": "DataHub notifications and updates",
                        "templateText": "You have new DataHub notifications",
                    }
                ]
            },
            "permissions": ["identity", "messageTeamMembers", "activityFeed"],
            "validDomains": ["*.ngrok.io", "*.ngrok-free.app", "datahubproject.io"],
            "webApplicationInfo": {
                "id": bot_id,
                "resource": "https://RscBasedStoreApp",
            },
        }

        return manifest

    def create_icon_files(self, output_dir: Path):
        """Copy the working icon files from the main DataHub Teams package"""
        import shutil

        # Path to existing working icons
        teams_packages_dir = self.script_dir.parent.parent.parent / "teams_packages"

        # Copy existing working icons
        if (teams_packages_dir / "color.png").exists():
            shutil.copy2(teams_packages_dir / "color.png", output_dir / "color.png")
            print("   ✅ Copied working color.png (192x192)")
        else:
            # Fallback: copy from current directory if available
            if (self.script_dir / "color.png").exists():
                shutil.copy2(self.script_dir / "color.png", output_dir / "color.png")
                print("   ✅ Copied color.png from install directory")
            else:
                raise FileNotFoundError("No color.png found to copy")

        if (teams_packages_dir / "outline.png").exists():
            shutil.copy2(teams_packages_dir / "outline.png", output_dir / "outline.png")
            print("   ✅ Copied working outline.png (32x32)")
        else:
            # Fallback: copy from current directory if available
            if (self.script_dir / "outline.png").exists():
                shutil.copy2(
                    self.script_dir / "outline.png", output_dir / "outline.png"
                )
                print("   ✅ Copied outline.png from install directory")
            else:
                raise FileNotFoundError("No outline.png found to copy")

    def generate_manifest_package(
        self,
        scenario_name: str = "test-install",
        bot_id: Optional[str] = None,
        scopes: List[str] = None,
        app_name: Optional[str] = None,
        test_mode: bool = None,
    ) -> str:
        """Generate complete Teams app package"""

        if bot_id is None:
            config = self.load_bot_config()
            bot_id = config["bot_id"]
            if not bot_id:
                raise ValueError(
                    "No bot ID found in configuration. Please provide bot_id parameter."
                )

        if scopes is None:
            scopes = ["personal", "team", "groupchat"]

        # Determine test mode: auto-detect or explicit
        if test_mode is None:
            test_mode = (
                scenario_name.startswith(("test-", "demo-"))
                or "test" in scenario_name.lower()
            )

        print(f"🔨 Generating Teams app manifest for scenario: {scenario_name}")
        print(f"   Bot ID: {bot_id}")
        print(f"   Scopes: {scopes}")
        print(f"   Test Mode: {test_mode}")

        # Create manifest with proper app name and mode
        if app_name is None:
            if test_mode:
                # Test mode: "DataHub Test <scenario> <timestamp>"
                timestamp = datetime.now().strftime("%Y%m%d-%H%M")
                app_name = f"DataHub Test {scenario_name} {timestamp}"
            else:
                # Production mode: extract username from scenario or use generic
                if "-" in scenario_name:
                    username = scenario_name.split("-")[0]
                    timestamp = datetime.now().strftime("%b %d %H:%M")
                    app_name = f"DataHub Dev {username} {timestamp}"
                else:
                    app_name = f"DataHub Dev {scenario_name}"

        manifest = self.create_manifest(
            bot_id=bot_id, app_name=app_name, scopes=scopes, test_mode=test_mode
        )

        # Create temp directory for package contents
        temp_dir = self.output_dir / f"temp_{scenario_name}"
        temp_dir.mkdir(exist_ok=True)

        try:
            # Write manifest.json
            with open(temp_dir / "manifest.json", "w") as f:
                json.dump(manifest, f, indent=2)

            # Create icon files
            self.create_icon_files(temp_dir)

            # Create zip package
            package_path = (
                self.output_dir
                / f"datahub-teams-{scenario_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
            )

            with zipfile.ZipFile(package_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(temp_dir / "manifest.json", "manifest.json")
                zipf.write(temp_dir / "color.png", "color.png")
                zipf.write(temp_dir / "outline.png", "outline.png")

            # Clean up temp directory
            import shutil

            shutil.rmtree(temp_dir)

            print(f"✅ Teams app package created: {package_path}")
            print("📋 App details:")
            print(f"   App ID: {manifest['id']}")
            print(f"   Version: {manifest['version']}")
            print(f"   Scopes: {', '.join(scopes)}")

            return str(package_path)

        except Exception as e:
            # Clean up on error
            import shutil

            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise e

    def list_scenarios(self) -> List[Dict]:
        """List available test scenarios"""
        scenarios = [
            {
                "name": "personal-test",
                "description": "DataHub Test App - Personal scope only",
                "scopes": ["personal"],
                "use_case": "Test bot installation in 1:1 chat",
            },
            {
                "name": "team-test",
                "description": "DataHub Test App - Team scope only",
                "scopes": ["team"],
                "use_case": "Test bot installation in team channels",
            },
            {
                "name": "full-test",
                "description": "DataHub Test App - All scopes",
                "scopes": ["personal", "team", "groupchat"],
                "use_case": "Test bot installation everywhere",
            },
            {
                "name": "groupchat-test",
                "description": "DataHub Test App - Group chat only",
                "scopes": ["groupchat"],
                "use_case": "Test bot installation in group chats only",
            },
        ]
        return scenarios


def main():
    """Main CLI interface"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Teams app manifest for installation testing"
    )
    parser.add_argument(
        "--scenario",
        default="test-install",
        help="Test scenario name (default: test-install)",
    )
    parser.add_argument("--bot-id", help="Microsoft App ID for the bot")
    parser.add_argument(
        "--scopes",
        nargs="+",
        choices=["personal", "team", "groupchat"],
        default=["personal", "team", "groupchat"],
        help="Bot scopes (default: all scopes)",
    )
    parser.add_argument(
        "--list-scenarios", action="store_true", help="List available test scenarios"
    )

    args = parser.parse_args()

    generator = TeamsManifestGenerator()

    if args.list_scenarios:
        scenarios = generator.list_scenarios()
        print("📋 Available test scenarios:")
        for scenario in scenarios:
            print(f"  • {scenario['name']}: {scenario['description']}")
            print(f"    Scopes: {', '.join(scenario['scopes'])}")
            print(f"    Use case: {scenario['use_case']}")
            print()
        return

    try:
        package_path = generator.generate_manifest_package(
            scenario_name=args.scenario, bot_id=args.bot_id, scopes=args.scopes
        )

        print("\n🚀 Next steps:")
        print("1. Start the installation test server:")
        print("   python3 installation_test_server.py")
        print("2. Upload the package to Teams:")
        print("   • Go to Teams > Apps > Manage your apps > Upload an app")
        print(f"   • Upload: {package_path}")
        print("3. Install the app in a Teams workspace")
        print("4. Check the server logs for installation events")

    except Exception as e:
        print(f"❌ Error generating manifest: {e}")
        return 1


if __name__ == "__main__":
    exit(main() or 0)
