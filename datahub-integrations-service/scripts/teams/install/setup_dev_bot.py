#!/usr/bin/env python3
"""
DataHub Teams Bot Setup

Integrated workflow for creating Azure Bot resources and Teams app manifests.
Handles the complete flow from bot creation to app package generation.
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import yaml
from create_azure_bot import AzureBotCreator
from generate_teams_manifest import TeamsManifestGenerator
from loguru import logger


class DevBotSetup:
    """Integrated setup for development bots and Teams apps"""

    def __init__(self, username: str, config_file: Optional[str] = None):
        self.username = username
        self.config_file = config_file
        self.script_dir = Path(__file__).parent
        self.output_dir = self.script_dir / "dev_bots" / username
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.bot_creator = AzureBotCreator(config_file) if config_file else None
        self.manifest_generator = TeamsManifestGenerator()

    def generate_config_from_template(self, template_vars: Dict) -> str:
        """Generate a personalized bot configuration from template"""
        template_path = self.script_dir / "bot_config_template.yaml"

        if not template_path.exists():
            raise FileNotFoundError("bot_config_template.yaml not found")

        with open(template_path, "r") as f:
            template_content = f.read()

        # Add additional template variables
        all_vars = {
            "username": self.username,
            "timestamp": datetime.now().strftime("%Y%m%d-%H%M%S"),
            "uuid": str(__import__("uuid").uuid4()),
            "subscription_id": "6b171c8c-84c4-4293-9865-3d16ff142515",  # Default subscription
            **template_vars,
        }

        # Replace template variables
        config_content = template_content
        for key, value in all_vars.items():
            config_content = config_content.replace(f"{{{key}}}", str(value))

        # Generate personalized config file
        config_path = self.output_dir / "bot_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_content)

        logger.info(f"Generated config file: {config_path}")
        return str(config_path)

    def create_bot_resources(self, config_vars: Dict) -> Dict:
        """Create Azure bot resources using generated config"""
        logger.info(f"Creating bot resources for user: {self.username}")

        # Generate config from template
        config_path = self.generate_config_from_template(config_vars)

        # Create bot creator with generated config
        bot_creator = AzureBotCreator(config_path)

        # Check Azure CLI availability
        if not bot_creator.check_azure_cli():
            raise Exception(
                "Azure CLI not available. Please install and login to Azure CLI."
            )

        # Create bots from config
        results = bot_creator.create_bots_from_yaml()

        if not results:
            raise Exception("No bots were created")

        # Save results for reference
        results_path = self.output_dir / "bot_creation_results.yaml"
        with open(results_path, "w") as f:
            yaml.dump(
                {"created_bots": results, "timestamp": datetime.now().isoformat()}, f
            )

        logger.info(f"Bot creation results saved: {results_path}")
        return results[0]  # Return first bot result

    def create_teams_app_package(
        self, bot_result: Dict, scenario_name: str = None
    ) -> str:
        """Create Teams app manifest package using bot information"""
        if scenario_name is None:
            scenario_name = f"{self.username}-dev"

        app_id = bot_result["app_registration"]["app_id"]

        logger.info(f"Creating Teams app package for bot: {bot_result['bot_name']}")
        logger.info(f"Using App ID: {app_id}")

        # Generate Teams app manifest package (production mode)
        timestamp = datetime.now().strftime("%b %d %H:%M")
        custom_app_name = f"DataHub Dev {self.username} {timestamp}"

        package_path = self.manifest_generator.generate_manifest_package(
            scenario_name=scenario_name,
            bot_id=app_id,
            scopes=["personal", "team", "groupchat"],
            app_name=custom_app_name,
            test_mode=False,  # Production mode
        )

        # Copy package to our output directory
        package_file = Path(package_path)
        dest_path = self.output_dir / package_file.name

        import shutil

        shutil.copy2(package_path, dest_path)

        logger.info(f"Teams app package copied to: {dest_path}")
        return str(dest_path)

    def create_environment_file(self, bot_result: Dict) -> str:
        """Create environment file with bot configuration"""
        env_content = f"""# DataHub Teams Bot Environment Configuration
# Generated for user: {self.username}
# Generated on: {datetime.now().isoformat()}

# Azure Bot Configuration
DATAHUB_TEAMS_APP_ID={bot_result["app_registration"]["app_id"]}
DATAHUB_TEAMS_APP_SECRET={bot_result["app_registration"]["client_secret"]}
DATAHUB_TEAMS_WEBHOOK_URL={bot_result["teams_config"]["webhook_url"]}

# Azure Resource Information
AZURE_BOT_NAME={bot_result["bot_name"]}
AZURE_RESOURCE_GROUP={bot_result["resource_group"]}
AZURE_BOT_RESOURCE_ID={bot_result["bot_resource"]["resource_id"]}

# Teams App Information  
TEAMS_APP_NAME="DataHub Dev - {self.username}"
TEAMS_BOT_ID={bot_result["app_registration"]["app_id"]}

# Usage Instructions
# 1. Source this file: source {self.output_dir.name}/.env
# 2. Start your DataHub integrations service with these environment variables
# 3. Install the Teams app package in Microsoft Teams
# 4. Test the bot functionality

# Security Notes
# - Keep the CLIENT_SECRET secure and never commit it to version control
# - The webhook URL should point to your development environment
# - Update the webhook URL when your ngrok tunnel changes
"""

        env_path = self.output_dir / ".env"
        with open(env_path, "w") as f:
            f.write(env_content)

        logger.info(f"Environment file created: {env_path}")
        return str(env_path)

    def create_complete_setup(
        self, webhook_url: str = None, ngrok_subdomain: str = None
    ) -> Dict:
        """Create complete bot setup including Azure resources and Teams app"""
        logger.info(f"🚀 Starting complete bot setup for user: {self.username}")

        # Determine webhook URL
        if webhook_url is None:
            if ngrok_subdomain:
                webhook_url = f"https://{ngrok_subdomain}.ngrok-free.app/integrations/teams/webhook"
            else:
                webhook_url = (
                    f"https://{self.username}.ngrok-free.app/integrations/teams/webhook"
                )

        # Template variables for configuration
        config_vars = {
            "username": self.username,
            "timestamp": datetime.now().strftime("%Y%m%d-%H%M%S"),
            "webhook_url": webhook_url,
        }

        logger.info(f"Using webhook URL: {webhook_url}")

        # Step 1: Create Azure bot resources
        logger.info("📋 Step 1: Creating Azure bot resources...")
        bot_result = self.create_bot_resources(config_vars)

        # Step 2: Create Teams app package
        logger.info("📱 Step 2: Creating Teams app package...")
        package_path = self.create_teams_app_package(bot_result)

        # Step 3: Create environment configuration
        logger.info("⚙️ Step 3: Creating environment configuration...")
        env_path = self.create_environment_file(bot_result)

        # Summary
        setup_result = {
            "username": self.username,
            "bot_name": bot_result["bot_name"],
            "app_id": bot_result["app_registration"]["app_id"],
            "resource_group": bot_result["resource_group"],
            "webhook_url": webhook_url,
            "teams_package": package_path,
            "environment_file": env_path,
            "output_directory": str(self.output_dir),
        }

        # Save setup summary
        summary_path = self.output_dir / "setup_summary.yaml"
        with open(summary_path, "w") as f:
            yaml.dump(setup_result, f, default_flow_style=False)

        logger.info("✅ Complete bot setup finished successfully!")
        self._print_setup_summary(setup_result)

        return setup_result

    def create_app_only(
        self, bot_name: str = None, app_id: str = None, scenario_name: str = None
    ) -> Dict:
        """Create only Teams app package for existing bot"""
        logger.info(f"🔄 Creating app-only package for user: {self.username}")

        if scenario_name is None:
            scenario_name = f"{self.username}-app-only"

        # Try to find existing bot information
        if not app_id:
            # Check for existing setup results
            results_path = self.output_dir / "bot_creation_results.yaml"
            if results_path.exists():
                with open(results_path, "r") as f:
                    results = yaml.safe_load(f)
                    created_bots = results.get("created_bots", [])
                    if created_bots:
                        app_id = created_bots[0]["app_registration"]["app_id"]
                        bot_name = created_bots[0]["bot_name"]
                        logger.info(f"Found existing bot info: {bot_name} ({app_id})")

        if not app_id:
            # Try to get from environment
            app_id = os.environ.get("DATAHUB_TEAMS_APP_ID")
            if app_id:
                logger.info(f"Using app ID from environment: {app_id}")

        if not app_id:
            raise ValueError(
                "No app ID found. Provide --app-id or ensure bot was created previously."
            )

        logger.info(f"Creating Teams app package with App ID: {app_id}")

        # Generate Teams app manifest package (production mode - uses bot_id as app_id)
        timestamp = datetime.now().strftime("%b %d %H:%M")
        custom_app_name = f"DataHub Dev {self.username} {timestamp}"

        package_path = self.manifest_generator.generate_manifest_package(
            scenario_name=scenario_name,
            bot_id=app_id,
            scopes=["personal", "team", "groupchat"],
            app_name=custom_app_name,
            test_mode=False,  # Production mode
        )

        # Copy package to our output directory
        package_file = Path(package_path)
        dest_path = self.output_dir / package_file.name

        import shutil

        shutil.copy2(package_path, dest_path)

        result = {
            "username": self.username,
            "bot_name": bot_name or f"bot-{self.username}",
            "app_id": app_id,
            "teams_package": str(dest_path),
            "scenario_name": scenario_name,
            "app_only": True,
        }

        logger.info(f"✅ App-only package created: {dest_path}")
        self._print_app_only_summary(result)

        return result

    def _print_app_only_summary(self, result: Dict):
        """Print app-only creation summary"""
        print("\n" + "=" * 60)
        print(f"📱 Teams App Package Created for {result['username']}")
        print("=" * 60)
        print(f"App ID: {result['app_id']}")
        print(f"Package: {Path(result['teams_package']).name}")
        print(f"Scenario: {result['scenario_name']}")
        print("\n📋 Next Steps:")
        print("1. Upload the package to Microsoft Teams:")
        print("   • Go to Teams → Apps → Manage your apps → Upload an app")
        print(f"   • Upload: {result['teams_package']}")
        print("2. Install and test the app")
        print("\n💡 Note: This uses existing Azure bot resources")

    def _print_setup_summary(self, setup_result: Dict):
        """Print setup summary and next steps"""
        print("\n" + "=" * 60)
        print(f"🎉 DataHub Teams Bot Setup Complete for {self.username}")
        print("=" * 60)
        print(f"Bot Name: {setup_result['bot_name']}")
        print(f"App ID: {setup_result['app_id']}")
        print(f"Resource Group: {setup_result['resource_group']}")
        print(f"Webhook URL: {setup_result['webhook_url']}")
        print(f"Teams Package: {Path(setup_result['teams_package']).name}")
        print(f"Output Directory: {setup_result['output_directory']}")
        print("\n📋 Next Steps:")
        print("1. Source the environment file:")
        print(f"   source {setup_result['environment_file']}")
        print("\n2. Start DataHub integrations service:")
        print("   cd datahub-integrations-service")
        print(
            "   uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 --reload"
        )
        print("\n3. Install Teams app:")
        print("   • Go to Microsoft Teams → Apps → Manage your apps → Upload an app")
        print(f"   • Upload: {setup_result['teams_package']}")
        print("\n4. Test the bot:")
        print("   • Search for your app in Teams")
        print("   • Add it to a chat or team")
        print("   • Send a test message")
        print("\n⚠️ Important:")
        print("   • Keep your client secret secure")
        print("   • Update webhook URL when ngrok tunnel changes")
        print(
            "   • Delete resources when done: az bot delete --name <bot-name> --resource-group <rg>"
        )

    def cleanup_resources(
        self, bot_name: str = None, resource_group: str = None
    ) -> bool:
        """Clean up created Azure resources"""
        if not bot_name or not resource_group:
            # Try to load from setup summary
            summary_path = self.output_dir / "setup_summary.yaml"
            if summary_path.exists():
                with open(summary_path, "r") as f:
                    summary = yaml.safe_load(f)
                    bot_name = bot_name or summary.get("bot_name")
                    resource_group = resource_group or summary.get("resource_group")

        if not bot_name or not resource_group:
            logger.error("Cannot cleanup: bot_name and resource_group required")
            return False

        logger.info(f"Cleaning up resources for bot: {bot_name}")

        # Load bot results to get app_id
        results_path = self.output_dir / "bot_creation_results.yaml"
        if results_path.exists():
            with open(results_path, "r") as f:
                results = yaml.safe_load(f)
                created_bots = results.get("created_bots", [])
                if created_bots:
                    app_id = created_bots[0]["app_registration"]["app_id"]
                    bot_creator = AzureBotCreator()
                    return bot_creator.delete_bot(bot_name, resource_group, app_id)

        logger.error("Could not find bot creation results for cleanup")
        return False


def main():
    """Main CLI interface"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Setup DataHub Teams bot for development"
    )
    parser.add_argument("username", help="Developer username/identifier")

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--full-setup",
        action="store_true",
        default=True,
        help="Create complete setup (bot + app) - default",
    )
    mode_group.add_argument(
        "--app-only",
        action="store_true",
        help="Create only Teams app package for existing bot",
    )
    mode_group.add_argument(
        "--cleanup", action="store_true", help="Clean up created Azure resources"
    )
    mode_group.add_argument(
        "--dry-run", action="store_true", help="Show what would be created"
    )

    # Configuration options
    parser.add_argument(
        "--webhook-url",
        help="Custom webhook URL (default: https://{username}.ngrok-free.app/integrations/teams/webhook)",
    )
    parser.add_argument("--ngrok-subdomain", help="ngrok subdomain to use")
    parser.add_argument("--config", help="Custom bot configuration YAML file")
    parser.add_argument("--subscription-id", help="Azure subscription ID")

    # App-only specific options
    parser.add_argument("--app-id", help="Azure App ID for app-only mode")
    parser.add_argument("--bot-name", help="Bot name for reference")
    parser.add_argument("--scenario-name", help="Scenario name for Teams app package")

    args = parser.parse_args()

    # Set default mode if none specified
    if not any([args.app_only, args.cleanup, args.dry_run]):
        args.full_setup = True

    # Validate username
    if not re.match(r"^[a-zA-Z0-9\-_]+$", args.username):
        logger.error(
            "Username must contain only letters, numbers, hyphens, and underscores"
        )
        return 1

    try:
        setup = DevBotSetup(args.username, args.config)

        if args.cleanup:
            success = setup.cleanup_resources(
                bot_name=args.bot_name, resource_group=f"datahub-teams-{args.username}"
            )
            if success:
                logger.info("✅ Cleanup completed successfully")
            else:
                logger.error("❌ Cleanup failed")
                return 1
            return

        if args.dry_run:
            logger.info(f"Dry run mode - would create bot setup for: {args.username}")
            webhook_url = (
                args.webhook_url
                or f"https://{args.username}.ngrok-free.app/integrations/teams/webhook"
            )
            logger.info(f"Would use webhook URL: {webhook_url}")
            return

        if args.app_only:
            setup.create_app_only(
                bot_name=args.bot_name,
                app_id=args.app_id,
                scenario_name=args.scenario_name,
            )
            logger.info("📱 App-only creation completed successfully!")
        else:
            # Full setup mode
            config_vars = {}
            if args.subscription_id:
                config_vars["subscription_id"] = args.subscription_id

            setup.create_complete_setup(
                webhook_url=args.webhook_url, ngrok_subdomain=args.ngrok_subdomain
            )
            logger.info("🎉 Complete bot setup finished successfully!")

    except KeyboardInterrupt:
        logger.info("Setup cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main() or 0)
