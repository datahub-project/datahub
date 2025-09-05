#!/usr/bin/env python3
"""
Azure Bot Resource Creator

Creates Azure Bot Service resources programmatically using Azure CLI.
Supports YAML configuration for batch creation and management.
"""

import json
import os
import subprocess
from typing import Dict, List, Optional

import yaml
from loguru import logger


class AzureBotCreator:
    """Creates and manages Azure Bot Service resources"""

    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file
        self.config = self._load_config() if config_file else {}

    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_file, "r") as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded configuration from {self.config_file}")
                return config
        except Exception as e:
            logger.error(f"Failed to load config file {self.config_file}: {e}")
            raise

    def check_azure_cli(self, subscription_id: str = None) -> bool:
        """Check if Azure CLI is installed and logged in"""
        try:
            # Check if Azure CLI is installed
            result = subprocess.run(
                ["az", "--version"], capture_output=True, text=True, check=False
            )
            if result.returncode != 0:
                logger.error(
                    "Azure CLI not found. Please install Azure CLI: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
                )
                return False

            # Check if logged in
            result = subprocess.run(
                ["az", "account", "show"], capture_output=True, text=True, check=False
            )
            if result.returncode != 0:
                logger.error("Not logged into Azure CLI. Please run: az login")
                return False

            account_info = json.loads(result.stdout)
            logger.info(
                f"Logged into Azure as: {account_info.get('user', {}).get('name', 'Unknown')}"
            )
            logger.info(f"Active subscription: {account_info.get('name', 'Unknown')}")

            # Set subscription if specified
            if subscription_id and account_info.get("id") != subscription_id:
                logger.info(f"Setting active subscription to: {subscription_id}")
                result = subprocess.run(
                    ["az", "account", "set", "--subscription", subscription_id],
                    capture_output=True,
                    text=True,
                    check=False,
                )

                if result.returncode != 0:
                    logger.error(f"Failed to set subscription: {result.stderr}")
                    return False

                logger.info(f"✅ Active subscription set to: {subscription_id}")

            return True

        except Exception as e:
            logger.error(f"Error checking Azure CLI: {e}")
            return False

    def check_existing_bot(self, bot_name: str, resource_group: str) -> Optional[Dict]:
        """Check if bot resource already exists and return its information"""
        try:
            result = subprocess.run(
                [
                    "az",
                    "bot",
                    "show",
                    "--resource-group",
                    resource_group,
                    "--name",
                    bot_name,
                    "--output",
                    "json",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                bot_info = json.loads(result.stdout)
                logger.info(f"Found existing bot resource: {bot_name}")
                return {
                    "name": bot_name,
                    "resource_group": resource_group,
                    "app_id": bot_info.get("properties", {}).get("msaAppId", ""),
                    "resource_id": bot_info.get("id", ""),
                    "endpoint": bot_info.get("properties", {}).get("endpoint", ""),
                }
            else:
                logger.info(f"Bot resource {bot_name} does not exist")
                return None

        except Exception as e:
            logger.error(f"Error checking existing bot: {e}")
            return None

    def check_existing_app_registration(self, app_id: str) -> Optional[Dict]:
        """Check if app registration exists and return its information"""
        try:
            result = subprocess.run(
                ["az", "ad", "app", "show", "--id", app_id, "--output", "json"],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                app_info = json.loads(result.stdout)
                logger.info(f"Found existing app registration: {app_id}")
                return {
                    "app_id": app_id,
                    "object_id": app_info.get("id", ""),
                    "display_name": app_info.get("displayName", ""),
                    "client_secret": None,  # Cannot retrieve existing secrets
                }
            else:
                logger.info(f"App registration {app_id} does not exist")
                return None

        except Exception as e:
            logger.error(f"Error checking existing app registration: {e}")
            return None

    def _add_bot_permissions(self, app_id: str):
        """Add required Microsoft Graph permissions for Teams bot functionality"""
        logger.info(f"Adding Microsoft Graph permissions to app: {app_id}")

        # Required permissions for Teams bots
        permissions = [
            "e1fe6dd8-ba31-4d61-89e7-88639da4683d",  # User.Read
            "14dad69e-099b-42c9-810b-d002981feec1",  # profile
            "37f7f235-527c-4136-accd-4a02d197296e",  # openid
        ]

        for permission_id in permissions:
            try:
                subprocess.run(
                    [
                        "az",
                        "ad",
                        "app",
                        "permission",
                        "add",
                        "--id",
                        app_id,
                        "--api",
                        "00000003-0000-0000-c000-000000000000",  # Microsoft Graph
                        "--api-permissions",
                        f"{permission_id}=Scope",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            except Exception as e:
                logger.warning(f"Could not add permission {permission_id}: {e}")

        # Grant admin consent
        try:
            subprocess.run(
                ["az", "ad", "app", "permission", "admin-consent", "--id", app_id],
                capture_output=True,
                text=True,
                check=False,
            )
            logger.info("✅ Microsoft Graph permissions added and consented")
        except Exception as e:
            logger.warning(f"Could not grant admin consent: {e}")

    def _configure_oauth_redirect_uri(self, app_id: str, webhook_url: str):
        """Configure OAuth redirect URI for the app registration"""
        if not webhook_url:
            logger.warning(
                "No webhook URL provided, skipping OAuth redirect URI configuration"
            )
            return

        # Convert webhook URL to OAuth callback URL
        # e.g., https://example.ngrok-free.app/public/teams/webhook -> https://example.ngrok-free.app/public/teams/oauth/callback
        oauth_callback_url = webhook_url.replace("/webhook", "/oauth/callback")

        logger.info(f"Configuring OAuth redirect URI: {oauth_callback_url}")

        try:
            subprocess.run(
                [
                    "az",
                    "ad",
                    "app",
                    "update",
                    "--id",
                    app_id,
                    "--web-redirect-uris",
                    oauth_callback_url,
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            logger.info(f"✅ OAuth redirect URI configured: {oauth_callback_url}")

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to configure OAuth redirect URI: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Error configuring OAuth redirect URI: {e}")
            raise

    def create_app_registration(self, app_name: str, webhook_url: str = "") -> Dict:
        """Create Azure App Registration for the bot"""
        logger.info(f"Creating App Registration: {app_name}")

        try:
            # Check if app already exists
            existing_result = subprocess.run(
                [
                    "az",
                    "ad",
                    "app",
                    "list",
                    "--display-name",
                    app_name,
                    "--output",
                    "json",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            existing_apps = json.loads(existing_result.stdout)

            if existing_apps:
                # Use existing app
                app_info = existing_apps[0]
                app_id = app_info["appId"]
                object_id = app_info["id"]
                logger.info(f"Using existing App Registration - App ID: {app_id}")
            else:
                # Create new app registration
                result = subprocess.run(
                    [
                        "az",
                        "ad",
                        "app",
                        "create",
                        "--display-name",
                        app_name,
                        "--sign-in-audience",
                        "AzureADMyOrg",
                        "--output",
                        "json",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )

                app_info = json.loads(result.stdout)
                app_id = app_info["appId"]
                object_id = app_info["id"]
                logger.info(f"Created App Registration - App ID: {app_id}")

            # Create service principal (skip if already exists)
            sp_result = subprocess.run(
                ["az", "ad", "sp", "create", "--id", app_id, "--output", "none"],
                capture_output=True,
                text=True,
                check=False,
            )

            if sp_result.returncode == 0:
                logger.info(f"Created Service Principal for App ID: {app_id}")
            else:
                logger.info(f"Service Principal already exists for App ID: {app_id}")

            # Create client secret
            secret_result = subprocess.run(
                [
                    "az",
                    "ad",
                    "app",
                    "credential",
                    "reset",
                    "--id",
                    app_id,
                    "--append",
                    "--output",
                    "json",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            secret_info = json.loads(secret_result.stdout)
            client_secret = secret_info["password"]

            logger.info(f"Created client secret for App ID: {app_id}")

            # Add required Microsoft Graph permissions
            self._add_bot_permissions(app_id)

            # Configure OAuth redirect URI
            self._configure_oauth_redirect_uri(app_id, webhook_url)

            return {
                "app_id": app_id,
                "object_id": object_id,
                "client_secret": client_secret,
                "display_name": app_name,
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create App Registration: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Error creating App Registration: {e}")
            raise

    def get_tenant_id(self) -> str:
        """Get current Azure tenant ID"""
        try:
            result = subprocess.run(
                ["az", "account", "show", "--query", "tenantId", "--output", "tsv"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except Exception as e:
            logger.error(f"Failed to get tenant ID: {e}")
            raise

    def create_bot_resource(self, bot_config: Dict) -> Dict:
        """Create Azure Bot Service resource"""
        bot_name = bot_config["name"]
        resource_group = bot_config["resource_group"]
        location = bot_config.get("location", "global")
        webhook_url = bot_config.get("webhook_url", "")
        app_registration = bot_config["app_registration"]

        logger.info(f"Creating Bot Service resource: {bot_name}")

        try:
            # Get tenant ID for SingleTenant bot
            tenant_id = self.get_tenant_id()
            logger.info(f"Using tenant ID: {tenant_id}")

            # Create the bot resource
            result = subprocess.run(
                [
                    "az",
                    "bot",
                    "create",
                    "--resource-group",
                    resource_group,
                    "--name",
                    bot_name,
                    "--app-type",
                    "SingleTenant",
                    "--appid",
                    app_registration["app_id"],
                    "--tenant-id",
                    tenant_id,
                    "--location",
                    location,
                    "--endpoint",
                    webhook_url,
                    "--description",
                    f"DataHub Teams bot for {bot_config.get('owner', 'development')}",
                    "--output",
                    "json",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            bot_info = json.loads(result.stdout)
            logger.info(f"Created Bot Service resource: {bot_name}")

            # Configure Microsoft Teams channel
            subprocess.run(
                [
                    "az",
                    "bot",
                    "msteams",
                    "create",
                    "--resource-group",
                    resource_group,
                    "--name",
                    bot_name,
                    "--output",
                    "none",
                    "--location",
                    location,
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            logger.info(f"Enabled Microsoft Teams channel for bot: {bot_name}")

            return {
                "name": bot_name,
                "resource_group": resource_group,
                "location": location,
                "app_id": app_registration["app_id"],
                "client_secret": app_registration["client_secret"],
                "webhook_url": webhook_url,
                "resource_id": bot_info.get("id", ""),
                "endpoint": bot_info.get("properties", {}).get("endpoint", ""),
            }

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create Bot Service resource: {e.stderr}")
            raise
        except Exception as e:
            logger.error(f"Error creating Bot Service resource: {e}")
            raise

    def create_resource_group_if_needed(
        self, resource_group: str, location: str = "eastus"
    ) -> bool:
        """Create resource group if it doesn't exist"""
        try:
            # Check if resource group exists
            result = subprocess.run(
                ["az", "group", "exists", "--name", resource_group],
                capture_output=True,
                text=True,
                check=True,
            )

            if result.stdout.strip() == "true":
                logger.info(f"Resource group {resource_group} already exists")
                return True

            # Create resource group
            subprocess.run(
                [
                    "az",
                    "group",
                    "create",
                    "--name",
                    resource_group,
                    "--location",
                    location,
                    "--output",
                    "none",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            logger.info(f"Created resource group: {resource_group}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create resource group: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"Error with resource group: {e}")
            return False

    def create_bot_from_config(self, bot_config: Dict) -> Dict:
        """Create complete bot setup from configuration"""
        bot_name = bot_config["name"]
        resource_group = bot_config["resource_group"]
        reuse_existing = bot_config.get("reuse_existing", True)
        create_if_not_exists = bot_config.get("create_if_not_exists", True)

        logger.info(f"Starting bot setup for: {bot_name}")

        # Check for existing bot resource first
        existing_bot = (
            self.check_existing_bot(bot_name, resource_group)
            if reuse_existing
            else None
        )

        if existing_bot:
            logger.info(f"♻️ Reusing existing bot resource: {bot_name}")

            # Check for existing app registration
            app_id = existing_bot["app_id"]
            existing_app = (
                self.check_existing_app_registration(app_id) if app_id else None
            )

            if existing_app:
                logger.info(f"♻️ Reusing existing app registration: {app_id}")

                # For existing apps, we can't retrieve the client secret
                # User will need to create a new secret if needed
                app_registration = existing_app
                if not app_registration.get("client_secret"):
                    logger.warning(
                        f"⚠️ Cannot retrieve existing client secret for {app_id}"
                    )
                    logger.warning(
                        "   You may need to create a new client secret manually"
                    )
                    logger.warning(
                        "   Or provide the existing secret via environment variable"
                    )
            else:
                logger.error(f"❌ Bot exists but app registration {app_id} not found")
                if not create_if_not_exists:
                    raise Exception(
                        f"App registration {app_id} not found and create_if_not_exists=False"
                    )

                # Create new app registration for existing bot (unusual case)
                logger.warning("Creating new app registration for existing bot")
                app_name = f"{bot_name}-app-new"
                app_registration = self.create_app_registration(
                    app_name, bot_config.get("webhook_url", "")
                )

                # Update bot to use new app registration
                logger.warning(
                    "You'll need to manually update the bot's app ID in Azure Portal"
                )

            bot_resource = existing_bot

        else:
            logger.info(f"🆕 No existing bot found for: {bot_name}")

            if not create_if_not_exists:
                raise Exception(
                    f"Bot {bot_name} does not exist and create_if_not_exists=False"
                )

            # Ensure resource group exists
            if not self.create_resource_group_if_needed(
                resource_group, bot_config.get("location", "eastus")
            ):
                raise Exception(
                    f"Failed to create or access resource group: {resource_group}"
                )

            # Create App Registration
            app_name = f"{bot_name}-app"
            app_registration = self.create_app_registration(
                app_name, bot_config.get("webhook_url", "")
            )

            # Add app registration to bot config
            bot_config["app_registration"] = app_registration

            # Create Bot Resource
            bot_resource = self.create_bot_resource(bot_config)

        # Combine all information
        result = {
            "bot_name": bot_name,
            "resource_group": resource_group,
            "app_registration": app_registration,
            "bot_resource": bot_resource,
            "existing_resources": existing_bot is not None,
            "teams_config": {
                "app_id": app_registration["app_id"],
                "bot_id": app_registration["app_id"],
                "client_secret": app_registration.get("client_secret"),
                "webhook_url": bot_config.get("webhook_url", ""),
            },
        }

        if existing_bot:
            logger.info(f"♻️ Successfully reused existing bot: {bot_name}")
        else:
            logger.info(f"✅ Successfully created new bot: {bot_name}")

        logger.info(f"   App ID: {app_registration['app_id']}")
        logger.info(f"   Resource Group: {resource_group}")

        return result

    def create_bots_from_yaml(self) -> List[Dict]:
        """Create all bots defined in YAML configuration"""
        if not self.config:
            raise ValueError("No configuration loaded")

        # Get subscription ID from global config or first bot
        subscription_id = None
        if "global" in self.config:
            subscription_id = self.config["global"].get("subscription_id")

        bots = self.config.get("bots", [])
        if not bots:
            raise ValueError("No bots defined in configuration")

        # Use subscription_id from first bot if not in global config
        if not subscription_id and bots:
            subscription_id = bots[0].get("subscription_id")

        if not self.check_azure_cli(subscription_id):
            raise Exception("Azure CLI not available or not logged in")

        results = []

        for bot_config in bots:
            try:
                result = self.create_bot_from_config(bot_config)
                results.append(result)
            except Exception as e:
                logger.error(
                    f"Failed to create bot {bot_config.get('name', 'unknown')}: {e}"
                )
                if not bot_config.get("continue_on_error", False):
                    raise

        return results

    def update_bot_oauth_redirect_uri(self, app_id: str, webhook_url: str) -> bool:
        """Update OAuth redirect URI for an existing bot's app registration"""
        logger.info(f"Updating OAuth redirect URI for App ID: {app_id}")

        try:
            self._configure_oauth_redirect_uri(app_id, webhook_url)
            logger.info(f"✅ Updated OAuth redirect URI for App ID: {app_id}")
            return True

        except Exception as e:
            logger.error(
                f"Failed to update OAuth redirect URI for App ID {app_id}: {e}"
            )
            return False

    def delete_bot(self, bot_name: str, resource_group: str, app_id: str) -> bool:
        """Delete bot resource and app registration"""
        logger.info(f"Deleting bot: {bot_name}")

        try:
            # Delete bot resource
            subprocess.run(
                [
                    "az",
                    "bot",
                    "delete",
                    "--resource-group",
                    resource_group,
                    "--name",
                    bot_name,
                    "--yes",
                    "--output",
                    "none",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            # Delete app registration
            subprocess.run(
                ["az", "ad", "app", "delete", "--id", app_id, "--output", "none"],
                capture_output=True,
                text=True,
                check=True,
            )

            logger.info(f"✅ Deleted bot: {bot_name}")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to delete bot: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"Error deleting bot: {e}")
            return False


def main():
    """Main CLI interface"""
    import argparse

    parser = argparse.ArgumentParser(description="Create Azure Bot Service resources")
    parser.add_argument("--config", required=True, help="YAML configuration file")
    parser.add_argument("--output", help="Output file for bot information")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without creating",
    )
    parser.add_argument(
        "--update-oauth",
        action="store_true",
        help="Update OAuth redirect URIs for existing bots",
    )

    args = parser.parse_args()

    try:
        creator = AzureBotCreator(args.config)

        if args.dry_run:
            logger.info("Dry run mode - showing configuration:")
            print(yaml.dump(creator.config, default_flow_style=False))
            return

        if args.update_oauth:
            logger.info("Updating OAuth redirect URIs for existing bots...")
            bots = creator.config.get("bots", [])
            success_count = 0

            for bot_config in bots:
                app_id = os.environ.get("DATAHUB_TEAMS_APP_ID") or bot_config.get(
                    "app_id"
                )
                webhook_url = bot_config.get("webhook_url", "")

                if not app_id:
                    logger.error(
                        f"No app_id found for bot {bot_config.get('name')}. Set DATAHUB_TEAMS_APP_ID or add app_id to config."
                    )
                    continue

                if creator.update_bot_oauth_redirect_uri(app_id, webhook_url):
                    success_count += 1

            logger.info(
                f"✅ Successfully updated OAuth redirect URIs for {success_count} bot(s)"
            )
            return

        results = creator.create_bots_from_yaml()

        if args.output:
            with open(args.output, "w") as f:
                yaml.dump({"created_bots": results}, f, default_flow_style=False)
            logger.info(f"Bot information saved to: {args.output}")

        logger.info(f"✅ Successfully created {len(results)} bot(s)")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main() or 0)
