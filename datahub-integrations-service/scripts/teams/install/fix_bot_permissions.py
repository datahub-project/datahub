#!/usr/bin/env python3
"""
Fix Bot Permissions

Adds the required Microsoft Graph API permissions for Teams bot functionality.
"""

import json
import logging
import subprocess
from pathlib import Path

import yaml

# Setup basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def load_permissions_config():
    """Load target permissions from YAML config file"""
    config_path = Path(__file__).parent / "bot_permissions.yaml"

    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        return None

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        return None


def resolve_permission_names_to_ids():
    """Resolve permission names to GUIDs using Microsoft Graph service principal"""
    try:
        # Get Microsoft Graph service principal
        result = subprocess.run(
            [
                "az",
                "ad",
                "sp",
                "show",
                "--id",
                "00000003-0000-0000-c000-000000000000",
                "--query",
                "{appRoles: appRoles, oauth2PermissionScopes: oauth2PermissionScopes}",
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        graph_sp = json.loads(result.stdout)

        # Build name -> ID mappings
        app_roles = {role["value"]: role["id"] for role in graph_sp.get("appRoles", [])}
        oauth2_scopes = {
            scope["value"]: scope["id"]
            for scope in graph_sp.get("oauth2PermissionScopes", [])
        }

        return {
            "application_permissions": app_roles,
            "delegated_permissions": oauth2_scopes,
        }

    except Exception as e:
        logger.error(f"Failed to resolve permission names: {e}")
        return None


def sync_graph_permissions(app_id: str):
    """Sync Microsoft Graph permissions - remove unwanted, add missing"""

    # Load target permissions from config
    config = load_permissions_config()
    if not config:
        return

    # Resolve permission names to IDs
    permission_mappings = resolve_permission_names_to_ids()
    if not permission_mappings:
        return

    graph_config = config.get("microsoft_graph", {})
    resource_app_id = "00000003-0000-0000-c000-000000000000"  # Microsoft Graph

    # Convert permission names to IDs
    target_delegated = set()
    target_application = set()

    for perm in graph_config.get("delegated_permissions", []):
        perm_name = perm["name"]
        if perm_name in permission_mappings["delegated_permissions"]:
            perm_id = permission_mappings["delegated_permissions"][perm_name]
            target_delegated.add(perm_id)
            logger.info(f"📋 Target delegated permission: {perm_name} -> {perm_id}")
        else:
            logger.warning(f"⚠️ Unknown delegated permission: {perm_name}")

    for perm in graph_config.get("application_permissions", []):
        perm_name = perm["name"]
        if perm_name in permission_mappings["application_permissions"]:
            perm_id = permission_mappings["application_permissions"][perm_name]
            target_application.add(perm_id)
            logger.info(f"📋 Target application permission: {perm_name} -> {perm_id}")
        else:
            logger.warning(f"⚠️ Unknown application permission: {perm_name}")

    logger.info(
        f"Resolved {len(target_delegated)} delegated and {len(target_application)} application permissions"
    )

    logger.info(f"Syncing Microsoft Graph permissions for app: {app_id}")

    # Get current permissions
    try:
        result = subprocess.run(
            [
                "az",
                "ad",
                "app",
                "show",
                "--id",
                app_id,
                "--query",
                "requiredResourceAccess",
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        current_permissions = json.loads(result.stdout)
    except Exception as e:
        logger.error(f"Failed to get current permissions: {e}")
        return

    # Find Microsoft Graph resource
    graph_resource = None
    for resource in current_permissions or []:
        if resource.get("resourceAppId") == resource_app_id:
            graph_resource = resource
            break

    current_delegated = set()
    current_application = set()

    if graph_resource:
        for access in graph_resource.get("resourceAccess", []):
            perm_id = access.get("id")
            perm_type = access.get("type")
            if perm_type == "Scope":
                current_delegated.add(perm_id)
            elif perm_type == "Role":
                current_application.add(perm_id)

    # Calculate diffs

    delegated_to_add = target_delegated - current_delegated
    application_to_add = target_application - current_application

    logger.info(
        f"Current delegated: {len(current_delegated)}, target: {len(target_delegated)}"
    )
    logger.info(
        f"Current application: {len(current_application)}, target: {len(target_application)}"
    )

    # Complete cleanup approach: remove ALL Microsoft Graph permissions, then add only what we need
    if current_delegated or current_application:
        logger.info("🧹 Performing complete cleanup of Microsoft Graph permissions...")

        # Remove all current Microsoft Graph permissions
        for perm_id in current_delegated:
            try:
                result = subprocess.run(
                    [
                        "az",
                        "ad",
                        "app",
                        "permission",
                        "delete",
                        "--id",
                        app_id,
                        "--api",
                        resource_app_id,
                        "--api-permissions",
                        f"{perm_id}=Scope",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode == 0:
                    logger.info(f"🗑️ Removed delegated permission: {perm_id}")
            except Exception as e:
                logger.error(f"Error removing delegated permission {perm_id}: {e}")

        for perm_id in current_application:
            try:
                result = subprocess.run(
                    [
                        "az",
                        "ad",
                        "app",
                        "permission",
                        "delete",
                        "--id",
                        app_id,
                        "--api",
                        resource_app_id,
                        "--api-permissions",
                        f"{perm_id}=Role",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode == 0:
                    logger.info(f"🗑️ Removed application permission: {perm_id}")
            except Exception as e:
                logger.error(f"Error removing application permission {perm_id}: {e}")

        # Now add all target permissions (this ensures no duplicates)
        delegated_to_add = target_delegated
        application_to_add = target_application
    else:
        # No cleanup needed, just add missing permissions
        delegated_to_add = target_delegated - current_delegated
        application_to_add = target_application - current_application

    # Add missing permissions
    for perm_id in delegated_to_add:
        try:
            result = subprocess.run(
                [
                    "az",
                    "ad",
                    "app",
                    "permission",
                    "add",
                    "--id",
                    app_id,
                    "--api",
                    resource_app_id,
                    "--api-permissions",
                    f"{perm_id}=Scope",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                logger.info(f"✅ Added delegated permission: {perm_id}")
            else:
                logger.warning(f"⚠️ Failed to add delegated permission: {perm_id}")
        except Exception as e:
            logger.error(f"Error adding delegated permission {perm_id}: {e}")

    for perm_id in application_to_add:
        try:
            result = subprocess.run(
                [
                    "az",
                    "ad",
                    "app",
                    "permission",
                    "add",
                    "--id",
                    app_id,
                    "--api",
                    resource_app_id,
                    "--api-permissions",
                    f"{perm_id}=Role",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                logger.info(f"✅ Added application permission: {perm_id}")
            else:
                logger.warning(f"⚠️ Failed to add application permission: {perm_id}")
        except Exception as e:
            logger.error(f"Error adding application permission {perm_id}: {e}")

    # Grant admin consent for the permissions
    logger.info("Granting admin consent for permissions...")
    try:
        result = subprocess.run(
            ["az", "ad", "app", "permission", "admin-consent", "--id", app_id],
            capture_output=True,
            text=True,
            check=True,
        )

        logger.info("✅ Admin consent granted for all permissions")

    except subprocess.CalledProcessError as e:
        logger.warning(f"⚠️ Could not grant admin consent automatically: {e.stderr}")
        logger.info("Manual admin consent may be required in Azure Portal")


def verify_permissions(app_id: str):
    """Verify the permissions were added correctly"""
    try:
        result = subprocess.run(
            [
                "az",
                "ad",
                "app",
                "show",
                "--id",
                app_id,
                "--query",
                "requiredResourceAccess",
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        permissions = json.loads(result.stdout)

        if permissions:
            logger.info("✅ Current API permissions:")
            for resource in permissions:
                resource_id = resource.get("resourceAppId")
                if resource_id == "00000003-0000-0000-c000-000000000000":
                    logger.info("  📋 Microsoft Graph:")
                    for access in resource.get("resourceAccess", []):
                        perm_id = access.get("id")
                        perm_type = access.get("type")
                        logger.info(f"    • {perm_id} ({perm_type})")
        else:
            logger.warning("⚠️ No API permissions found")

    except Exception as e:
        logger.error(f"Failed to verify permissions: {e}")


def main():
    app_id = "fcfde16d-63d4-49f4-9e4a-60140562e2cb"

    logger.info(f"🔧 Syncing bot permissions for app: {app_id}")

    # Sync required permissions (remove unwanted, add missing)
    sync_graph_permissions(app_id)

    # Grant admin consent for the permissions
    logger.info("Granting admin consent for permissions...")
    try:
        subprocess.run(
            ["az", "ad", "app", "permission", "admin-consent", "--id", app_id],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("✅ Admin consent granted for all permissions")
    except subprocess.CalledProcessError as e:
        logger.warning(f"⚠️ Could not grant admin consent automatically: {e.stderr}")
        logger.info("Manual admin consent may be required in Azure Portal")

    # Verify permissions were synced
    verify_permissions(app_id)

    logger.info("✅ Bot permissions sync complete!")

    print("\n" + "=" * 60)
    print("🔐 Bot Permissions Summary")
    print("=" * 60)
    print(f"App ID: {app_id}")
    print("Target Permissions:")
    print("  • User.Read - Read user profile information")
    print("  • profile - Access user's basic profile")
    print("  • openid - Sign in and read user profile")
    print("  • Chat.ReadWrite.All - Read and write Teams chat messages")
    print("  • Team.ReadBasic.All - Read basic team information")
    print("  • Channel.ReadBasic.All - Read basic channel information")
    print("\n⚠️ Important Notes:")
    print("1. Removed any permissions not in the target set")
    print("2. Added any missing permissions from the target set")
    print("3. Admin consent may be required for new permissions")
    print("4. Restart the integration service after consent to refresh tokens")
    print("=" * 60)


if __name__ == "__main__":
    main()
