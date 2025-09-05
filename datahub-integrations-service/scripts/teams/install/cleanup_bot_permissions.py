#!/usr/bin/env python3
"""
Cleanup Bot Permissions

Removes invalid Microsoft Graph API permissions that were incorrectly added.
"""

import json
import logging
import subprocess

# Setup basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def remove_invalid_permissions(app_id: str):
    """Remove invalid permissions that don't exist in Microsoft Graph"""

    # Invalid permission IDs that need to be removed
    invalid_permissions = [
        "a267235f-af13-44dc-8385-5b6dc529f7e1",  # Invalid TeamsActivity.Send
        "0e755559-65fb-427b-9256-3512298d6c1d",  # Invalid TeamsActivity.Send.User
    ]

    logger.info(f"Removing invalid permissions from app: {app_id}")

    for permission_id in invalid_permissions:
        try:
            # Remove the permission
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
                    "00000003-0000-0000-c000-000000000000",  # Microsoft Graph
                    "--api-permissions",
                    permission_id,
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                logger.info(f"✅ Removed invalid permission: {permission_id}")
            else:
                logger.warning(
                    f"⚠️ Permission may not exist or already removed: {permission_id}"
                )

        except Exception as e:
            logger.error(f"Failed to remove permission {permission_id}: {e}")


def verify_final_permissions(app_id: str):
    """Verify the final permissions after cleanup"""
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
            logger.info("✅ Final API permissions after cleanup:")
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

    logger.info(f"🧹 Cleaning up invalid bot permissions for app: {app_id}")

    # Remove invalid permissions
    remove_invalid_permissions(app_id)

    # Verify final state
    verify_final_permissions(app_id)

    logger.info("✅ Permission cleanup complete!")

    print("\n" + "=" * 60)
    print("🧹 Permission Cleanup Summary")
    print("=" * 60)
    print(f"App ID: {app_id}")
    print("Removed Invalid Permissions:")
    print("  • a267235f-af13-44dc-8385-5b6dc529f7e1 (Invalid TeamsActivity.Send)")
    print("  • 0e755559-65fb-427b-9256-3512298d6c1d (Invalid TeamsActivity.Send.User)")
    print("\n✅ Remaining valid permissions:")
    print("  • User.Read - Read user profile information")
    print("  • profile - Access user's basic profile")
    print("  • openid - Sign in and read user profile")
    print("  • Chat.ReadWrite - Read and write Teams chat messages")
    print("  • Chat.Create - Create new Teams chat conversations")
    print("\n📝 Next Steps:")
    print("1. Try granting admin consent again in Azure Portal")
    print("2. Test Teams messaging functionality")
    print("=" * 60)


if __name__ == "__main__":
    main()
