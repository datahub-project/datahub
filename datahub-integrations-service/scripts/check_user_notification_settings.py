#!/usr/bin/env python3
"""
Check User Notification Settings

This script checks what Teams notification settings were saved for the current user.
"""

import json
import os
import sys

# Add the source directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datahub.ingestion.graph.client import get_default_graph


def main():
    print("🔍 Checking User Notification Settings...")
    print()

    # Get graph client with proper authentication
    graph = get_default_graph()

    # Query user notification settings
    try:
        query = """
        query getUserNotificationSettings {
            getUserNotificationSettings {
                sinkTypes
                slackSettings {
                    userHandle
                }
                emailSettings {
                    email
                }
                teamsSettings {
                    userHandle
                    channels
                }
                settings {
                    type
                    value
                    params {
                        key
                        value
                    }
                }
            }
        }
        """

        result = graph.execute_graphql(query)

        print("✅ User notification settings retrieved!")
        print("Raw result:")
        print(json.dumps(result, indent=2))

        if result.get("getUserNotificationSettings"):
            settings = result["getUserNotificationSettings"]

            print("\n📊 Settings Summary:")
            print(f"  - Enabled sinks: {settings.get('sinkTypes', [])}")

            teams_settings = settings.get("teamsSettings")
            if teams_settings:
                print("\n📱 Teams Settings:")
                print(f"  - User Handle: {teams_settings.get('userHandle', 'Not set')}")
                print(f"  - Channels: {teams_settings.get('channels', 'Not set')}")
            else:
                print("\n📱 Teams Settings: Not configured")

            slack_settings = settings.get("slackSettings")
            if slack_settings:
                print("\n💬 Slack Settings:")
                print(f"  - User Handle: {slack_settings.get('userHandle', 'Not set')}")
            else:
                print("\n💬 Slack Settings: Not configured")

            email_settings = settings.get("emailSettings")
            if email_settings:
                print("\n📧 Email Settings:")
                print(f"  - Email: {email_settings.get('email', 'Not set')}")
            else:
                print("\n📧 Email Settings: Not configured")
        else:
            print("❌ No user notification settings found")

    except Exception as e:
        print(f"❌ Error getting user notification settings: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
