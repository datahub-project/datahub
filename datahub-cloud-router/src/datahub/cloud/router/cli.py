"""
Command-line interface for DataHub Cloud Router.
"""

import argparse
import os
import sys

from .core import MultiTenantRouter
from .db import DatabaseConfig
from .server import DataHubMultiTenantRouter


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="DataHub Cloud Router - Multi-tenant Teams integration router",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run in routing mode (default port 9005, routes to localhost:9003)
  datahub-router

  # Run on custom port
  datahub-router --port 8090

  # Run with custom database configuration
  datahub-router --db-type inmemory

  # Run with custom target URL
  DATAHUB_ROUTER_TARGET_URL=http://datahub.company.com:8080 datahub-router

Environment Variables:
  DATAHUB_ROUTER_TARGET_URL       Default DataHub instance URL (default: http://localhost:9003)
  DATAHUB_ROUTER_DB_TYPE          Database type: sqlite, inmemory (default: sqlite)
  DATAHUB_ROUTER_DB_PATH          Database file path for SQLite (default: .dev/router.db)
  DATAHUB_ROUTER_ADMIN_API_KEY    API key for admin endpoints (if not set, admin endpoints are unprotected)
  DATAHUB_ROUTER_ENABLE_AUTH      Enable authentication for admin endpoints (default: true)
  
  # Webhook Security (Integration-Specific)
  DATAHUB_TEAMS_APP_ID           Microsoft Teams app ID for Bot Framework validation
  DATAHUB_TEAMS_APP_PASSWORD     Microsoft Teams app password for Bot Framework validation  
  DATAHUB_SLACK_SIGNING_SECRET   Slack signing secret for webhook signature validation
  DATAHUB_ROUTER_OAUTH_SECRET    Secret key for OAuth state encryption (auto-generated if not set)
  DATAHUB_ROUTER_DISABLE_WEBHOOK_AUTH  Disable webhook authentication (INSECURE - dev only)
        """,
    )

    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Server host (default: 0.0.0.0)"
    )

    parser.add_argument(
        "--port", type=int, default=9005, help="Server port (default: 9005)"
    )

    parser.add_argument(
        "--db-type",
        type=str,
        choices=["sqlite", "mysql", "inmemory"],
        default=os.getenv("DATAHUB_ROUTER_DB_TYPE", "sqlite"),
        help="Database type (default: sqlite)",
    )

    parser.add_argument(
        "--db-path",
        type=str,
        default=os.getenv("DATAHUB_ROUTER_DB_PATH", ".dev/router.db"),
        help="Database file path for SQLite (default: .dev/router.db)",
    )

    parser.add_argument(
        "--target-url",
        type=str,
        default=os.getenv("DATAHUB_ROUTER_TARGET_URL", "http://localhost:9003"),
        help="Default DataHub instance URL (default: http://localhost:9003)",
    )

    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload on code changes (development mode)",
    )

    parser.add_argument(
        "--disable-admin-auth",
        action="store_true",
        help="Disable authentication for admin endpoints (INSECURE - dev only)",
    )

    parser.add_argument(
        "--disable-webhook-auth",
        action="store_true",
        help="Disable webhook authentication for all integrations (INSECURE - dev only)",
    )

    args = parser.parse_args()

    try:
        # Create database configuration
        db_config = DatabaseConfig(
            type=args.db_type, path=args.db_path if args.db_type == "sqlite" else None
        )

        # Create router instance
        router = MultiTenantRouter(db_config)

        # Override default target URL if specified
        if args.target_url != "http://localhost:9003":
            router.default_target_url = args.target_url

        # Determine auth configuration
        admin_auth_enabled = (
            not args.disable_admin_auth
            and os.getenv("DATAHUB_ROUTER_ENABLE_AUTH", "true").lower() != "false"
        )
        admin_api_key = os.getenv("DATAHUB_ROUTER_ADMIN_API_KEY")

        # Determine webhook auth configuration
        webhook_auth_enabled = (
            not args.disable_webhook_auth
            and os.getenv("DATAHUB_ROUTER_DISABLE_WEBHOOK_AUTH", "false").lower()
            != "true"
        )

        # Integration credentials
        teams_app_id = os.getenv("DATAHUB_TEAMS_APP_ID")
        teams_app_password = os.getenv("DATAHUB_TEAMS_APP_PASSWORD")
        slack_signing_secret = os.getenv("DATAHUB_SLACK_SIGNING_SECRET")
        oauth_secret = os.getenv("DATAHUB_ROUTER_OAUTH_SECRET")

        # Create and run server
        server = DataHubMultiTenantRouter(
            router,
            admin_auth_enabled=admin_auth_enabled,
            admin_api_key=admin_api_key,
            webhook_auth_enabled=webhook_auth_enabled,
            teams_app_id=teams_app_id,
            teams_app_password=teams_app_password,
            slack_signing_secret=slack_signing_secret,
            oauth_secret=oauth_secret,
        )

        if args.reload:
            # Run with reload mode
            import uvicorn

            uvicorn.run(
                server.app,
                host=args.host,
                port=args.port,
                reload=True,
                log_level="info",
            )
        else:
            # Run normally
            server.run_server(host=args.host, port=args.port)

    except KeyboardInterrupt:
        print("\n⏹️  Server stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
