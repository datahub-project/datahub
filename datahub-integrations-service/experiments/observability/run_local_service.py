#!/usr/bin/env python3
"""
Run Local Integrations Service - Standalone script for running local service with remote GMS.

This script allows you to run a local integrations service connected to a remote GMS,
useful for:
- Debugging integrations service locally with production/staging data
- Testing changes without deploying
- Fast iteration with real GMS

Usage:
    # With explicit credentials
    python run_local_service.py --gms-url https://dev01.acryl.io/api/gms --token YOUR_TOKEN

    # Auto-discover from kubectl
    python run_local_service.py --namespace my-namespace --auto-discover

    # Use saved connection profile
    python run_local_service.py --profile my-profile
"""

import argparse
import sys
import time
from pathlib import Path

from connection_manager import ConnectionManager
from kubectl_manager import KubectlManager
from local_integrations_manager import (
    IntegrationsServiceConfig,
    LocalIntegrationsManager,
)
from loguru import logger

# Configure logger
logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run local integrations service connected to remote GMS"
    )

    # Connection options
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--profile", help="Load connection from saved profile")
    group.add_argument(
        "--gms-url", help="GMS URL (e.g., https://dev01.acryl.io/api/gms)"
    )
    group.add_argument(
        "--auto-discover",
        action="store_true",
        help="Auto-discover GMS URL and token from kubectl",
    )

    # Auth
    parser.add_argument("--token", help="GMS API token (required if using --gms-url)")
    parser.add_argument(
        "--namespace", help="Kubernetes namespace (required if using --auto-discover)"
    )
    parser.add_argument("--context", help="Kubernetes context (optional)")

    # Service config
    parser.add_argument(
        "--port", type=int, default=9003, help="Local service port (default: 9003)"
    )
    parser.add_argument(
        "--aws-region", default="us-west-2", help="AWS region (default: us-west-2)"
    )
    parser.add_argument("--aws-profile", help="AWS profile name")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Service log level (default: INFO)",
    )

    return parser.parse_args()


def load_from_profile(profile_name: str) -> IntegrationsServiceConfig:
    """Load configuration from saved profile."""
    logger.info(f"Loading profile: {profile_name}")
    manager = ConnectionManager()
    profile = manager.get_profile(profile_name)

    if not profile:
        logger.error(f"Profile '{profile_name}' not found")
        sys.exit(1)

    config = profile.config

    if not config.gms_url or not config.gms_token:
        logger.error("Profile is missing GMS URL or token")
        sys.exit(1)

    return IntegrationsServiceConfig(
        gms_url=config.gms_url,
        gms_token=config.gms_token,
        service_port=config.local_service_port,
        aws_region=config.aws_region,
        aws_profile=config.aws_profile,
    )


def auto_discover_config(
    namespace: str, context: str = None, port: int = 9003
) -> IntegrationsServiceConfig:
    """Auto-discover GMS URL and token from kubectl."""
    logger.info(f"Auto-discovering configuration for namespace: {namespace}")

    try:
        kubectl = KubectlManager()
    except RuntimeError as e:
        logger.error(f"kubectl not available: {e}")
        sys.exit(1)

    # Get GMS URL
    gms_url = kubectl.get_gms_url_from_namespace(namespace, context)
    if not gms_url:
        logger.error(
            f"Could not auto-discover GMS URL for namespace {namespace}. "
            "Please provide --gms-url explicitly."
        )
        sys.exit(1)

    logger.success(f"✓ Discovered GMS URL: {gms_url}")

    # Get token
    frontend_url = gms_url.replace("/api/gms", "")
    token = kubectl.get_datahub_token(namespace, frontend_url, context)

    if not token:
        logger.error(
            "Could not auto-generate token. Make sure AWS credentials are configured. "
            "Alternatively, provide --token explicitly."
        )
        sys.exit(1)

    logger.success("✓ Generated DataHub token")

    return IntegrationsServiceConfig(
        gms_url=gms_url,
        gms_token=token,
        service_port=port,
    )


def main():
    """Main entry point."""
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Local Integrations Service Runner")
    logger.info("=" * 60)

    # Load configuration based on mode
    if args.profile:
        service_config = load_from_profile(args.profile)
        service_config.service_port = args.port  # Override port if specified
    elif args.auto_discover:
        if not args.namespace:
            logger.error("--namespace is required when using --auto-discover")
            sys.exit(1)
        service_config = auto_discover_config(args.namespace, args.context, args.port)
    else:
        # Explicit GMS URL
        if not args.token:
            logger.error("--token is required when using --gms-url")
            sys.exit(1)
        service_config = IntegrationsServiceConfig(
            gms_url=args.gms_url,
            gms_token=args.token,
            service_port=args.port,
            aws_region=args.aws_region,
            aws_profile=args.aws_profile,
            log_level=args.log_level,
        )

    # Initialize manager
    try:
        manager = LocalIntegrationsManager()
    except RuntimeError as e:
        logger.error(f"Failed to initialize manager: {e}")
        sys.exit(1)

    # Start service
    logger.info("")
    logger.info("Starting integrations service with configuration:")
    logger.info(f"  GMS URL: {service_config.gms_url}")
    logger.info(f"  Service Port: {service_config.service_port}")
    logger.info(f"  Log Level: {service_config.log_level}")
    logger.info("")

    try:
        process = manager.start_service(service_config)

        logger.success("=" * 60)
        logger.success("Service Started Successfully!")
        logger.success("=" * 60)
        logger.info(f"  URL: http://localhost:{process.port}")
        logger.info(f"  PID: {process.pid}")
        logger.info(f"  Logs: {process.log_file}")
        logger.info("")
        logger.info("Press Ctrl+C to stop the service")
        logger.info("=" * 60)

        # Keep running until interrupted
        try:
            while True:
                time.sleep(1)

                # Check if process is still alive
                if process.process.poll() is not None:
                    logger.error("Service process died unexpectedly!")
                    logger.info(f"Check logs: {process.log_file}")
                    sys.exit(1)

        except KeyboardInterrupt:
            logger.info("")
            logger.info("Stopping service...")
            manager.stop_service(process)
            logger.success("✓ Service stopped")

    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
