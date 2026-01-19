"""
AWS Profile Manager - Handles AWS profile detection and setup.

Provides intelligent AWS profile detection based on:
- Kubernetes cluster context (extracts account ID from ARN)
- ~/.aws/config file parsing (matches sso_account_id)
- Account registry (.aws-accounts.yaml) for setup guidance
"""

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from loguru import logger


@dataclass
class AwsAccountInfo:
    """AWS account metadata from registry."""

    account_id: str
    name: str
    description: str
    sso_start_url: str
    sso_region: str
    default_role: str
    suggested_profile_name: str
    clusters: List[str]


@dataclass
class AwsProfile:
    """AWS profile from ~/.aws/config."""

    name: str
    sso_account_id: Optional[str]
    sso_region: Optional[str]
    sso_start_url: Optional[str]
    region: Optional[str]


@dataclass
class ProfileDetectionResult:
    """Result of profile detection for a cluster context."""

    account_id: Optional[str]
    region: Optional[str]
    matching_profiles: List[str]
    setup_needed: bool
    account_info: Optional[AwsAccountInfo]
    recommended_profile: Optional[str]


class AwsProfileManager:
    """Manages AWS profile detection and setup."""

    def __init__(self):
        self.account_registry = self._load_account_registry()

    def _load_account_registry(self) -> Dict[str, AwsAccountInfo]:
        """
        Load AWS account registry from .aws-accounts.yaml.

        Checks in order:
        1. Repo root: chat-ui-web/.aws-accounts.yaml
        2. User home: ~/.datahub-aws-accounts.yaml

        Returns:
            Dict mapping account_id -> AwsAccountInfo
        """
        registry_paths = [
            Path(__file__).parent.parent / ".aws-accounts.yaml",  # Repo root
            Path.home() / ".datahub-aws-accounts.yaml",  # User home
        ]

        for registry_path in registry_paths:
            if registry_path.exists():
                try:
                    with open(registry_path, "r") as f:
                        data = yaml.safe_load(f)

                    accounts = {}
                    for account_id, info in data.get("aws_accounts", {}).items():
                        accounts[account_id] = AwsAccountInfo(
                            account_id=account_id,
                            name=info["name"],
                            description=info["description"],
                            sso_start_url=info["sso_start_url"],
                            sso_region=info["sso_region"],
                            default_role=info["default_role"],
                            suggested_profile_name=info["suggested_profile_name"],
                            clusters=info.get("clusters", []),
                        )

                    logger.info(
                        f"Loaded AWS account registry from {registry_path}: {len(accounts)} accounts"
                    )
                    return accounts

                except Exception as e:
                    logger.warning(f"Failed to load account registry from {registry_path}: {e}")

        logger.debug("No AWS account registry found")
        return {}

    def extract_account_from_cluster_arn(self, cluster_context: str) -> tuple[Optional[str], Optional[str]]:
        """
        Extract AWS account ID and region from cluster ARN.

        Args:
            cluster_context: Kubernetes context name (may be cluster ARN)

        Returns:
            Tuple of (account_id, region) or (None, None)

        Examples:
            "arn:aws:eks:us-west-2:243536687406:cluster/usw2-trials-01-dmz"
            -> ("243536687406", "us-west-2")
        """
        # Pattern: arn:aws:eks:{region}:{account_id}:cluster/{cluster_name}
        arn_pattern = r"arn:aws:eks:([^:]+):(\d+):cluster/(.+)"
        match = re.search(arn_pattern, cluster_context)

        if match:
            region = match.group(1)
            account_id = match.group(2)
            logger.debug(f"Extracted from ARN: account={account_id}, region={region}")
            return account_id, region

        return None, None

    def get_aws_profiles(self) -> List[AwsProfile]:
        """
        Parse ~/.aws/config and extract all profiles.

        Returns:
            List of AwsProfile objects
        """
        aws_config_path = Path.home() / ".aws" / "config"

        if not aws_config_path.exists():
            logger.warning("~/.aws/config not found")
            return []

        profiles = []
        current_profile = None
        current_config = {}

        try:
            with open(aws_config_path, "r") as f:
                for line in f:
                    line = line.strip()

                    # Profile header: [profile name] or [default]
                    if line.startswith("["):
                        # Save previous profile
                        if current_profile:
                            profiles.append(
                                AwsProfile(
                                    name=current_profile,
                                    sso_account_id=current_config.get("sso_account_id"),
                                    sso_region=current_config.get("sso_region"),
                                    sso_start_url=current_config.get("sso_start_url"),
                                    region=current_config.get("region"),
                                )
                            )

                        # Start new profile
                        profile_match = re.match(r"\[profile\s+(.+)\]|\[(.+)\]", line)
                        if profile_match:
                            current_profile = profile_match.group(1) or profile_match.group(2)
                            current_config = {}

                    # Config key-value: key = value
                    elif "=" in line and current_profile:
                        key, value = line.split("=", 1)
                        current_config[key.strip()] = value.strip()

                # Save last profile
                if current_profile:
                    profiles.append(
                        AwsProfile(
                            name=current_profile,
                            sso_account_id=current_config.get("sso_account_id"),
                            sso_region=current_config.get("sso_region"),
                            sso_start_url=current_config.get("sso_start_url"),
                            region=current_config.get("region"),
                        )
                    )

            logger.debug(f"Parsed {len(profiles)} AWS profiles from config")
            return profiles

        except Exception as e:
            logger.error(f"Failed to parse ~/.aws/config: {e}")
            return []

    def find_profiles_for_account(self, account_id: str) -> List[str]:
        """
        Find AWS profiles that match the given account ID.

        Args:
            account_id: AWS account ID

        Returns:
            List of profile names
        """
        profiles = self.get_aws_profiles()
        matching = [p.name for p in profiles if p.sso_account_id == account_id]

        logger.debug(f"Found {len(matching)} profiles for account {account_id}: {matching}")
        return matching

    def detect_profile_for_context(self, cluster_context: str) -> ProfileDetectionResult:
        """
        Detect AWS profile needed for a kubectl context.

        Args:
            cluster_context: Kubernetes context name

        Returns:
            ProfileDetectionResult with detection results
        """
        # Extract account ID from cluster ARN
        account_id, region = self.extract_account_from_cluster_arn(cluster_context)

        if not account_id:
            logger.warning(f"Could not extract account ID from context: {cluster_context}")
            return ProfileDetectionResult(
                account_id=None,
                region=None,
                matching_profiles=[],
                setup_needed=False,
                account_info=None,
                recommended_profile=None,
            )

        # Find matching profiles
        matching_profiles = self.find_profiles_for_account(account_id)

        # Get account info from registry
        account_info = self.account_registry.get(account_id)

        # Determine recommended profile
        recommended_profile = None
        if matching_profiles:
            # Use first matching profile as default
            recommended_profile = matching_profiles[0]
        elif account_info:
            # Suggest profile name from registry
            recommended_profile = account_info.suggested_profile_name

        return ProfileDetectionResult(
            account_id=account_id,
            region=region,
            matching_profiles=matching_profiles,
            setup_needed=len(matching_profiles) == 0,
            account_info=account_info,
            recommended_profile=recommended_profile,
        )

    def setup_profile(
        self,
        profile_name: str,
        account_id: str,
        sso_start_url: str,
        sso_region: str,
        role_name: str,
        region: str = "us-west-2",
    ) -> tuple[bool, Optional[str]]:
        """
        Set up a new AWS SSO profile using aws configure sso.

        Args:
            profile_name: Name for the new profile
            account_id: AWS account ID
            sso_start_url: SSO start URL
            sso_region: SSO region
            role_name: IAM role name
            region: Default AWS region

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Build aws configure sso command
            cmd = [
                "aws",
                "configure",
                "sso",
                "--profile",
                profile_name,
            ]

            # Use environment variables for non-interactive setup
            env = {
                "AWS_PROFILE": profile_name,
            }

            # Note: aws configure sso is interactive, so we can't fully automate it
            # We'll need to guide users through the interactive process
            logger.info(f"Setting up AWS profile: {profile_name}")
            logger.info(f"Command: {' '.join(cmd)}")

            # For now, return instructions instead of executing
            instructions = f"""
Run this command to set up the profile:

aws configure sso \\
  --profile {profile_name}

Then provide these values when prompted:
• SSO Start URL: {sso_start_url}
• SSO Region: {sso_region}
• Account ID: {account_id}
• Role: {role_name}
• Region: {region}
• Output format: json (or press Enter for default)

After completing the setup, click "Refresh Profiles" to continue.
            """.strip()

            return True, instructions

        except Exception as e:
            logger.error(f"Failed to set up profile: {e}")
            return False, str(e)

    def validate_profile(self, profile_name: str) -> tuple[bool, Optional[str]]:
        """
        Validate that an AWS profile exists and has valid credentials.

        Args:
            profile_name: Profile name to validate

        Returns:
            Tuple of (valid, error_message)
        """
        try:
            # Check if profile exists
            profiles = self.get_aws_profiles()
            if not any(p.name == profile_name for p in profiles):
                return False, f"Profile '{profile_name}' not found in ~/.aws/config"

            # Try to get caller identity
            result = subprocess.run(
                ["aws", "sts", "get-caller-identity", "--profile", profile_name],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                logger.info(f"Profile '{profile_name}' validated successfully")
                return True, None
            else:
                error = result.stderr.strip()
                logger.warning(f"Profile '{profile_name}' validation failed: {error}")
                return False, error

        except subprocess.TimeoutExpired:
            return False, "Validation timeout - AWS CLI took too long to respond"
        except FileNotFoundError:
            return False, "AWS CLI not found. Please install AWS CLI first."
        except Exception as e:
            logger.error(f"Profile validation failed: {e}")
            return False, str(e)
