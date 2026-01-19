"""
DataHub Token Generator - Standalone module for generating DataHub access tokens.

This module can generate DataHub access tokens by:
1. Retrieving admin password from AWS Parameter Store
2. Logging in to DataHub with admin credentials
3. Generating a personal access token via GraphQL API
4. Caching tokens locally with expiry tracking

Can be used independently or integrated into other tools.
"""

import datetime
import json
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

import boto3
import requests
from loguru import logger


class TokenValidity(str, Enum):
    """Token validity duration options."""

    ONE_HOUR = "ONE_HOUR"
    ONE_DAY = "ONE_DAY"
    ONE_MONTH = "ONE_MONTH"
    THREE_MONTHS = "THREE_MONTHS"


@dataclass
class TokenInfo:
    """Information about a cached token."""

    name: str
    token: str
    frontend_url: str
    created_at: str
    expires_at: str


class TokenGenerator:
    """Generator for DataHub access tokens."""

    def __init__(self, cache_path: Optional[Path] = None):
        """
        Initialize token generator.

        Args:
            cache_path: Path to token cache file (default: ~/.datahub_token_cache.json)
        """
        self.cache_path = cache_path or Path.home() / ".datahub_token_cache.json"
        self._ensure_cache_file()

    def _ensure_cache_file(self) -> None:
        """Ensure cache file exists with proper permissions."""
        if not self.cache_path.exists():
            self.cache_path.write_text("{}")
            # Set restrictive permissions (owner read/write only)
            self.cache_path.chmod(0o600)

    def _load_cache(self) -> dict:
        """Load token cache from disk."""
        try:
            return json.loads(self.cache_path.read_text())
        except Exception as e:
            logger.warning(f"Failed to load token cache: {e}")
            return {}

    def _save_cache(self, cache: dict) -> None:
        """Save token cache to disk."""
        try:
            self.cache_path.write_text(json.dumps(cache, indent=2))
            self.cache_path.chmod(0o600)
        except Exception as e:
            logger.error(f"Failed to save token cache: {e}")

    def get_cached_token(self, frontend_url: str) -> Optional[TokenInfo]:
        """
        Get cached token for a frontend URL if still valid.

        Args:
            frontend_url: DataHub frontend URL

        Returns:
            TokenInfo if valid token exists, None otherwise
        """
        cache = self._load_cache()
        token_data = cache.get(frontend_url)

        if not token_data:
            return None

        try:
            expires_at = datetime.datetime.fromisoformat(token_data["expires_at"])
            now = datetime.datetime.now(tz=datetime.timezone.utc)

            # Token is valid if it expires more than 15 minutes from now
            if expires_at > now + datetime.timedelta(minutes=15):
                logger.info(
                    f"Using cached token for {frontend_url} (expires in {(expires_at - now).total_seconds() / 60:.0f} minutes)"
                )
                return TokenInfo(**token_data)
            else:
                logger.debug(f"Cached token for {frontend_url} is expired or expiring soon")
                return None
        except Exception as e:
            logger.warning(f"Failed to parse cached token: {e}")
            return None

    def _cache_token(self, token_info: TokenInfo) -> None:
        """Cache token to disk."""
        cache = self._load_cache()
        cache[token_info.frontend_url] = {
            "name": token_info.name,
            "token": token_info.token,
            "frontend_url": token_info.frontend_url,
            "created_at": token_info.created_at,
            "expires_at": token_info.expires_at,
        }
        self._save_cache(cache)
        logger.debug(f"Cached token for {token_info.frontend_url}")

    def get_admin_password_from_aws(
        self, namespace: str, region: str, cluster: str, profile: Optional[str] = None
    ) -> Optional[str]:
        """
        Retrieve admin password from AWS Parameter Store.

        Tries multiple path patterns to handle different cluster naming conventions:
        - Staging clusters use short names (usw2-staging)
        - Prod clusters use full names (usw2-saas-01-prod)

        Args:
            namespace: Kubernetes namespace (e.g., "7c3c6c7b85-dev01")
            region: AWS region (e.g., "us-west-2")
            cluster: Cluster name (e.g., "usw2-saas-01-staging" or "usw2-staging")
            profile: Optional AWS profile name (uses AWS_PROFILE env or default if not provided)

        Returns:
            Admin password if found, None otherwise
        """
        # Smart fallback: profile param → AWS_PROFILE env → default credentials
        profile_to_use = profile or os.environ.get("AWS_PROFILE")
        if profile_to_use:
            logger.debug(f"Using AWS profile: {profile_to_use}")
            session = boto3.session.Session(profile_name=profile_to_use)
        else:
            logger.debug("Using default AWS credentials chain")
            session = boto3.session.Session()

        ssm_client = session.client(service_name="ssm", region_name=region)

        # Generate possible paths based on cluster type
        # - SaaS clusters: Try both full (usw2-saas-01-prod) and short (usw2-prod) names
        # - Trial clusters: Use full name only (usw2-trials-01-dmz, euc1-trials-01-dmz)
        # - Legacy/demo clusters: Some customers use "demo" instead of cluster name
        paths_to_try = []

        # Always try the provided cluster name first
        paths_to_try.append(f"/{namespace}/{cluster}/datahub/password")

        # If cluster contains "saas", also try shortened version
        if "saas" in cluster:
            # Convert usw2-saas-01-prod -> usw2-prod
            parts = cluster.split("-")
            if len(parts) >= 4:  # e.g., ['usw2', 'saas', '01', 'prod']
                short_cluster = f"{parts[0]}-{parts[-1]}"  # usw2-prod
                paths_to_try.append(f"/{namespace}/{short_cluster}/datahub/password")
        # Trial clusters use full cluster name in Parameter Store (no shortened version needed)
        # Example: /4f9bdc1186-loved-raven-6a50a05/usw2-trials-01-dmz/datahub/password

        # Fallback: Try "demo" pattern (used by some legacy/demo customers)
        # Example: /18ctce7lp6-chime/demo/datahub/password
        paths_to_try.append(f"/{namespace}/demo/datahub/password")

        # Try each path pattern
        last_error = None
        for secret_path in paths_to_try:
            try:
                logger.debug(
                    f"Trying to retrieve password from AWS Parameter Store: {secret_path} in {region}"
                )

                response = ssm_client.get_parameter(Name=secret_path, WithDecryption=True)
                password = response["Parameter"]["Value"]

                logger.info(f"✓ Retrieved admin password from AWS Parameter Store: {secret_path}")
                return password

            except ssm_client.exceptions.ParameterNotFound:
                logger.debug(f"Parameter not found: {secret_path}")
                continue
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error accessing {secret_path}: {e}")
                last_error = e

                # If it's an SSO token error, raise it immediately so UI can handle
                if "Token has expired" in error_msg or (
                    "sso" in error_msg.lower() and "refresh failed" in error_msg.lower()
                ):
                    raise RuntimeError(f"AWS SSO token expired: {error_msg}") from e

                continue

        # If we get here, none of the paths worked
        logger.error(f"Failed to get password from AWS Parameter Store. Tried paths: {paths_to_try}")
        if last_error:
            raise RuntimeError(
                f"Failed to get password from AWS Parameter Store. Last error: {last_error}"
            ) from last_error
        return None

    def login_to_datahub(
        self, frontend_url: str, username: str, password: str
    ) -> Optional[requests.Session]:
        """
        Login to DataHub and get authenticated session.

        Args:
            frontend_url: DataHub frontend URL (e.g., "https://dev01.acryl.io")
            username: Username (usually "admin")
            password: Password

        Returns:
            Authenticated session if successful, None otherwise
        """
        try:
            session = requests.Session()
            headers = {"Content-Type": "application/json"}
            data = json.dumps({"username": username, "password": password})

            logger.debug(f"Logging in to {frontend_url} as {username}")
            response = session.post(
                f"{frontend_url}/logIn", headers=headers, data=data, timeout=10
            )
            response.raise_for_status()

            logger.info(f"✓ Logged in to DataHub as {username}")
            return session

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to login to DataHub: {e}")
            return None

    def generate_token_via_api(
        self, frontend_url: str, session: requests.Session, validity: TokenValidity
    ) -> Optional[str]:
        """
        Generate access token via DataHub GraphQL API.

        Args:
            frontend_url: DataHub frontend URL
            session: Authenticated session
            validity: Token validity duration

        Returns:
            Access token if successful, None otherwise
        """
        try:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            timestamp = now.astimezone().isoformat()
            token_name = f"Observability Tool Token - {timestamp}"

            query = {
                "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
                    createAccessToken(input: $input) {
                        accessToken
                    }
                }""",
                "variables": {
                    "input": {
                        "type": "PERSONAL",
                        "actorUrn": "urn:li:corpuser:admin",
                        "duration": validity.value,
                        "name": token_name,
                    }
                },
            }

            logger.debug(f"Generating {validity.value} token via GraphQL API")
            response = session.post(f"{frontend_url}/api/v2/graphql", json=query, timeout=10)
            response.raise_for_status()

            token = (
                response.json()
                .get("data", {})
                .get("createAccessToken", {})
                .get("accessToken")
            )

            if token:
                logger.info(f"✓ Generated DataHub token (validity: {validity.value})")
                return token
            else:
                logger.error("Token generation returned empty response")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to generate token: {e}")
            return None

    def generate_token(
        self,
        frontend_url: str,
        namespace: str,
        region: str,
        cluster: str,
        validity: TokenValidity = TokenValidity.ONE_DAY,
        force_regenerate: bool = False,
        profile: Optional[str] = None,
    ) -> Optional[str]:
        """
        Generate DataHub token (or return cached if valid).

        Args:
            frontend_url: DataHub frontend URL (e.g., "https://dev01.acryl.io")
            namespace: Kubernetes namespace (e.g., "7c3c6c7b85-dev01")
            region: AWS region (e.g., "us-west-2")
            cluster: Cluster name (e.g., "usw2-staging")
            validity: Token validity duration
            force_regenerate: Force generation even if cached token exists
            profile: Optional AWS profile name (uses AWS_PROFILE env or default if not provided)

        Returns:
            Access token if successful, None otherwise
        """
        # Check cache first (unless force regenerate)
        if not force_regenerate:
            cached_token = self.get_cached_token(frontend_url)
            if cached_token:
                return cached_token.token

        # Get admin password from AWS Parameter Store
        password = self.get_admin_password_from_aws(namespace, region, cluster, profile)
        if not password:
            return None

        # Login to DataHub
        session = self.login_to_datahub(frontend_url, username="admin", password=password)
        if not session:
            return None

        # Generate token via API
        token = self.generate_token_via_api(frontend_url, session, validity)
        if not token:
            return None

        # Cache token
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        if validity == TokenValidity.ONE_HOUR:
            expiry_time = now + datetime.timedelta(hours=1)
        elif validity == TokenValidity.ONE_DAY:
            expiry_time = now + datetime.timedelta(days=1)
        elif validity == TokenValidity.ONE_MONTH:
            expiry_time = now + datetime.timedelta(days=30)
        elif validity == TokenValidity.THREE_MONTHS:
            expiry_time = now + datetime.timedelta(days=90)
        else:
            expiry_time = now + datetime.timedelta(days=1)

        token_info = TokenInfo(
            name=f"Observability Tool Token - {now.isoformat()}",
            token=token,
            frontend_url=frontend_url,
            created_at=now.isoformat(),
            expires_at=expiry_time.isoformat(),
        )
        self._cache_token(token_info)

        return token


def generate_token_for_namespace(
    frontend_url: str,
    namespace: str,
    region: str = "us-west-2",
    cluster: str = "usw2-staging",
    validity: TokenValidity = TokenValidity.ONE_DAY,
) -> Optional[str]:
    """
    Convenience function to generate token for a namespace.

    Args:
        frontend_url: DataHub frontend URL
        namespace: Kubernetes namespace
        region: AWS region (default: us-west-2)
        cluster: Cluster name (default: usw2-staging)
        validity: Token validity duration

    Returns:
        Access token if successful, None otherwise
    """
    generator = TokenGenerator()
    return generator.generate_token(
        frontend_url=frontend_url,
        namespace=namespace,
        region=region,
        cluster=cluster,
        validity=validity,
    )
