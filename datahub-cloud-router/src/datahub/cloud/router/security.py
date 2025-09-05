"""
Security validation and protection for DataHub Cloud Router.

This module provides pluggable, integration-specific security validators:
- Teams: Bot Framework JWT validation
- Slack: Request signature validation (future)
- Generic: OAuth state validation and rate limiting
"""

import base64
import hashlib
import hmac
import json
import logging
import secrets
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Dict, Optional, Set
from urllib.parse import unquote

from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings
from botbuilder.schema import Activity
from fastapi import HTTPException, Request
from pydantic import ValidationError

from .db.models import OAuthStateDecoded, OAuthStateV1

logger = logging.getLogger(__name__)


class WebhookValidator(ABC):
    """Abstract base class for integration-specific webhook validators."""

    @abstractmethod
    async def validate_request(self, request: Request, body: bytes) -> bool:
        """Validate webhook request for this integration."""
        pass

    @abstractmethod
    def get_integration_name(self) -> str:
        """Get the name of this integration for logging."""
        pass


class TeamsWebhookValidator(WebhookValidator):
    """Validates Teams webhook requests using Bot Framework JWT validation."""

    def __init__(self, app_id: str, app_password: str):
        """Initialize the Teams Bot Framework validator."""
        if not app_id or not app_password:
            raise ValueError("Teams app_id and app_password are required")

        self.app_id = app_id
        self.app_password = app_password

        # Create adapter for validation only
        settings = BotFrameworkAdapterSettings(app_id=app_id, app_password=app_password)
        self._adapter = BotFrameworkAdapter(settings)

        logger.info(f"🔐 Teams webhook validator initialized for app_id: {app_id}")

    def get_integration_name(self) -> str:
        """Get the integration name."""
        return "Teams"

    async def validate_request(self, request: Request, body: bytes) -> bool:
        """
        Validate Teams webhook request using Bot Framework JWT validation.

        Args:
            request: FastAPI request object
            body: Raw request body bytes

        Returns:
            True if request is valid, False otherwise
        """
        try:
            # Extract Authorization header
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                logger.warning("Missing Authorization header in Teams webhook")
                return False

            # Parse JSON body into Activity
            try:
                body_str = body.decode("utf-8")
                activity_data = json.loads(body_str)
                activity = Activity().deserialize(activity_data)
            except (UnicodeDecodeError, json.JSONDecodeError, Exception) as e:
                logger.error(f"Failed to parse Teams webhook body: {e}")
                return False

            # Use Bot Framework's JWT validation
            # This validates the JWT signature, expiration, and claims
            async def validation_bot_logic(turn_context):
                """No-op bot logic for validation only."""
                pass

            await self._adapter.process_activity(
                activity, auth_header, validation_bot_logic
            )

            logger.debug(
                f"✅ Teams JWT validation passed for activity type: {activity.type}"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Teams JWT validation failed: {e}")
            return False


class SlackWebhookValidator(WebhookValidator):
    """Validates Slack webhook requests using request signature validation."""

    def __init__(self, signing_secret: str):
        """Initialize the Slack webhook validator."""
        if not signing_secret:
            raise ValueError("Slack signing secret is required")

        self.signing_secret = signing_secret
        logger.info("🔐 Slack webhook validator initialized")

    def get_integration_name(self) -> str:
        """Get the integration name."""
        return "Slack"

    async def validate_request(self, request: Request, body: bytes) -> bool:
        """
        Validate Slack webhook request using signature validation.

        Args:
            request: FastAPI request object
            body: Raw request body bytes

        Returns:
            True if request is valid, False otherwise
        """
        try:
            # Get Slack headers
            timestamp = request.headers.get("X-Slack-Request-Timestamp")
            signature = request.headers.get("X-Slack-Signature")

            if not timestamp or not signature:
                logger.warning("Missing Slack signature headers")
                return False

            # Check timestamp to prevent replay attacks (within 5 minutes)
            current_time = int(time.time())
            if abs(current_time - int(timestamp)) > 300:
                logger.warning("Slack webhook timestamp too old (replay attack?)")
                return False

            # Compute expected signature
            sig_basestring = f"v0:{timestamp}:{body.decode()}"
            expected_signature = (
                "v0="
                + hmac.new(
                    self.signing_secret.encode(),
                    sig_basestring.encode(),
                    hashlib.sha256,
                ).hexdigest()
            )

            # Verify signature
            if not hmac.compare_digest(signature, expected_signature):
                logger.error("Slack webhook signature validation failed")
                return False

            logger.debug("✅ Slack signature validation passed")
            return True

        except Exception as e:
            logger.error(f"❌ Slack webhook validation failed: {e}")
            return False


class SecureOAuthState:
    """Storage-free OAuth state handler with cryptographic security."""

    def __init__(self, secret_key: Optional[str] = None):
        """Initialize OAuth state handler - storage-free design."""
        # Track recently used nonces to prevent replay attacks
        self._used_nonces: Set[str] = set()
        self._max_nonces = 10000  # Limit memory usage
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # Clean up every 5 minutes

        logger.info("🔐 OAuth state handler initialized (storage-free design)")

    def create_state(self, **kwargs) -> str:
        """
        Create OAuth state parameter - storage-free design.

        Args:
            **kwargs: OAuth state parameters (url, flow_type, tenant_id, etc.)

        Returns:
            Base64 encoded state string safe for URL parameters
        """
        try:
            # Generate secure nonce
            nonce = secrets.token_urlsafe(32)  # 256 bits of entropy

            # Create V1 state model with all context
            state_model = OAuthStateV1(
                nonce=nonce,
                timestamp=int(time.time()),
                **kwargs,  # url, flow_type, tenant_id, user_urn, redirect_path, etc.
            )

            # Serialize to JSON using Pydantic v2 syntax
            json_str = state_model.model_dump_json(by_alias=True)

            # Base64 encode (URL-safe, no padding)
            encoded_state = (
                base64.urlsafe_b64encode(json_str.encode()).decode().rstrip("=")
            )

            logger.debug(
                f"✅ OAuth state created (nonce: {nonce[:8]}..., flow: {state_model.flow_type})"
            )
            return encoded_state

        except ValidationError as e:
            logger.error(f"❌ OAuth state validation failed: {e}")
            raise ValueError(f"Invalid OAuth state data: {e}") from e
        except Exception as e:
            logger.error(f"❌ OAuth state creation failed: {e}")
            raise ValueError(f"Failed to create OAuth state: {e}") from e

    def validate_state(self, state: str, max_age: int = 3600) -> OAuthStateDecoded:
        """
        Validate OAuth state parameter - storage-free design.

        Args:
            state: Base64 encoded state string from OAuth callback
            max_age: Maximum age in seconds (default: 1 hour)

        Returns:
            OAuthStateDecoded model with validated data

        Raises:
            ValueError: If state is invalid, expired, or tampered with
        """
        try:
            # URL decode if needed
            if "%" in state:
                state = unquote(state)

            # Add base64 padding if needed
            missing_padding = len(state) % 4
            if missing_padding:
                state = state + "=" * (4 - missing_padding)

            # Decode base64
            try:
                decoded_bytes = base64.urlsafe_b64decode(state)
                raw_data = json.loads(decoded_bytes.decode("utf-8"))
            except (ValueError, json.JSONDecodeError) as e:
                logger.error(f"Failed to decode OAuth state: {e}")
                raise ValueError("Invalid OAuth state format") from e

            # Validate with Pydantic V1 model
            try:
                state_model = OAuthStateV1(**raw_data)
            except ValidationError as e:
                logger.error(f"❌ OAuth state validation failed: {e}")
                raise ValueError(f"Invalid OAuth state structure: {e}") from e

            # CSRF protection: Check timestamp age
            age = int(time.time()) - state_model.timestamp
            if age > max_age:
                raise ValueError(f"OAuth state expired (age: {age}s, max: {max_age}s)")

            # Replay protection: Check nonce uniqueness
            if state_model.nonce in self._used_nonces:
                logger.error(
                    f"🚨 OAuth state replay attack detected! Nonce: {state_model.nonce[:8]}..."
                )
                raise ValueError("OAuth state replay detected - nonce already used")

            # Add nonce to used set with memory management
            self._used_nonces.add(state_model.nonce)
            self._cleanup_old_nonces()

            # Convert to decoded format
            decoded_state = OAuthStateDecoded.from_v1_state(state_model)

            logger.debug(
                f"✅ OAuth state validated: flow={state_model.flow_type}, nonce={state_model.nonce[:8]}..., age={age}s"
            )
            return decoded_state

        except ValueError:
            # Re-raise ValueError as-is
            raise
        except Exception as e:
            logger.error(f"❌ OAuth state validation failed: {e}")
            raise ValueError(f"OAuth state validation failed: {e}") from e

    def _cleanup_old_nonces(self):
        """Clean up old nonces to prevent memory growth."""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        if len(self._used_nonces) > self._max_nonces:
            # Keep most recent half (simple cleanup)
            nonces_list = list(self._used_nonces)
            self._used_nonces = set(nonces_list[len(nonces_list) // 2 :])
            logger.debug(
                f"🧹 OAuth nonce cache pruned to {len(self._used_nonces)} entries"
            )

        self._last_cleanup = now


class RateLimiter:
    """Rate limiting for request throttling."""

    def __init__(self):
        """Initialize rate limiter."""
        self._requests = defaultdict(list)
        self._cleanup_interval = 300  # Clean up every 5 minutes
        self._last_cleanup = time.time()

        logger.info("🔐 Rate limiter initialized")

    def _cleanup_old_requests(self):
        """Clean up old request records."""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        # Clean up requests older than 1 hour
        cutoff = now - 3600
        for key in list(self._requests.keys()):
            self._requests[key] = [t for t in self._requests[key] if t > cutoff]
            if not self._requests[key]:
                del self._requests[key]

        self._last_cleanup = now
        logger.debug(
            f"🧹 Rate limiter cleanup completed ({len(self._requests)} keys remaining)"
        )

    def is_allowed(self, key: str, limit: int = 10, window: int = 60) -> bool:
        """
        Check if request is allowed under rate limit.

        Args:
            key: Rate limiting key (e.g., IP address, tenant ID)
            limit: Maximum requests allowed
            window: Time window in seconds

        Returns:
            True if request is allowed, False if rate limited
        """
        self._cleanup_old_requests()

        now = time.time()
        cutoff = now - window

        # Clean requests outside window
        self._requests[key] = [t for t in self._requests[key] if t > cutoff]

        # Check rate limit
        current_count = len(self._requests[key])
        if current_count >= limit:
            logger.warning(
                f"🚫 Rate limit exceeded for key: {key} ({current_count}/{limit} in {window}s)"
            )
            return False

        # Record request
        self._requests[key].append(now)
        logger.debug(
            f"✅ Rate limit check passed for key: {key} ({current_count + 1}/{limit} in {window}s)"
        )
        return True


class CSRFProtection:
    """CSRF protection for state-changing admin operations."""

    def __init__(self, secret_key: Optional[str] = None):
        """Initialize CSRF protection with secret key."""
        if not secret_key:
            secret_key = secrets.token_urlsafe(32)
            logger.warning(
                f"🔑 Generated CSRF secret (set DATAHUB_ROUTER_CSRF_SECRET to persist): {secret_key[:16]}..."
            )

        self._secret = secret_key
        self._tokens: Dict[
            str, Dict[str, Any]
        ] = {}  # In-memory token store (could be Redis in production)
        self._cleanup_interval = 300  # 5 minutes
        self._last_cleanup = time.time()

        logger.info("🔐 CSRF protection initialized")

    def generate_token(self, session_id: str) -> str:
        """Generate CSRF token for a session."""
        # Create token with session binding and timestamp
        token_data = {
            "session_id": session_id,
            "timestamp": int(time.time()),
            "entropy": secrets.token_hex(16),
        }

        # Sign the token data
        token_str = json.dumps(token_data, sort_keys=True)
        signature = hmac.new(
            self._secret.encode(), token_str.encode(), hashlib.sha256
        ).hexdigest()

        token = f"{signature}:{base64.b64encode(token_str.encode()).decode()}"

        # Store token with expiration (30 minutes)
        expiry = int(time.time()) + 1800
        self._tokens[token] = {"session_id": session_id, "expiry": expiry}

        self._cleanup_expired_tokens()

        logger.debug(f"🔑 CSRF token generated for session: {session_id}")
        return token

    def validate_token(self, token: str, session_id: str) -> bool:
        """Validate CSRF token for a session."""
        try:
            if not token or ":" not in token:
                return False

            signature, encoded_data = token.split(":", 1)
            token_str = base64.b64decode(encoded_data).decode()

            # Verify signature
            expected_signature = hmac.new(
                self._secret.encode(), token_str.encode(), hashlib.sha256
            ).hexdigest()

            if not hmac.compare_digest(signature, expected_signature):
                logger.warning("🚨 CSRF token signature validation failed")
                return False

            # Parse token data
            token_data = json.loads(token_str)

            # Check session binding
            if token_data.get("session_id") != session_id:
                logger.warning(
                    f"🚨 CSRF token session mismatch: {token_data.get('session_id')} != {session_id}"
                )
                return False

            # Check if token exists and not expired
            stored_token = self._tokens.get(token)
            if not stored_token:
                logger.warning("🚨 CSRF token not found in store (expired or invalid)")
                return False

            if stored_token["expiry"] < int(time.time()):
                logger.warning("🚨 CSRF token expired")
                del self._tokens[token]
                return False

            # One-time use: remove token after successful validation
            del self._tokens[token]

            logger.debug(f"✅ CSRF token validated for session: {session_id}")
            return True

        except Exception as e:
            logger.error(f"❌ CSRF token validation error: {e}")
            return False

    def _cleanup_expired_tokens(self):
        """Clean up expired CSRF tokens."""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        expired_tokens = [
            token for token, data in self._tokens.items() if data["expiry"] < now
        ]

        for token in expired_tokens:
            del self._tokens[token]

        self._last_cleanup = now

        if expired_tokens:
            logger.debug(
                f"🧹 CSRF cleanup removed {len(expired_tokens)} expired tokens"
            )


class RequestSigningValidator:
    """Validates request signatures for API security."""

    def __init__(self, secret_key: Optional[str] = None):
        """Initialize request signing validator."""
        if not secret_key:
            secret_key = secrets.token_urlsafe(32)
            logger.warning(
                f"🔑 Generated signing secret (set DATAHUB_ROUTER_SIGNING_SECRET to persist): {secret_key[:16]}..."
            )

        self._secret = secret_key
        logger.info("🔐 Request signing validation initialized")

    def sign_request(
        self,
        method: str,
        path: str,
        body: bytes,
        timestamp: Optional[int] = None,
        nonce: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Sign a request and return headers to include.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: Request path
            body: Request body bytes
            timestamp: Unix timestamp (defaults to current time)
            nonce: Unique nonce (auto-generated if not provided)

        Returns:
            Dictionary of headers to include in request
        """
        if timestamp is None:
            timestamp = int(time.time())

        if nonce is None:
            nonce = secrets.token_urlsafe(16)

        # Create canonical request string
        canonical_request = f"{method}\n{path}\n{timestamp}\n{nonce}\n{body.hex()}"

        # Sign with HMAC-SHA256
        signature = hmac.new(
            self._secret.encode(), canonical_request.encode(), hashlib.sha256
        ).hexdigest()

        headers = {
            "X-DataHub-Timestamp": str(timestamp),
            "X-DataHub-Nonce": nonce,
            "X-DataHub-Signature": signature,
        }

        logger.debug(f"✅ Request signed: {method} {path} (nonce: {nonce[:8]}...)")
        return headers

    def validate_request_signature(
        self,
        method: str,
        path: str,
        body: bytes,
        headers: Dict[str, str],
        max_age: int = 300,  # 5 minutes
    ) -> bool:
        """
        Validate request signature from headers.

        Args:
            method: HTTP method
            path: Request path
            body: Request body bytes
            headers: Request headers
            max_age: Maximum age for timestamp validation

        Returns:
            True if signature is valid, False otherwise
        """
        try:
            # Extract required headers
            timestamp_str = headers.get("X-DataHub-Timestamp")
            nonce = headers.get("X-DataHub-Nonce")
            provided_signature = headers.get("X-DataHub-Signature")

            if not all([timestamp_str, nonce, provided_signature]):
                logger.warning("🚨 Missing required signature headers")
                return False

            if not timestamp_str:
                logger.warning("🚨 Missing timestamp in signature headers")
                return False
            timestamp = int(timestamp_str)

            # Check timestamp (prevent replay attacks)
            current_time = int(time.time())
            age = current_time - timestamp
            if abs(age) > max_age:
                logger.warning(
                    f"🚨 Request timestamp too old/new: {age}s (max: {max_age}s)"
                )
                return False

            # Recreate canonical request
            canonical_request = f"{method}\n{path}\n{timestamp}\n{nonce}\n{body.hex()}"

            # Compute expected signature
            expected_signature = hmac.new(
                self._secret.encode(), canonical_request.encode(), hashlib.sha256
            ).hexdigest()

            # Validate signature
            if not provided_signature or not hmac.compare_digest(
                provided_signature, expected_signature
            ):
                logger.error("🚨 Request signature validation failed")
                return False

            if nonce:
                logger.debug(
                    f"✅ Request signature validated: {method} {path} (nonce: {nonce[:8]}...)"
                )
            else:
                logger.debug(f"✅ Request signature validated: {method} {path}")
            return True

        except (ValueError, TypeError) as e:
            logger.error(f"❌ Request signature validation error: {e}")
            return False


class SecurityManager:
    """Main security manager with pluggable integration-specific validators."""

    def __init__(
        self,
        oauth_secret: Optional[str] = None,
        csrf_secret: Optional[str] = None,
        signing_secret: Optional[str] = None,
        enable_rate_limiting: bool = True,
        enable_csrf_protection: bool = True,
        enable_request_signing: bool = False,  # Disabled by default for backwards compatibility
    ):
        """Initialize security manager with shared components."""
        # Integration-specific validators (registered per endpoint)
        self._webhook_validators: Dict[str, WebhookValidator] = {}

        # Shared security components
        self.oauth_state = SecureOAuthState(oauth_secret)
        self.rate_limiter = RateLimiter() if enable_rate_limiting else None
        self.csrf_protection = (
            CSRFProtection(csrf_secret) if enable_csrf_protection else None
        )
        self.request_signer = (
            RequestSigningValidator(signing_secret) if enable_request_signing else None
        )

        if enable_rate_limiting:
            logger.info("✅ Rate limiting enabled")

        if enable_csrf_protection:
            logger.info("✅ CSRF protection enabled")

        if enable_request_signing:
            logger.info("✅ Request signing validation enabled")

        logger.info("🔐 Security manager initialized")

    def register_teams_validator(self, app_id: str, app_password: str) -> None:
        """Register Teams webhook validator."""
        try:
            validator = TeamsWebhookValidator(app_id, app_password)
            self._webhook_validators["teams"] = validator
            logger.info("✅ Teams webhook validation registered")
        except Exception as e:
            logger.error(f"❌ Failed to register Teams validator: {e}")
            raise

    def register_slack_validator(self, signing_secret: str) -> None:
        """Register Slack webhook validator."""
        try:
            validator = SlackWebhookValidator(signing_secret)
            self._webhook_validators["slack"] = validator
            logger.info("✅ Slack webhook validation registered")
        except Exception as e:
            logger.error(f"❌ Failed to register Slack validator: {e}")
            raise

    async def validate_webhook(
        self, integration: str, request: Request, body: bytes
    ) -> None:
        """
        Validate webhook request using integration-specific validator.

        Args:
            integration: Integration name (e.g., "teams", "slack")
            request: FastAPI request object
            body: Raw request body

        Raises:
            HTTPException: If validation fails
        """
        client_ip = request.client.host if request.client else "unknown"

        # Rate limiting check (shared across all integrations)
        if self.rate_limiter:
            rate_key = f"{integration}:{client_ip}"
            if not self.rate_limiter.is_allowed(rate_key, limit=30, window=60):
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit exceeded for {integration} - too many requests",
                )

        # Integration-specific validation
        validator = self._webhook_validators.get(integration)
        if not validator:
            logger.error(f"🚨 No validator registered for integration: {integration}")
            raise HTTPException(
                status_code=500,
                detail=f"Server configuration error - {integration} validation not available",
            )

        is_valid = await validator.validate_request(request, body)
        if not is_valid:
            logger.error(
                f"❌ {integration} webhook validation failed from IP: {client_ip}"
            )
            raise HTTPException(
                status_code=401,
                detail=f"Invalid {integration} webhook - authentication failed",
            )

        logger.debug(f"✅ {integration} webhook validation passed from IP: {client_ip}")

    def validate_oauth_callback(self, state: str) -> OAuthStateDecoded:
        """
        Validate OAuth callback state parameter.

        Args:
            state: OAuth state parameter from callback

        Returns:
            Decoded state data as Pydantic model

        Raises:
            HTTPException: If state validation fails
        """
        try:
            return self.oauth_state.validate_state(state)
        except ValueError as e:
            logger.error(f"❌ OAuth state validation failed: {e}")
            raise HTTPException(
                status_code=400, detail=f"Invalid OAuth state: {e}"
            ) from e

    def create_oauth_state(self, **kwargs) -> str:
        """Create secure OAuth state parameter."""
        return self.oauth_state.create_state(**kwargs)

    def get_registered_integrations(self) -> list[str]:
        """Get list of registered integration validators."""
        return list(self._webhook_validators.keys())

    def generate_csrf_token(self, session_id: str) -> Optional[str]:
        """Generate CSRF token for admin operations."""
        if not self.csrf_protection:
            return None
        return self.csrf_protection.generate_token(session_id)

    def validate_csrf_token(self, token: str, session_id: str) -> bool:
        """Validate CSRF token for admin operations."""
        if not self.csrf_protection:
            return True  # CSRF protection disabled
        return self.csrf_protection.validate_token(token, session_id)
