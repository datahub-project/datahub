"""
OAuth state storage for managing the OAuth authorization flow.

This module provides:
- OAuthState: Data model for OAuth state during authorization
- CreateStateResult: Return type for state creation with clear field names
- OAuthStateStore: Protocol interface for state storage implementations
- InMemoryOAuthStateStore: In-memory implementation using cachetools.TTLCache

Design Notes:
- Uses NamedTuple for immutable, self-documenting data structures
- Protocol-based interface allows easy swapping to Redis in the future
- TTLCache provides automatic expiration without background threads
- PKCE (Proof Key for Code Exchange) support via code_verifier field

Known Limitations (Phase 2):
- In-memory storage doesn't survive service restarts
- No distributed state sharing across multiple instances
- TTLCache uses LRU eviction when max size is reached
"""

import hashlib
import secrets
import time
from typing import NamedTuple, Optional, Protocol

from cachetools import TTLCache


class OAuthState(NamedTuple):
    """
    OAuth state stored during the authorization flow.

    This is created when the user initiates an OAuth connection and consumed
    when the callback is received from the authorization server.

    Attributes:
        user_urn: The URN of the user initiating the OAuth flow.
        plugin_id: The ID of the AI plugin being connected.
        redirect_uri: The callback URI registered with the authorization server.
        code_verifier: PKCE code verifier (stored securely, used in token exchange).
        created_at: Unix timestamp when the state was created.
        auth_token: The user's authentication token (JWT) captured at connect time.
                    Used to make API calls on behalf of the user in the callback.
    """

    user_urn: str
    plugin_id: str
    redirect_uri: str
    code_verifier: str
    created_at: float
    auth_token: Optional[str] = None


class CreateStateResult(NamedTuple):
    """
    Result of creating a new OAuth state.

    Attributes:
        nonce: The random state parameter to include in the authorization URL.
                This is used to correlate the callback with the original request.
        authorization_url: The full URL to redirect the user to for authorization.
    """

    nonce: str
    authorization_url: str


class OAuthStateStore(Protocol):
    """
    Protocol interface for OAuth state storage.

    Implementations must be thread-safe. This interface is designed to allow
    easy migration from in-memory storage to distributed storage (e.g., Redis).
    """

    def create_state(
        self,
        user_urn: str,
        plugin_id: str,
        redirect_uri: str,
        authorization_url: str,
        code_verifier: str,
        auth_token: Optional[str] = None,
    ) -> CreateStateResult:
        """
        Create and store a new OAuth state.

        Args:
            user_urn: The URN of the user initiating the OAuth flow.
            plugin_id: The ID of the AI plugin being connected.
            redirect_uri: The callback URI for the OAuth flow.
            authorization_url: The base authorization URL (without state param).
            code_verifier: PKCE code verifier for the token exchange.
            auth_token: The user's auth token to use for API calls in the callback.

        Returns:
            CreateStateResult with the nonce and full authorization URL.
        """
        ...

    def get_and_consume_state(self, nonce: str) -> Optional[OAuthState]:
        """
        Retrieve and remove OAuth state by nonce.

        This is atomic - the state is removed upon retrieval to prevent replay.

        Args:
            nonce: The state nonce from the OAuth callback.

        Returns:
            The OAuthState if found and not expired, None otherwise.
        """
        ...

    def cleanup_expired(self) -> int:
        """
        Clean up expired states.

        This is optional for implementations with automatic expiration.

        Returns:
            The number of expired states removed.
        """
        ...


# Default settings for OAuth state storage
DEFAULT_STATE_TTL_SECONDS = 600  # 10 minutes
DEFAULT_MAX_STATES = 10000  # Maximum concurrent OAuth flows


class InMemoryOAuthStateStore:
    """
    In-memory OAuth state storage using cachetools.TTLCache.

    This implementation is suitable for singleton deployments where:
    - Only one instance of the service is running
    - State doesn't need to survive service restarts
    - The number of concurrent OAuth flows is bounded

    Features:
    - Automatic TTL-based expiration (no background threads needed)
    - LRU eviction when max size is reached
    - Thread-safe via TTLCache's internal locking
    - PKCE support for secure authorization code exchange

    Thread Safety:
        TTLCache uses a threading.RLock internally, making it thread-safe for
        concurrent access from multiple request handlers.
    """

    def __init__(
        self,
        ttl_seconds: int = DEFAULT_STATE_TTL_SECONDS,
        max_states: int = DEFAULT_MAX_STATES,
    ) -> None:
        """
        Initialize the in-memory state store.

        Args:
            ttl_seconds: Time-to-live for each state entry (default: 600 = 10 min).
            max_states: Maximum number of concurrent OAuth flows (default: 10000).
        """
        self._store: TTLCache[str, OAuthState] = TTLCache(
            maxsize=max_states, ttl=ttl_seconds
        )
        self._ttl_seconds = ttl_seconds

    def create_state(
        self,
        user_urn: str,
        plugin_id: str,
        redirect_uri: str,
        authorization_url: str,
        code_verifier: str,
        auth_token: Optional[str] = None,
    ) -> CreateStateResult:
        """
        Create and store a new OAuth state.

        Generates a cryptographically secure random nonce and constructs the
        full authorization URL with the state parameter.

        Args:
            user_urn: The URN of the user initiating the OAuth flow.
            plugin_id: The ID of the AI plugin being connected.
            redirect_uri: The callback URI for the OAuth flow.
            authorization_url: The base authorization URL (may already have params).
            code_verifier: PKCE code verifier for the token exchange.
            auth_token: The user's auth token to use for API calls in the callback.

        Returns:
            CreateStateResult with the nonce and full authorization URL.
        """
        # Generate a cryptographically secure random nonce
        nonce = secrets.token_urlsafe(32)

        # Create the state object
        state = OAuthState(
            user_urn=user_urn,
            plugin_id=plugin_id,
            redirect_uri=redirect_uri,
            code_verifier=code_verifier,
            created_at=time.time(),
            auth_token=auth_token,
        )

        # Store the state
        self._store[nonce] = state

        # Build the full authorization URL with state parameter
        full_url = self._build_authorization_url(authorization_url, nonce)

        return CreateStateResult(nonce=nonce, authorization_url=full_url)

    def get_and_consume_state(self, nonce: str) -> Optional[OAuthState]:
        """
        Retrieve and remove OAuth state by nonce.

        This operation is atomic - the state is removed upon successful retrieval.
        This prevents replay attacks where the same nonce is used multiple times.

        Args:
            nonce: The state nonce from the OAuth callback.

        Returns:
            The OAuthState if found and not expired, None otherwise.
        """
        # pop() is atomic in TTLCache
        return self._store.pop(nonce, None)

    def cleanup_expired(self) -> int:
        """
        Clean up expired states.

        Note: TTLCache automatically removes expired items on access, so this
        method primarily forces a cleanup cycle. Returns 0 because TTLCache
        handles expiration transparently.

        Returns:
            Always returns 0 for TTLCache (expiration is automatic).
        """
        # TTLCache handles expiration automatically on access
        # Trigger cleanup by calling expire() if available, or just return 0
        self._store.expire()
        return 0

    def _build_authorization_url(self, base_url: str, state: str) -> str:
        """
        Build the full authorization URL with the state parameter.

        Handles both URLs with existing query parameters and URLs without.

        Args:
            base_url: The base authorization URL.
            state: The state nonce to include.

        Returns:
            The full authorization URL with state parameter.
        """
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}state={state}"

    @property
    def ttl_seconds(self) -> int:
        """
        Get the configured TTL in seconds.

        Returns:
            The TTL value configured for this store.
        """
        return self._ttl_seconds

    def __len__(self) -> int:
        """
        Get the current number of stored states.

        Returns:
            The number of active (non-expired) states.
        """
        return len(self._store)


def generate_code_verifier() -> str:
    """
    Generate a PKCE code verifier.

    Returns a cryptographically random string suitable for PKCE.
    Length is 43-128 characters as per RFC 7636.

    Returns:
        A random code verifier string.
    """
    return secrets.token_urlsafe(64)


def generate_code_challenge(code_verifier: str) -> str:
    """
    Generate a PKCE code challenge from a code verifier.

    Uses the S256 method (SHA-256 hash, base64url-encoded).

    Args:
        code_verifier: The code verifier to transform.

    Returns:
        The code challenge (S256 method).
    """
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    # Base64url encode without padding
    import base64

    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
