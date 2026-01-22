"""Unit tests for OAuth state store."""

import re
import time

from datahub_integrations.oauth.state_store import (
    InMemoryOAuthStateStore,
    OAuthState,
    generate_code_challenge,
    generate_code_verifier,
)


class TestPKCEGeneration:
    """Test PKCE code verifier and challenge generation."""

    def test_code_verifier_is_long_enough(self) -> None:
        """Test code verifier has sufficient length for security."""
        verifier = generate_code_verifier()
        # Should be at least 43 chars (RFC 7636 minimum)
        assert len(verifier) >= 43

    def test_code_verifier_is_url_safe(self) -> None:
        """Test code verifier only contains URL-safe characters."""
        verifier = generate_code_verifier()
        # Should only contain alphanumeric, -, _
        assert re.match(r"^[A-Za-z0-9_-]+$", verifier)

    def test_code_verifiers_are_unique(self) -> None:
        """Test that each code verifier is unique."""
        verifiers = [generate_code_verifier() for _ in range(100)]
        assert len(set(verifiers)) == 100

    def test_code_challenge_is_deterministic(self) -> None:
        """Test that same verifier produces same challenge."""
        verifier = "test-verifier-12345"
        challenge1 = generate_code_challenge(verifier)
        challenge2 = generate_code_challenge(verifier)
        assert challenge1 == challenge2

    def test_code_challenge_is_url_safe(self) -> None:
        """Test code challenge only contains URL-safe characters."""
        verifier = generate_code_verifier()
        challenge = generate_code_challenge(verifier)
        assert re.match(r"^[A-Za-z0-9_-]+$", challenge)


class TestInMemoryOAuthStateStore:
    """Test in-memory OAuth state storage."""

    def test_create_and_get_state(self) -> None:
        """Test creating and retrieving state."""
        store = InMemoryOAuthStateStore(ttl_seconds=60)

        result = store.create_state(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:test-plugin",
            redirect_uri="https://example.com/callback",
            authorization_url="https://provider.com/auth",
            code_verifier="test-verifier",
        )

        assert result.nonce is not None
        assert "https://provider.com/auth" in result.authorization_url

        # Retrieve the state (consumes it)
        state = store.get_and_consume_state(result.nonce)
        assert state is not None
        assert state.user_urn == "urn:li:corpuser:test"
        assert state.plugin_id == "urn:li:service:test-plugin"
        assert state.code_verifier == "test-verifier"

    def test_state_is_deleted_after_get(self) -> None:
        """Test that state is deleted after being retrieved (one-time use)."""
        store = InMemoryOAuthStateStore(ttl_seconds=60)

        result = store.create_state(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:test-plugin",
            redirect_uri="https://example.com/callback",
            authorization_url="https://provider.com/auth",
            code_verifier="test-verifier",
        )

        # First retrieval should succeed
        state = store.get_and_consume_state(result.nonce)
        assert state is not None

        # Second retrieval should return None (consumed)
        state2 = store.get_and_consume_state(result.nonce)
        assert state2 is None

    def test_get_nonexistent_state_returns_none(self) -> None:
        """Test that getting a nonexistent state returns None."""
        store = InMemoryOAuthStateStore(ttl_seconds=60)

        state = store.get_and_consume_state("nonexistent-state-key")
        assert state is None

    def test_state_expires_after_ttl(self) -> None:
        """Test that state expires after TTL."""
        store = InMemoryOAuthStateStore(ttl_seconds=1)  # 1 second TTL

        result = store.create_state(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:test-plugin",
            redirect_uri="https://example.com/callback",
            authorization_url="https://provider.com/auth",
            code_verifier="test-verifier",
        )

        # State should exist immediately
        assert result.nonce is not None

        # Wait for TTL to expire
        time.sleep(1.5)

        # State should be expired
        state = store.get_and_consume_state(result.nonce)
        assert state is None

    def test_create_state_with_auth_token(self) -> None:
        """Test creating state with auth token for user-context mutations."""
        store = InMemoryOAuthStateStore(ttl_seconds=60)

        result = store.create_state(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:test-plugin",
            redirect_uri="https://example.com/callback",
            authorization_url="https://provider.com/auth",
            code_verifier="test-verifier",
            auth_token="user-jwt-token",
        )

        state = store.get_and_consume_state(result.nonce)
        assert state is not None
        assert state.auth_token == "user-jwt-token"

    def test_state_nonces_are_unique(self) -> None:
        """Test that state nonces are unique."""
        store = InMemoryOAuthStateStore(ttl_seconds=60)

        nonces = []
        for _ in range(100):
            result = store.create_state(
                user_urn="urn:li:corpuser:test",
                plugin_id="urn:li:service:test-plugin",
                redirect_uri="https://example.com/callback",
                authorization_url="https://provider.com/auth",
                code_verifier="test-verifier",
            )
            nonces.append(result.nonce)
            # Consume the state to allow creating more
            store.get_and_consume_state(result.nonce)

        assert len(set(nonces)) == 100


class TestOAuthState:
    """Test OAuthState NamedTuple."""

    def test_oauth_state_fields(self) -> None:
        """Test OAuthState has all required fields."""
        created_at = time.time()
        state = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:test",
            redirect_uri="https://example.com/callback",
            code_verifier="test-verifier",
            created_at=created_at,
        )

        assert state.user_urn == "urn:li:corpuser:test"
        assert state.plugin_id == "urn:li:service:test"
        assert state.redirect_uri == "https://example.com/callback"
        assert state.code_verifier == "test-verifier"
        assert state.created_at == created_at
        assert state.auth_token is None  # Optional field

    def test_oauth_state_with_auth_token(self) -> None:
        """Test OAuthState with auth token."""
        created_at = time.time()
        state = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="urn:li:service:test",
            redirect_uri="https://example.com/callback",
            code_verifier="test-verifier",
            created_at=created_at,
            auth_token="my-jwt-token",
        )

        assert state.auth_token == "my-jwt-token"
