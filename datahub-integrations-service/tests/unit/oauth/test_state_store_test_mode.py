"""Tests for OAuthFlowMode and flow mode in OAuthState."""

from datahub_integrations.oauth.state_store import (
    InMemoryOAuthStateStore,
    McpDiscoveryFlow,
    NormalOAuthFlow,
    OAuthState,
)


class TestOAuthFlowModeVariants:
    """Tests for the OAuthFlowMode discriminated union variants."""

    def test_normal_flow_type(self) -> None:
        flow = NormalOAuthFlow()
        assert flow.type == "normal"

    def test_mcp_discovery_flow_type(self) -> None:
        flow = McpDiscoveryFlow(
            oauth_config={"clientId": "test"},
            mcp_config={"url": "http://example.com"},
        )
        assert flow.type == "mcp_discovery"

    def test_mcp_discovery_flow_carries_payload(self) -> None:
        oauth_config = {
            "clientId": "client-123",
            "tokenUrl": "https://provider.com/token",
        }
        mcp_config = {"url": "https://mcp.example.com", "transport": "HTTP"}
        flow = McpDiscoveryFlow(oauth_config=oauth_config, mcp_config=mcp_config)
        assert flow.oauth_config == oauth_config
        assert flow.mcp_config == mcp_config

    def test_normal_flow_has_no_payload(self) -> None:
        flow = NormalOAuthFlow()
        assert not hasattr(flow, "oauth_config")
        assert not hasattr(flow, "mcp_config")


class TestOAuthStateWithFlowMode:
    """Tests for OAuthState with flow_mode field."""

    def test_default_flow_mode_is_normal(self) -> None:
        state = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="test-plugin",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=1000.0,
        )
        assert isinstance(state.flow_mode, NormalOAuthFlow)

    def test_mcp_discovery_flow_mode(self) -> None:
        oauth_config = {"clientId": "client-123"}
        mcp_config = {"url": "https://mcp.example.com"}
        state = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="__test__",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=1000.0,
            flow_mode=McpDiscoveryFlow(
                oauth_config=oauth_config,
                mcp_config=mcp_config,
            ),
        )
        assert isinstance(state.flow_mode, McpDiscoveryFlow)
        assert state.flow_mode.oauth_config == oauth_config
        assert state.flow_mode.mcp_config == mcp_config

    def test_state_is_immutable(self) -> None:
        """Frozen dataclass should prevent mutation."""
        state = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="test",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=1000.0,
        )
        try:
            state.flow_mode = McpDiscoveryFlow(  # type: ignore
                oauth_config={}, mcp_config={}
            )
            raise AssertionError("Should have raised FrozenInstanceError")
        except AttributeError:
            pass

    def test_isinstance_branching(self) -> None:
        """Callback-style branching on flow mode variant."""
        normal_state = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="plugin",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=1000.0,
        )
        assert isinstance(normal_state.flow_mode, NormalOAuthFlow)
        assert not isinstance(normal_state.flow_mode, McpDiscoveryFlow)

        test_state = OAuthState(
            user_urn="urn:li:corpuser:test",
            plugin_id="__test__",
            redirect_uri="https://example.com/callback",
            code_verifier="verifier",
            created_at=1000.0,
            flow_mode=McpDiscoveryFlow(oauth_config={}, mcp_config={}),
        )
        assert isinstance(test_state.flow_mode, McpDiscoveryFlow)
        assert not isinstance(test_state.flow_mode, NormalOAuthFlow)


class TestInMemoryOAuthStateStoreWithFlowMode:
    """Tests for state store with flow mode."""

    def test_create_and_consume_state_with_mcp_discovery(self) -> None:
        store = InMemoryOAuthStateStore(ttl_seconds=60)
        oauth_config = {"clientId": "test-client"}
        mcp_config = {"url": "https://mcp.example.com"}

        result = store.create_state(
            user_urn="urn:li:corpuser:test",
            plugin_id="__test__",
            redirect_uri="https://example.com/callback",
            authorization_url="https://provider.com/authorize?client_id=test",
            code_verifier="verifier",
            auth_token="jwt-token",
            flow_mode=McpDiscoveryFlow(
                oauth_config=oauth_config,
                mcp_config=mcp_config,
            ),
        )

        assert result.nonce
        assert "state=" in result.authorization_url

        state = store.get_and_consume_state(result.nonce)
        assert state is not None
        assert isinstance(state.flow_mode, McpDiscoveryFlow)
        assert state.flow_mode.oauth_config == oauth_config
        assert state.flow_mode.mcp_config == mcp_config

        # Consumed -- should return None on second call
        assert store.get_and_consume_state(result.nonce) is None

    def test_create_state_defaults_to_normal_flow(self) -> None:
        store = InMemoryOAuthStateStore(ttl_seconds=60)
        result = store.create_state(
            user_urn="urn:li:corpuser:test",
            plugin_id="plugin",
            redirect_uri="https://example.com/callback",
            authorization_url="https://provider.com/authorize",
            code_verifier="verifier",
        )
        state = store.get_and_consume_state(result.nonce)
        assert state is not None
        assert isinstance(state.flow_mode, NormalOAuthFlow)
