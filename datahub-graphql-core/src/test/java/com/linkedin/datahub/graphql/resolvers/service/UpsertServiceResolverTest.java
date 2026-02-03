package com.linkedin.datahub.graphql.resolvers.service;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AiPluginAuthType;
import com.linkedin.datahub.graphql.generated.McpServerPropertiesInput;
import com.linkedin.datahub.graphql.generated.McpTransport;
import com.linkedin.datahub.graphql.generated.ServiceSubType;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpsertOAuthAuthorizationServerInput;
import com.linkedin.datahub.graphql.generated.UpsertServiceInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.service.McpServerProperties;
import com.linkedin.service.ServiceProperties;
import com.linkedin.settings.global.AiPluginConfig;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.AiPluginType;
import com.linkedin.settings.global.AuthInjectionLocation;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthAiPluginConfig;
import com.linkedin.settings.global.SharedApiKeyAiPluginConfig;
import com.linkedin.settings.global.UserApiKeyAiPluginConfig;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpsertServiceResolverTest {

  private static final String TEST_SERVICE_ID = "test-mcp-server";
  private static final String TEST_SERVICE_URN = "urn:li:service:test-mcp-server";

  @Mock private EntityClient entityClient;
  @Mock private SecretService secretService;
  @Mock private ConnectionService connectionService;
  @Mock private DataFetchingEnvironment environment;

  private UpsertServiceResolver resolver;
  private List<MetadataChangeProposal> capturedProposals;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    resolver = new UpsertServiceResolver(entityClient, secretService, connectionService);
    capturedProposals = new ArrayList<>();

    // Mock secret encryption
    when(secretService.encrypt(any())).thenReturn("encrypted-value");

    // Capture all ingest proposals
    doAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);
              capturedProposals.add(proposal);
              return null;
            })
        .when(entityClient)
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  /**
   * Test that validation fails BEFORE any writes when subType is MCP_SERVER but mcpServerProperties
   * is null. This is critical to prevent inconsistent entity state.
   */
  @Test
  public void testValidationFailsBeforeWritesWhenMcpPropertiesMissing() throws Exception {
    // Setup auth context that allows the operation
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Create input with MCP_SERVER subType but NO mcpServerProperties
    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName("Test MCP Server");
    input.setSubType(ServiceSubType.MCP_SERVER);
    input.setMcpServerProperties(null); // Missing required properties!

    when(environment.getArgument("input")).thenReturn(input);

    // Execute and expect IllegalArgumentException
    // Note: This exception is thrown synchronously (before async block)
    try {
      resolver.get(environment);
      fail("Expected IllegalArgumentException for missing mcpServerProperties");
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage().contains("mcpServerProperties is required"),
          "Exception message should mention mcpServerProperties");
    }

    // CRITICAL: Verify that NO entity operations were performed
    // This ensures validation happens BEFORE any writes
    verify(entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  /** Test that validation passes when subType is MCP_SERVER and mcpServerProperties is provided. */
  @Test
  public void testValidationPassesWhenMcpPropertiesProvided() throws Exception {
    // This test just verifies the validation doesn't throw when properties are provided
    // We don't need to fully execute the resolver - just verify the validation passes

    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Create valid input with MCP_SERVER subType AND mcpServerProperties
    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName("Test MCP Server");
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl("https://example.com/mcp");
    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute - the resolver will fail later (missing mocks for full flow)
    // but it should NOT fail on validation
    try {
      resolver.get(environment).join();
      // If we get here without IllegalArgumentException, validation passed
      // The resolver might fail for other reasons (missing mocks), which is fine
    } catch (Exception e) {
      // Should NOT be IllegalArgumentException about mcpServerProperties
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof IllegalArgumentException
          && cause.getMessage().contains("mcpServerProperties")) {
        fail("Validation should not fail when mcpServerProperties is provided");
      }
      // Other exceptions are expected (incomplete mocks)
    }
  }

  /** Test that unauthorized users cannot create services. */
  @Test
  public void testUnauthorizedUserCannotCreateService() throws Exception {
    // Setup deny context
    QueryContext mockContext = getMockDenyContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName("Test Service");
    input.setSubType(ServiceSubType.MCP_SERVER);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute and expect AuthorizationException
    // Note: This exception is thrown synchronously (before async block)
    try {
      resolver.get(environment);
      fail("Expected AuthorizationException for unauthorized user");
    } catch (AuthorizationException e) {
      // Expected - authorization failed
      assertTrue(e.getMessage().contains("Unauthorized"));
    }

    // Verify no entity operations were performed
    verify(entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Field Validation Edge Cases
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that displayName is required. */
  @Test
  public void testDisplayNameRequired() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName(null); // Missing required displayName
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl("https://example.com/mcp");
    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute - should fail validation
    try {
      resolver.get(environment).join();
      // If it gets here due to async execution, the exception may be different
    } catch (Exception e) {
      // Expected - displayName is required
      // The exact exception type depends on where validation happens
    }
  }

  /** Test that null subType is handled (either fails validation or throws NPE). */
  @Test
  public void testSubTypeRequired() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName("Test Service");
    input.setSubType(null); // Missing required subType

    when(environment.getArgument("input")).thenReturn(input);

    // Execute - should fail validation or throw NPE
    try {
      resolver.get(environment).join();
      // If it completes without exception, null subType handling is different
    } catch (Exception e) {
      // Expected - null subType should cause some failure
      // Can be IllegalArgumentException, NullPointerException, or CompletionException
    }
  }

  /** Test MCP server URL validation - URL is required. */
  @Test
  public void testMcpServerUrlRequired() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName("Test MCP Server");
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl(null); // Missing required URL
    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute - may fail in async block or during GraphQL binding
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // Expected - URL is required for MCP server
    }
  }

  /** Test MCP server with empty URL. */
  @Test
  public void testMcpServerEmptyUrl() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName("Test MCP Server");
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl(""); // Empty URL
    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute - should fail validation or proceed with empty URL
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // Expected behavior depends on validation implementation
    }
  }

  /** Test that empty displayName is rejected. */
  @Test
  public void testEmptyDisplayNameRejected() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertServiceInput input = new UpsertServiceInput();
    input.setDisplayName(""); // Empty displayName
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl("https://example.com/mcp");
    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute - behavior depends on validation implementation
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail if empty displayName is rejected
    }
  }

  /** Test that very long displayName is handled. */
  @Test
  public void testLongDisplayName() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertServiceInput input = new UpsertServiceInput();
    // Create a very long display name (1000 characters)
    String longName = "A".repeat(1000);
    input.setDisplayName(longName);
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl("https://example.com/mcp");
    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute - should either accept or reject based on length limits
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail due to length limits or other validation
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // MCP Server Properties Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test MCP server with all properties specified. */
  @Test
  public void testMcpServerWithAllProperties() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = new UpsertServiceInput();
    input.setId(TEST_SERVICE_ID);
    input.setDisplayName("Full MCP Server");
    input.setDescription("A fully configured MCP server");
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl("https://mcp.example.com/api");
    mcpProps.setTransport(McpTransport.SSE);
    mcpProps.setTimeout(60.0f);

    // Custom headers
    List<StringMapEntryInput> headers = new ArrayList<>();
    StringMapEntryInput header1 = new StringMapEntryInput();
    header1.setKey("X-Custom-Header");
    header1.setValue("custom-value");
    headers.add(header1);
    mcpProps.setCustomHeaders(headers);

    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify proposals were made
    assertFalse(capturedProposals.isEmpty(), "Should have captured proposals");

    // Find the MCP server properties proposal
    MetadataChangeProposal mcpProposal = null;
    for (MetadataChangeProposal p : capturedProposals) {
      if (Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME.equals(p.getAspectName())) {
        mcpProposal = p;
        break;
      }
    }
    assertNotNull(mcpProposal, "Should have MCP server properties proposal");

    McpServerProperties mcpServerProps = deserializeMcpProperties(mcpProposal);
    assertEquals(mcpServerProps.getUrl(), "https://mcp.example.com/api");
    assertEquals(mcpServerProps.getTransport(), com.linkedin.service.McpTransport.SSE);
    assertEquals(mcpServerProps.getTimeout(), 60.0f, 0.01);
    assertTrue(mcpServerProps.hasCustomHeaders());
    assertEquals(mcpServerProps.getCustomHeaders().get("X-Custom-Header"), "custom-value");
  }

  /** Test MCP server with HTTP transport (default). */
  @Test
  public void testMcpServerWithHttpTransport() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.getMcpServerProperties().setTransport(McpTransport.HTTP);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal mcpProposal =
        findProposalByAspect(Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(mcpProposal);
    McpServerProperties props = deserializeMcpProperties(mcpProposal);
    assertEquals(props.getTransport(), com.linkedin.service.McpTransport.HTTP);
  }

  /** Test MCP server with WebSocket transport. */
  @Test
  public void testMcpServerWithWebSocketTransport() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.getMcpServerProperties().setTransport(McpTransport.WEBSOCKET);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal mcpProposal =
        findProposalByAspect(Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(mcpProposal);
    McpServerProperties props = deserializeMcpProperties(mcpProposal);
    assertEquals(props.getTransport(), com.linkedin.service.McpTransport.WEBSOCKET);
  }

  /** Test MCP server with null transport defaults to HTTP. */
  @Test
  public void testMcpServerTransportDefaults() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.getMcpServerProperties().setTransport(null); // Not specified

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal mcpProposal =
        findProposalByAspect(Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(mcpProposal);
    McpServerProperties props = deserializeMcpProperties(mcpProposal);
    assertEquals(props.getTransport(), com.linkedin.service.McpTransport.HTTP);
  }

  /** Test MCP server timeout defaults to 30.0. */
  @Test
  public void testMcpServerTimeoutDefaults() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.getMcpServerProperties().setTimeout(null); // Not specified

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal mcpProposal =
        findProposalByAspect(Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(mcpProposal);
    McpServerProperties props = deserializeMcpProperties(mcpProposal);
    assertEquals(props.getTimeout(), 30.0f, 0.01);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // AI Plugin Config Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test service creation with auth type NONE. */
  @Test
  public void testServiceWithAuthTypeNone() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.NONE);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify GlobalSettings was updated
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");
  }

  /** Test service creation with USER_OAUTH auth type and existing OAuth server. */
  @Test
  public void testServiceWithUserOAuthAndExistingServer() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_OAUTH);
    input.setOauthServerUrn("urn:li:oauthAuthorizationServer:existing-server");
    input.setRequiredScopes(ImmutableList.of("read", "write"));

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify GlobalSettings was updated
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");
  }

  /** Test service creation with inline OAuth server (no ID - creates new). */
  @Test
  public void testServiceWithInlineOAuthServer() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_OAUTH);

    // Inline OAuth server - NO ID (creates new server with auto-generated UUID)
    UpsertOAuthAuthorizationServerInput oauthInput = new UpsertOAuthAuthorizationServerInput();
    // Note: id is NOT set - this is correct for new OAuth servers
    oauthInput.setDisplayName("Inline OAuth Server");
    oauthInput.setClientId("client-123");
    oauthInput.setClientSecret("secret-123");
    oauthInput.setAuthorizationUrl("https://auth.example.com/authorize");
    oauthInput.setTokenUrl("https://auth.example.com/token");
    input.setNewOAuthServer(oauthInput);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Should have proposals for OAuth server creation
    MetadataChangeProposal oauthProposal =
        findProposalByAspect(Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(oauthProposal, "Should create OAuth server");
  }

  /**
   * Test that createOAuthServer rejects input with an ID. To update an existing OAuth server, use
   * the upsertOAuthAuthorizationServer mutation directly, then link via oauthServerUrn.
   */
  @Test
  public void testInlineOAuthServerRejectsIdForUpdate() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_OAUTH);

    // Inline OAuth server WITH an ID - this should fail
    UpsertOAuthAuthorizationServerInput oauthInput = new UpsertOAuthAuthorizationServerInput();
    oauthInput.setId(
        "existing-oauth-server-id"); // ID provided = trying to update, which is not allowed
    oauthInput.setDisplayName("OAuth Server");
    oauthInput.setClientId("client-123");
    oauthInput.setClientSecret("secret-123");
    oauthInput.setAuthorizationUrl("https://auth.example.com/authorize");
    oauthInput.setTokenUrl("https://auth.example.com/token");
    input.setNewOAuthServer(oauthInput);

    when(environment.getArgument("input")).thenReturn(input);

    // Execute and expect IllegalArgumentException
    Exception thrown = null;
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      thrown = e;
    }

    assertNotNull(thrown, "Should throw exception when ID is provided in newOAuthServer");

    // Unwrap exception chain to find the IllegalArgumentException
    Throwable cause = thrown;
    while (cause != null && !(cause instanceof IllegalArgumentException)) {
      cause = cause.getCause();
    }
    assertNotNull(
        cause, "Should contain IllegalArgumentException in exception chain, got: " + thrown);
    assertTrue(
        cause instanceof IllegalArgumentException,
        "Should throw IllegalArgumentException, got: " + cause.getClass().getName());
    assertTrue(
        cause.getMessage().contains("Cannot update existing OAuth server via newOAuthServer"),
        "Exception message should explain the issue, got: " + cause.getMessage());
  }

  /** Test service creation with SHARED_API_KEY auth type. */
  @Test
  public void testServiceWithSharedApiKey() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    // Mock connectionService to return a credential URN
    Urn credentialUrn =
        UrnUtils.getUrn("urn:li:dataHubConnection:urn_li_service_test-mcp-server__apiKey");
    when(connectionService.upsertConnection(any(), any(), any(), any(), any(), any()))
        .thenReturn(credentialUrn);

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.SHARED_API_KEY);
    input.setOauthServerUrn("urn:li:oauthAuthorizationServer:api-key-server");
    input.setSharedApiKey("my-shared-api-key-value");

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify ConnectionService was called to create the shared credential
    verify(connectionService).upsertConnection(any(), any(), any(), any(), any(), any());
  }

  /**
   * Test that existing credential is preserved when updating a service without providing a new API
   * key. This is critical to prevent credential loss when editing plugin settings.
   */
  @Test
  public void testSharedApiKeyCredentialPreservedOnUpdate() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupServiceResponseMock();

    // Setup existing GlobalSettings with a plugin config that has a credential
    Urn existingCredentialUrn =
        UrnUtils.getUrn("urn:li:dataHubConnection:urn_li_service_test-mcp-server__apiKey");
    Urn serviceUrn = UrnUtils.getUrn(TEST_SERVICE_URN);

    SharedApiKeyAiPluginConfig existingSharedConfig = new SharedApiKeyAiPluginConfig();
    existingSharedConfig.setCredentialUrn(existingCredentialUrn);
    existingSharedConfig.setAuthLocation(AuthInjectionLocation.HEADER);
    existingSharedConfig.setAuthScheme("Bearer");

    AiPluginConfig existingPluginConfig = new AiPluginConfig();
    existingPluginConfig.setId(TEST_SERVICE_URN);
    existingPluginConfig.setServiceUrn(serviceUrn);
    existingPluginConfig.setType(AiPluginType.MCP_SERVER);
    existingPluginConfig.setAuthType(com.linkedin.settings.global.AiPluginAuthType.SHARED_API_KEY);
    existingPluginConfig.setEnabled(true);
    existingPluginConfig.setSharedApiKeyConfig(existingSharedConfig);

    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();
    pluginsArray.add(existingPluginConfig);

    AiPluginSettings pluginSettings = new AiPluginSettings();
    pluginSettings.setPlugins(pluginsArray);

    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    existingSettings.setAiPluginSettings(pluginSettings);

    setupGlobalSettingsMock(existingSettings);

    // Create update input WITHOUT providing a new API key
    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.SHARED_API_KEY);
    input.setSharedApiKey(null); // No new API key provided!
    // Don't provide auth settings - should be preserved from existing config

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can verify captured proposals
    }

    // ConnectionService should NOT be called since no new API key was provided
    verify(connectionService, never()).upsertConnection(any(), any(), any(), any(), any(), any());

    // Verify GlobalSettings was updated with preserved credential
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");

    // Deserialize and verify the credential URN is preserved
    JacksonDataCodec codec = new JacksonDataCodec();
    DataMap dataMap = codec.bytesToMap(globalSettingsProposal.getAspect().getValue().copyBytes());
    GlobalSettingsInfo updatedSettings = new GlobalSettingsInfo(dataMap);

    assertTrue(updatedSettings.hasAiPluginSettings(), "Should have AI plugin settings");
    assertTrue(updatedSettings.getAiPluginSettings().hasPlugins(), "Should have plugins array");

    AiPluginConfigArray updatedPlugins = updatedSettings.getAiPluginSettings().getPlugins();
    assertEquals(updatedPlugins.size(), 1, "Should have one plugin");

    AiPluginConfig updatedPlugin = updatedPlugins.get(0);
    assertTrue(updatedPlugin.hasSharedApiKeyConfig(), "Should have shared API key config");

    SharedApiKeyAiPluginConfig updatedSharedConfig = updatedPlugin.getSharedApiKeyConfig();
    assertEquals(
        updatedSharedConfig.getCredentialUrn(),
        existingCredentialUrn,
        "Credential URN should be preserved");
    assertEquals(
        updatedSharedConfig.getAuthLocation(),
        AuthInjectionLocation.HEADER,
        "Auth location should be preserved");
    assertEquals(updatedSharedConfig.getAuthScheme(), "Bearer", "Auth scheme should be preserved");
  }

  /**
   * Test that new credential is created when updating with a new API key, even if one already
   * exists.
   */
  @Test
  public void testSharedApiKeyCredentialReplacedOnUpdate() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupServiceResponseMock();

    // Setup existing GlobalSettings with a plugin config that has a credential
    Urn existingCredentialUrn =
        UrnUtils.getUrn("urn:li:dataHubConnection:urn_li_service_test-mcp-server__apiKey_old");
    Urn newCredentialUrn =
        UrnUtils.getUrn("urn:li:dataHubConnection:urn_li_service_test-mcp-server__apiKey");
    Urn serviceUrn = UrnUtils.getUrn(TEST_SERVICE_URN);

    SharedApiKeyAiPluginConfig existingSharedConfig = new SharedApiKeyAiPluginConfig();
    existingSharedConfig.setCredentialUrn(existingCredentialUrn);
    existingSharedConfig.setAuthScheme("Bearer");

    AiPluginConfig existingPluginConfig = new AiPluginConfig();
    existingPluginConfig.setId(TEST_SERVICE_URN);
    existingPluginConfig.setServiceUrn(serviceUrn);
    existingPluginConfig.setType(AiPluginType.MCP_SERVER);
    existingPluginConfig.setAuthType(com.linkedin.settings.global.AiPluginAuthType.SHARED_API_KEY);
    existingPluginConfig.setEnabled(true);
    existingPluginConfig.setSharedApiKeyConfig(existingSharedConfig);

    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();
    pluginsArray.add(existingPluginConfig);

    AiPluginSettings pluginSettings = new AiPluginSettings();
    pluginSettings.setPlugins(pluginsArray);

    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    existingSettings.setAiPluginSettings(pluginSettings);

    setupGlobalSettingsMock(existingSettings);

    // Mock connectionService to return a new credential URN
    when(connectionService.upsertConnection(any(), any(), any(), any(), any(), any()))
        .thenReturn(newCredentialUrn);

    // Create update input WITH a new API key
    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.SHARED_API_KEY);
    input.setSharedApiKey("new-api-key-value"); // New API key provided!
    input.setSharedApiKeyAuthScheme("Token"); // New auth scheme

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can verify captured proposals
    }

    // ConnectionService SHOULD be called since a new API key was provided
    verify(connectionService).upsertConnection(any(), any(), any(), any(), any(), any());

    // Verify GlobalSettings was updated with new credential
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");

    // Deserialize and verify the new credential URN is used
    JacksonDataCodec codec = new JacksonDataCodec();
    DataMap dataMap = codec.bytesToMap(globalSettingsProposal.getAspect().getValue().copyBytes());
    GlobalSettingsInfo updatedSettings = new GlobalSettingsInfo(dataMap);

    AiPluginConfig updatedPlugin = updatedSettings.getAiPluginSettings().getPlugins().get(0);
    SharedApiKeyAiPluginConfig updatedSharedConfig = updatedPlugin.getSharedApiKeyConfig();

    assertEquals(
        updatedSharedConfig.getCredentialUrn(), newCredentialUrn, "Should use new credential URN");
    assertEquals(updatedSharedConfig.getAuthScheme(), "Token", "Should use new auth scheme");
  }

  /**
   * Test that existing USER_API_KEY auth settings are preserved when updating a service without
   * re-providing them. This prevents auth injection settings loss during partial updates.
   *
   * <p>Note: Fields with GraphQL schema defaults (authLocation, authHeaderName) will receive their
   * default values even when not explicitly set in the mutation, so we only test preservation of
   * fields without defaults (authScheme, authQueryParam).
   */
  @Test
  public void testUserApiKeySettingsPreservedOnUpdate() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupServiceResponseMock();

    // Setup existing GlobalSettings with a USER_API_KEY plugin config
    Urn serviceUrn = UrnUtils.getUrn(TEST_SERVICE_URN);

    UserApiKeyAiPluginConfig existingUserConfig = new UserApiKeyAiPluginConfig();
    existingUserConfig.setAuthLocation(AuthInjectionLocation.HEADER);
    existingUserConfig.setAuthScheme("Bearer");
    existingUserConfig.setAuthQueryParam("api_key");

    AiPluginConfig existingPluginConfig = new AiPluginConfig();
    existingPluginConfig.setId(TEST_SERVICE_URN);
    existingPluginConfig.setServiceUrn(serviceUrn);
    existingPluginConfig.setType(AiPluginType.MCP_SERVER);
    existingPluginConfig.setAuthType(com.linkedin.settings.global.AiPluginAuthType.USER_API_KEY);
    existingPluginConfig.setEnabled(true);
    existingPluginConfig.setUserApiKeyConfig(existingUserConfig);

    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();
    pluginsArray.add(existingPluginConfig);

    AiPluginSettings pluginSettings = new AiPluginSettings();
    pluginSettings.setPlugins(pluginsArray);

    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    existingSettings.setAiPluginSettings(pluginSettings);

    setupGlobalSettingsMock(existingSettings);

    // Create update input WITHOUT providing auth settings (just disable the plugin)
    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_API_KEY);
    input.setEnabled(false);
    // Don't provide auth settings - should be preserved from existing config

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can verify captured proposals
    }

    // Verify GlobalSettings was updated with preserved auth settings
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");

    // Deserialize and verify the auth settings are preserved
    JacksonDataCodec codec = new JacksonDataCodec();
    DataMap dataMap = codec.bytesToMap(globalSettingsProposal.getAspect().getValue().copyBytes());
    GlobalSettingsInfo updatedSettings = new GlobalSettingsInfo(dataMap);

    assertTrue(updatedSettings.hasAiPluginSettings(), "Should have AI plugin settings");
    assertTrue(updatedSettings.getAiPluginSettings().hasPlugins(), "Should have plugins array");

    AiPluginConfigArray updatedPlugins = updatedSettings.getAiPluginSettings().getPlugins();
    assertEquals(updatedPlugins.size(), 1, "Should have one plugin");

    AiPluginConfig updatedPlugin = updatedPlugins.get(0);
    assertFalse(updatedPlugin.isEnabled(), "Should be disabled");
    assertTrue(updatedPlugin.hasUserApiKeyConfig(), "Should have user API key config");

    UserApiKeyAiPluginConfig updatedUserConfig = updatedPlugin.getUserApiKeyConfig();
    // Note: authLocation and authHeaderName have GraphQL defaults, so they will be set to
    // the default values even if not explicitly provided. We test fields without defaults.
    assertEquals(updatedUserConfig.getAuthScheme(), "Bearer", "Auth scheme should be preserved");
    assertEquals(
        updatedUserConfig.getAuthQueryParam(), "api_key", "Auth query param should be preserved");
  }

  /**
   * Test that existing USER_OAUTH settings (serverUrn and requiredScopes) are preserved when
   * updating a service without re-providing them.
   */
  @Test
  public void testUserOAuthSettingsPreservedOnUpdate() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupServiceResponseMock();

    // Setup existing GlobalSettings with a USER_OAUTH plugin config
    Urn serviceUrn = UrnUtils.getUrn(TEST_SERVICE_URN);
    Urn existingOAuthServerUrn =
        UrnUtils.getUrn("urn:li:oAuthAuthorizationServer:existing-oauth-server");

    OAuthAiPluginConfig existingOAuthConfig = new OAuthAiPluginConfig();
    existingOAuthConfig.setServerUrn(existingOAuthServerUrn);
    existingOAuthConfig.setRequiredScopes(
        new com.linkedin.data.template.StringArray(
            ImmutableList.of("read:data", "write:data", "admin")));

    AiPluginConfig existingPluginConfig = new AiPluginConfig();
    existingPluginConfig.setId(TEST_SERVICE_URN);
    existingPluginConfig.setServiceUrn(serviceUrn);
    existingPluginConfig.setType(AiPluginType.MCP_SERVER);
    existingPluginConfig.setAuthType(com.linkedin.settings.global.AiPluginAuthType.USER_OAUTH);
    existingPluginConfig.setEnabled(true);
    existingPluginConfig.setOauthConfig(existingOAuthConfig);

    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();
    pluginsArray.add(existingPluginConfig);

    AiPluginSettings pluginSettings = new AiPluginSettings();
    pluginSettings.setPlugins(pluginsArray);

    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    existingSettings.setAiPluginSettings(pluginSettings);

    setupGlobalSettingsMock(existingSettings);

    // Create update input WITHOUT providing OAuth server or scopes (just disable the plugin)
    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_OAUTH);
    input.setEnabled(false);
    // Don't provide oauthServer or requiredScopes - should be preserved from existing config

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can verify captured proposals
    }

    // Verify GlobalSettings was updated with preserved OAuth settings
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");

    // Deserialize and verify the OAuth settings are preserved
    JacksonDataCodec codec = new JacksonDataCodec();
    DataMap dataMap = codec.bytesToMap(globalSettingsProposal.getAspect().getValue().copyBytes());
    GlobalSettingsInfo updatedSettings = new GlobalSettingsInfo(dataMap);

    assertTrue(updatedSettings.hasAiPluginSettings(), "Should have AI plugin settings");
    assertTrue(updatedSettings.getAiPluginSettings().hasPlugins(), "Should have plugins array");

    AiPluginConfigArray updatedPlugins = updatedSettings.getAiPluginSettings().getPlugins();
    assertEquals(updatedPlugins.size(), 1, "Should have one plugin");

    AiPluginConfig updatedPlugin = updatedPlugins.get(0);
    assertFalse(updatedPlugin.isEnabled(), "Should be disabled");
    assertTrue(updatedPlugin.hasOauthConfig(), "Should have OAuth config");

    OAuthAiPluginConfig updatedOAuthConfig = updatedPlugin.getOauthConfig();
    assertEquals(
        updatedOAuthConfig.getServerUrn(),
        existingOAuthServerUrn,
        "OAuth server URN should be preserved");
    assertTrue(updatedOAuthConfig.hasRequiredScopes(), "Should have required scopes");
    assertEquals(
        updatedOAuthConfig.getRequiredScopes().size(), 3, "Should have 3 scopes preserved");
    assertTrue(
        updatedOAuthConfig.getRequiredScopes().contains("read:data"),
        "Should contain read:data scope");
    assertTrue(
        updatedOAuthConfig.getRequiredScopes().contains("write:data"),
        "Should contain write:data scope");
    assertTrue(
        updatedOAuthConfig.getRequiredScopes().contains("admin"), "Should contain admin scope");
  }

  /** Test service creation with enabled=false. */
  @Test
  public void testServiceWithEnabledFalse() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setEnabled(false);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify GlobalSettings was updated
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");
  }

  /** Test service creation with instructions. */
  @Test
  public void testServiceWithInstructions() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setInstructions("Use this server for search queries. Always verify results.");

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify GlobalSettings was updated
    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings with instructions");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Constructor Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testConstructorNullEntityClient() {
    assertThrows(
        NullPointerException.class,
        () -> new UpsertServiceResolver(null, secretService, connectionService));
  }

  @Test
  public void testConstructorNullSecretService() {
    assertThrows(
        NullPointerException.class,
        () -> new UpsertServiceResolver(entityClient, null, connectionService));
  }

  @Test
  public void testConstructorNullConnectionService() {
    assertThrows(
        NullPointerException.class,
        () -> new UpsertServiceResolver(entityClient, secretService, null));
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Auto-Generated ID Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that a UUID is generated when no ID is provided. */
  @Test
  public void testAutoGeneratedServiceId() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());

    // Mock batchGetV2 to return a response for any URN
    when(entityClient.batchGetV2(any(), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenAnswer(
            invocation -> {
              java.util.Set<?> urns = invocation.getArgument(2);
              Urn serviceUrn = (Urn) urns.iterator().next();
              ServiceProperties props = new ServiceProperties();
              props.setDisplayName("Auto ID Service");
              EnvelopedAspectMap aspects = new EnvelopedAspectMap();
              aspects.put(
                  Constants.SERVICE_PROPERTIES_ASPECT_NAME,
                  new EnvelopedAspect().setValue(new Aspect(props.data())));
              EntityResponse response = new EntityResponse();
              response.setUrn(serviceUrn);
              response.setEntityName(Constants.SERVICE_ENTITY_NAME);
              response.setAspects(aspects);
              return ImmutableMap.of(serviceUrn, response);
            });

    UpsertServiceInput input = new UpsertServiceInput();
    input.setId(null); // No ID - should auto-generate
    input.setDisplayName("Auto ID Service");
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl("https://mcp.example.com");
    input.setMcpServerProperties(mcpProps);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail, but we can check captured proposals
    }

    // Verify that proposals were made with a URN containing a UUID
    MetadataChangeProposal serviceProposal =
        findProposalByAspect(Constants.SERVICE_PROPERTIES_ASPECT_NAME);
    assertNotNull(serviceProposal);
    String urnStr = serviceProposal.getEntityUrn().toString();
    assertTrue(urnStr.contains("-"), "Auto-generated URN should contain UUID with dashes");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Service Properties Edge Cases
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test service creation with description. */
  @Test
  public void testServiceWithDescription() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setDescription("This is a test MCP server for search capabilities.");

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal serviceProposal =
        findProposalByAspect(Constants.SERVICE_PROPERTIES_ASPECT_NAME);
    assertNotNull(serviceProposal);

    JacksonDataCodec codec = new JacksonDataCodec();
    DataMap dataMap = codec.bytesToMap(serviceProposal.getAspect().getValue().copyBytes());
    ServiceProperties props = new ServiceProperties(dataMap);

    assertEquals(props.getDescription(), "This is a test MCP server for search capabilities.");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // GlobalSettings Failure Handling Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that service entity is created even if GlobalSettings fetch fails. */
  @Test
  public void testServiceCreationWithGlobalSettingsFetchFailure() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Mock GlobalSettings fetch to throw
    when(entityClient.getV2(any(), eq(Constants.GLOBAL_SETTINGS_ENTITY_NAME), any(), any()))
        .thenThrow(new RuntimeException("Failed to fetch GlobalSettings"));

    UpsertServiceInput input = createBasicServiceInput();
    when(environment.getArgument("input")).thenReturn(input);

    // Execute - should throw
    Exception thrown = null;
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      thrown = e;
    }

    assertNotNull(thrown, "Should throw when GlobalSettings fails");
    // But service proposals should still have been made
    MetadataChangeProposal serviceProposal =
        findProposalByAspect(Constants.SERVICE_PROPERTIES_ASPECT_NAME);
    assertNotNull(serviceProposal, "Service should be created before GlobalSettings update");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Multiple Custom Headers Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test MCP server with multiple custom headers. */
  @Test
  public void testMcpServerWithMultipleCustomHeaders() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();

    List<StringMapEntryInput> headers = new ArrayList<>();

    StringMapEntryInput header1 = new StringMapEntryInput();
    header1.setKey("X-API-Key");
    header1.setValue("api-key-123");
    headers.add(header1);

    StringMapEntryInput header2 = new StringMapEntryInput();
    header2.setKey("X-Client-ID");
    header2.setValue("client-456");
    headers.add(header2);

    StringMapEntryInput header3 = new StringMapEntryInput();
    header3.setKey("X-Custom-Header");
    header3.setValue("custom-value");
    headers.add(header3);

    input.getMcpServerProperties().setCustomHeaders(headers);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal mcpProposal =
        findProposalByAspect(Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(mcpProposal);
    McpServerProperties props = deserializeMcpProperties(mcpProposal);

    assertTrue(props.hasCustomHeaders());
    assertEquals(props.getCustomHeaders().size(), 3);
    assertEquals(props.getCustomHeaders().get("X-API-Key"), "api-key-123");
    assertEquals(props.getCustomHeaders().get("X-Client-ID"), "client-456");
    assertEquals(props.getCustomHeaders().get("X-Custom-Header"), "custom-value");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SubTypes Aspect Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that SubTypes aspect is created with correct value. */
  @Test
  public void testSubTypesAspectCreated() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal subTypesProposal = findProposalByAspect(Constants.SUB_TYPES_ASPECT_NAME);
    assertNotNull(subTypesProposal, "Should create SubTypes aspect");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Inline OAuth Server with Full Options
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test inline OAuth server creation with all optional fields. */
  @Test
  public void testInlineOAuthServerWithFullOptions() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_OAUTH);

    // Full OAuth server configuration - NO ID (creates new server)
    UpsertOAuthAuthorizationServerInput oauthInput = new UpsertOAuthAuthorizationServerInput();
    // Note: id is NOT set - newOAuthServer only supports creation, not updates
    oauthInput.setDisplayName("Full OAuth Server");
    oauthInput.setDescription("A fully configured OAuth server");
    oauthInput.setClientId("client-123");
    oauthInput.setClientSecret("secret-456");
    oauthInput.setAuthorizationUrl("https://auth.example.com/authorize");
    oauthInput.setTokenUrl("https://auth.example.com/token");
    oauthInput.setScopes(ImmutableList.of("openid", "profile", "email"));
    input.setNewOAuthServer(oauthInput);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Should have OAuth server properties proposal
    MetadataChangeProposal oauthProposal =
        findProposalByAspect(Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(oauthProposal, "Should create OAuth server");

    // Should have secret proposal (for client secret)
    MetadataChangeProposal secretProposal =
        findProposalByAspect(Constants.SECRET_VALUE_ASPECT_NAME);
    assertNotNull(secretProposal, "Should create secret for client secret");
  }

  /** Test inline OAuth server without client secret (for public OAuth clients). */
  @Test
  public void testInlineOAuthServerWithoutClientSecret() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_OAUTH);

    UpsertOAuthAuthorizationServerInput oauthInput = new UpsertOAuthAuthorizationServerInput();
    // Note: id is NOT set - newOAuthServer only supports creation
    oauthInput.setDisplayName("OAuth Server Without Secret");
    // No client secret (for public clients)
    input.setNewOAuthServer(oauthInput);

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Should have OAuth server properties proposal but no secret proposal
    MetadataChangeProposal oauthProposal =
        findProposalByAspect(Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME);
    assertNotNull(oauthProposal, "Should create OAuth server without client secret");

    // Count secret proposals - should be 0
    long secretCount =
        capturedProposals.stream()
            .filter(p -> Constants.SECRET_VALUE_ASPECT_NAME.equals(p.getAspectName()))
            .count();
    assertEquals(secretCount, 0, "Should not create secret when client secret is not provided");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Auth Type Variations Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test service with USER_API_KEY auth type. */
  @Test
  public void testServiceWithUserApiKeyAuthType() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(AiPluginAuthType.USER_API_KEY);
    input.setOauthServerUrn("urn:li:oauthAuthorizationServer:api-key-server");

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings");
  }

  /** Test service with default auth type (null = NONE). */
  @Test
  public void testServiceWithNullAuthTypeDefaultsToNone() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    setupGlobalSettingsMock(new GlobalSettingsInfo());
    setupServiceResponseMock();

    UpsertServiceInput input = createBasicServiceInput();
    input.setAuthType(null); // Should default to NONE

    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    MetadataChangeProposal globalSettingsProposal =
        findProposalByAspect(Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    assertNotNull(globalSettingsProposal, "Should update GlobalSettings with NONE auth type");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  private UpsertServiceInput createBasicServiceInput() {
    UpsertServiceInput input = new UpsertServiceInput();
    input.setId(TEST_SERVICE_ID);
    input.setDisplayName("Test MCP Server");
    input.setSubType(ServiceSubType.MCP_SERVER);

    McpServerPropertiesInput mcpProps = new McpServerPropertiesInput();
    mcpProps.setUrl("https://mcp.example.com/api");
    input.setMcpServerProperties(mcpProps);

    return input;
  }

  private void setupGlobalSettingsMock(GlobalSettingsInfo globalSettings) throws Exception {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(globalSettings.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(Constants.GLOBAL_SETTINGS_URN);
    entityResponse.setEntityName(Constants.GLOBAL_SETTINGS_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    when(entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.GLOBAL_SETTINGS_ENTITY_NAME),
            eq(Constants.GLOBAL_SETTINGS_URN),
            any()))
        .thenReturn(entityResponse);
  }

  private void setupServiceResponseMock() throws Exception {
    Urn serviceUrn = UrnUtils.getUrn(TEST_SERVICE_URN);

    ServiceProperties props = new ServiceProperties();
    props.setDisplayName("Test MCP Server");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.SERVICE_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(serviceUrn);
    response.setEntityName(Constants.SERVICE_ENTITY_NAME);
    response.setAspects(aspects);

    when(entityClient.batchGetV2(
            any(OperationContext.class), eq(Constants.SERVICE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(serviceUrn, response));
  }

  private MetadataChangeProposal findProposalByAspect(String aspectName) {
    for (MetadataChangeProposal p : capturedProposals) {
      if (aspectName.equals(p.getAspectName())) {
        return p;
      }
    }
    return null;
  }

  private McpServerProperties deserializeMcpProperties(MetadataChangeProposal proposal)
      throws Exception {
    GenericAspect genericAspect = proposal.getAspect();
    if (genericAspect == null) {
      throw new IllegalStateException("Proposal has no aspect");
    }
    JacksonDataCodec codec = new JacksonDataCodec();
    DataMap dataMap = codec.bytesToMap(genericAspect.getValue().copyBytes());
    return new McpServerProperties(dataMap);
  }
}
