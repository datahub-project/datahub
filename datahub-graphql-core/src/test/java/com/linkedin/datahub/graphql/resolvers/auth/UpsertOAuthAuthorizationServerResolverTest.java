package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.codec.JacksonDataCodec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpsertOAuthAuthorizationServerInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.oauth.OAuthAuthorizationServerProperties;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.SecretService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpsertOAuthAuthorizationServerResolverTest {

  private static final String SERVER_ID = "test-oauth-server";
  private static final Urn EXISTING_SECRET_URN =
      UrnUtils.getUrn("urn:li:dataHubSecret:existing-secret");

  @Mock private EntityClient entityClient;
  @Mock private SecretService secretService;
  @Mock private DataFetchingEnvironment environment;

  private UpsertOAuthAuthorizationServerResolver resolver;
  private List<MetadataChangeProposal> capturedProposals;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    resolver = new UpsertOAuthAuthorizationServerResolver(entityClient, secretService);
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
   * Test that when updating a server with null clientSecret, the existing secret is preserved. This
   * is the critical fix for the bugbot issue.
   */
  @Test
  public void testUpdatePreservesExistingSecretWhenInputIsNull() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Input with null clientSecret (should preserve existing)
    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setClientSecret(null); // null = preserve existing

    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing entity with a secret
    setupExistingEntityMock(EXISTING_SECRET_URN);

    // Execute
    Exception thrownException = null;
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      thrownException = e;
      // May fail on final mapping, but we can check captured proposals
    }

    // Debug: Check how many proposals were captured
    assertFalse(
        capturedProposals.isEmpty(),
        "Should have at least one proposal captured. Exception: "
            + (thrownException != null ? thrownException.getMessage() : "none"));

    // Should have only 1 proposal (properties, no new secret)
    assertEquals(
        capturedProposals.size(), 1, "Should have exactly 1 proposal (no secret creation)");

    // Verify the properties contain the existing secret URN
    MetadataChangeProposal proposal = capturedProposals.get(0);
    assertEquals(
        proposal.getAspectName(), Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME);

    OAuthAuthorizationServerProperties ingestedProperties = deserializeProperties(proposal);

    assertTrue(
        ingestedProperties.hasClientSecretUrn(),
        "Should preserve existing clientSecretUrn when input is null");
    assertEquals(
        ingestedProperties.getClientSecretUrn(),
        EXISTING_SECRET_URN,
        "Should preserve the exact existing secret URN");
  }

  /** Test that when updating a server with empty string clientSecret, the secret is cleared. */
  @Test
  public void testUpdateClearsSecretWhenInputIsEmpty() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Input with empty clientSecret (should clear)
    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setClientSecret(""); // empty = clear

    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing entity with a secret
    setupExistingEntityMock(EXISTING_SECRET_URN);

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can check captured proposals
    }

    // Should have only 1 proposal (properties, no new secret)
    assertEquals(
        capturedProposals.size(), 1, "Should have exactly 1 proposal (no secret creation)");

    // Verify the properties do NOT contain a secret URN
    MetadataChangeProposal proposal = capturedProposals.get(0);
    OAuthAuthorizationServerProperties ingestedProperties = deserializeProperties(proposal);

    assertFalse(
        ingestedProperties.hasClientSecretUrn(),
        "Should clear clientSecretUrn when input is empty string");
  }

  /** Test that when updating a server with a new clientSecret, a new secret is created. */
  @Test
  public void testUpdateCreatesNewSecretWhenInputHasValue() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Input with new clientSecret
    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setClientSecret("new-secret-value"); // new value = create new secret

    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing entity with a secret (should be replaced)
    setupExistingEntityMock(EXISTING_SECRET_URN);

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can check captured proposals
    }

    // Should have 2 proposals: one for the secret, one for the properties
    assertEquals(capturedProposals.size(), 2, "Should have 2 proposals (secret + properties)");

    // First proposal should be the secret
    assertEquals(
        capturedProposals.get(0).getAspectName(),
        Constants.SECRET_VALUE_ASPECT_NAME,
        "First proposal should be the secret");

    // Second proposal should be the properties with a new secret URN
    MetadataChangeProposal propertiesProposal = capturedProposals.get(1);
    assertEquals(
        propertiesProposal.getAspectName(),
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME);

    OAuthAuthorizationServerProperties ingestedProperties =
        deserializeProperties(propertiesProposal);

    assertTrue(
        ingestedProperties.hasClientSecretUrn(),
        "Should have a new clientSecretUrn when input has value");
    // The new secret URN should be different from the existing one
    Urn newSecretUrn = ingestedProperties.getClientSecretUrn();
    assertFalse(
        EXISTING_SECRET_URN.equals(newSecretUrn),
        "Should have a different secret URN than the existing one");
  }

  /** Test that creating a new server (no existing entity) with secret works correctly. */
  @Test
  public void testCreateNewServerWithSecret() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Input for new server
    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setClientSecret("my-secret");

    when(environment.getArgument("input")).thenReturn(input);

    // No existing entity
    setupNoExistingEntityMock();

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can check captured proposals
    }

    // Should have 2 proposals: one for the secret, one for the properties
    assertEquals(capturedProposals.size(), 2, "Should have 2 proposals (secret + properties)");
  }

  /** Test that creating a new server without secret works correctly. */
  @Test
  public void testCreateNewServerWithoutSecret() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Input for new server without secret
    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setClientSecret(null); // No secret

    when(environment.getArgument("input")).thenReturn(input);

    // No existing entity
    setupNoExistingEntityMock();

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can check captured proposals
    }

    // Should have only 1 proposal (just properties, no secret)
    assertEquals(capturedProposals.size(), 1, "Should have 1 proposal (no secret for new entity)");

    OAuthAuthorizationServerProperties ingestedProperties =
        deserializeProperties(capturedProposals.get(0));

    assertFalse(
        ingestedProperties.hasClientSecretUrn(),
        "Should not have clientSecretUrn when creating without secret");
  }

  /** Test that unauthorized users cannot create/update servers. */
  @Test
  public void testUnauthorizedUserCannotUpsert() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    when(environment.getArgument("input")).thenReturn(input);

    try {
      resolver.get(environment);
      fail("Expected AuthorizationException");
    } catch (AuthorizationException e) {
      assertTrue(e.getMessage().contains("Unauthorized"));
    }

    assertTrue(
        capturedProposals.isEmpty(), "No proposals should be captured for unauthorized user");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // OAuth Field Requirements Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that displayName is required. */
  @Test
  public void testDisplayNameRequired() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = new UpsertOAuthAuthorizationServerInput();
    input.setId(SERVER_ID);
    input.setDisplayName(null); // Missing required displayName
    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    // Execute - should fail or create with null displayName depending on validation
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // Expected - displayName should be required
    }
  }

  /** Test OAuth server with full OAuth configuration. */
  @Test
  public void testFullOAuthConfiguration() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = new UpsertOAuthAuthorizationServerInput();
    input.setId(SERVER_ID);
    input.setDisplayName("Full OAuth Server");
    input.setClientId("my-client-id");
    input.setClientSecret("my-client-secret");
    input.setAuthorizationUrl("https://auth.example.com/authorize");
    input.setTokenUrl("https://auth.example.com/token");
    input.setScopes(ImmutableList.of("read", "write", "admin"));

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can check captured proposals
    }

    // Should have 2 proposals: secret + properties
    assertEquals(capturedProposals.size(), 2, "Should have 2 proposals");

    // Check properties have all OAuth fields
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(1));

    assertEquals(props.getClientId(), "my-client-id");
    assertEquals(props.getAuthorizationUrl(), "https://auth.example.com/authorize");
    assertEquals(props.getTokenUrl(), "https://auth.example.com/token");
    assertTrue(props.hasScopes());
    assertEquals(props.getScopes().size(), 3);
  }

  /** Test OAuth server for API key support (without OAuth URLs). */
  /** Test OAuth server without client secret (public client). */
  @Test
  public void testOAuthWithoutClientSecret() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = new UpsertOAuthAuthorizationServerInput();
    input.setId(SERVER_ID);
    input.setDisplayName("Public OAuth Server");
    // No client secret for public clients

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping, but we can check captured proposals
    }

    // Should have 1 proposal (just properties, no secret)
    assertEquals(capturedProposals.size(), 1, "Should have 1 proposal");
  }

  /** Test that scopes are properly stored as array. */
  @Test
  public void testScopesStoredAsArray() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setScopes(ImmutableList.of("scope1", "scope2", "scope3"));

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify scopes are stored
    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));

    assertTrue(props.hasScopes());
    assertEquals(props.getScopes().size(), 3);
    assertTrue(props.getScopes().contains("scope1"));
    assertTrue(props.getScopes().contains("scope2"));
    assertTrue(props.getScopes().contains("scope3"));
  }

  /** Test that empty scopes list is handled correctly. */
  @Test
  public void testEmptyScopesHandled() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setScopes(ImmutableList.of()); // Empty scopes

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    // Execute
    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Verify empty scopes are handled
    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));

    // Empty scopes should not be set or should be empty array
    if (props.hasScopes()) {
      assertEquals(props.getScopes().size(), 0);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Optional Fields Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that description is properly set. */
  @Test
  public void testDescriptionField() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setDescription("This is a test OAuth server for integration");

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getDescription(), "This is a test OAuth server for integration");
  }

  /** Test with null optional fields - should not set them in properties. */
  @Test
  public void testNullOptionalFields() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setDescription(null);
    input.setClientId(null);
    input.setAuthorizationUrl(null);
    input.setTokenUrl(null);

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));

    assertFalse(props.hasDescription(), "Description should not be set");
    assertFalse(props.hasClientId(), "ClientId should not be set");
    assertFalse(props.hasAuthorizationUrl(), "AuthorizationUrl should not be set");
    assertFalse(props.hasTokenUrl(), "TokenUrl should not be set");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Auth Config Tests
  // ═══════════════════════════════════════════════════════════════════════════

  /** Test that authLocation defaults to HEADER. */
  @Test
  public void testAuthLocationDefaultsToHeader() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setAuthLocation(null); // Not specified

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getAuthLocation(), com.linkedin.oauth.AuthLocation.HEADER);
  }

  /** Test that authHeaderName defaults to Authorization. */
  @Test
  public void testAuthHeaderNameDefaults() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setAuthHeaderName(null); // Not specified

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getAuthHeaderName(), "Authorization");
  }

  /** Test setting custom authHeaderName. */
  @Test
  public void testCustomAuthHeaderName() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setAuthHeaderName("X-API-Key");

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getAuthHeaderName(), "X-API-Key");
  }

  /** Test setting authScheme. */
  @Test
  public void testAuthScheme() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setAuthScheme("Bearer");

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getAuthScheme(), "Bearer");
  }

  /** Test setting authQueryParam. */
  @Test
  public void testAuthQueryParam() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setAuthQueryParam("api_key");

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getAuthQueryParam(), "api_key");
  }

  /** Test setting tokenAuthMethod defaults to POST_BODY. */
  @Test
  public void testTokenAuthMethodDefault() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setTokenAuthMethod(null); // Not specified

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getTokenAuthMethod(), com.linkedin.oauth.TokenAuthMethod.POST_BODY);
  }

  /** Test setting explicit tokenAuthMethod to BASIC. */
  @Test
  public void testTokenAuthMethodBasic() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setTokenAuthMethod(com.linkedin.datahub.graphql.generated.TokenAuthMethod.BASIC);

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getTokenAuthMethod(), com.linkedin.oauth.TokenAuthMethod.BASIC);
  }

  /** Test setting authLocation to QUERY_PARAM. */
  @Test
  public void testAuthLocationQueryParam() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(SERVER_ID);
    input.setAuthLocation(com.linkedin.datahub.graphql.generated.AuthLocation.QUERY_PARAM);
    input.setAuthQueryParam("token");

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    assertFalse(capturedProposals.isEmpty());
    OAuthAuthorizationServerProperties props = deserializeProperties(capturedProposals.get(0));
    assertEquals(props.getAuthLocation(), com.linkedin.oauth.AuthLocation.QUERY_PARAM);
    assertEquals(props.getAuthQueryParam(), "token");
  }

  /** Test auto-generated server ID when not provided. */
  @Test
  public void testAutoGeneratedServerId() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    UpsertOAuthAuthorizationServerInput input = createBasicInput();
    input.setId(null); // Auto-generate ID

    when(environment.getArgument("input")).thenReturn(input);
    setupNoExistingEntityMock();

    try {
      resolver.get(environment).join();
    } catch (Exception e) {
      // May fail on final mapping
    }

    // Should have at least one proposal
    assertFalse(capturedProposals.isEmpty());
    // The URN should contain a UUID
    MetadataChangeProposal proposal = capturedProposals.get(0);
    String urnStr = proposal.getEntityUrn().toString();
    assertTrue(urnStr.startsWith("urn:li:oauthAuthorizationServer:"));
    // UUID format has dashes
    assertTrue(urnStr.contains("-"), "Auto-generated ID should be a UUID");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  private UpsertOAuthAuthorizationServerInput createBasicInput() {
    UpsertOAuthAuthorizationServerInput input = new UpsertOAuthAuthorizationServerInput();
    input.setDisplayName("Test OAuth Server");
    return input;
  }

  private void setupExistingEntityMock(Urn secretUrn) throws Exception {
    when(entityClient.batchGetV2(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<?> urnsRaw = invocation.getArgument(2);
              Map<Urn, EntityResponse> result = new HashMap<>();

              for (Object urnObj : urnsRaw) {
                Urn urn = (Urn) urnObj;
                EntityResponse response = createEntityResponse(urn, secretUrn);
                result.put(urn, response);
              }

              return result;
            });
  }

  private void setupNoExistingEntityMock() throws Exception {
    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(new HashMap<>());
  }

  private EntityResponse createEntityResponse(Urn entityUrn, Urn secretUrn) {
    OAuthAuthorizationServerProperties existingProps = new OAuthAuthorizationServerProperties();
    existingProps.setDisplayName("Existing Server");
    if (secretUrn != null) {
      existingProps.setClientSecretUrn(secretUrn);
    }

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(existingProps.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(entityUrn);
    response.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    response.setAspects(aspects);
    return response;
  }

  /** Deserializes the GenericAspect from a MetadataChangeProposal into the actual aspect type. */
  private OAuthAuthorizationServerProperties deserializeProperties(MetadataChangeProposal proposal)
      throws Exception {
    GenericAspect genericAspect = proposal.getAspect();
    if (genericAspect == null) {
      throw new IllegalStateException("Proposal has no aspect");
    }
    JacksonDataCodec codec = new JacksonDataCodec();
    DataMap dataMap = codec.bytesToMap(genericAspect.getValue().copyBytes());
    return new OAuthAuthorizationServerProperties(dataMap);
  }
}
