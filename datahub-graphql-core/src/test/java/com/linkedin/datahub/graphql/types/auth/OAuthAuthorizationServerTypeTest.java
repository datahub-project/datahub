package com.linkedin.datahub.graphql.types.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServer;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.oauth.OAuthAuthorizationServerProperties;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OAuthAuthorizationServerTypeTest {

  private static final String SERVER_URN_1 = "urn:li:oauthAuthorizationServer:server1";
  private static final String SERVER_URN_2 = "urn:li:oauthAuthorizationServer:server2";

  @Mock private EntityClient entityClient;
  @Mock private QueryContext queryContext;
  @Mock private OperationContext operationContext;

  private OAuthAuthorizationServerType serverType;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    serverType = new OAuthAuthorizationServerType(entityClient);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Basic Accessor Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testType() {
    assertEquals(serverType.type(), EntityType.OAUTH_AUTHORIZATION_SERVER);
  }

  @Test
  public void testObjectClass() {
    assertEquals(serverType.objectClass(), OAuthAuthorizationServer.class);
  }

  @Test
  public void testGetKeyProvider() {
    OAuthAuthorizationServer server = new OAuthAuthorizationServer();
    server.setUrn(SERVER_URN_1);
    assertEquals(serverType.getKeyProvider().apply(server), SERVER_URN_1);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Constructor Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testConstructorNullEntityClient() {
    assertThrows(NullPointerException.class, () -> new OAuthAuthorizationServerType(null));
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // BatchLoad Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testBatchLoadSingleServer() throws Exception {
    List<String> urns = ImmutableList.of(SERVER_URN_1);
    Urn serverUrn = UrnUtils.getUrn(SERVER_URN_1);

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("OAuth Server 1");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(ImmutableMap.of(serverUrn, entityResponse));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<OAuthAuthorizationServer>> results =
        serverType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertNotNull(results.get(0));
    assertNotNull(results.get(0).getData());
    assertEquals(results.get(0).getData().getUrn(), SERVER_URN_1);
    assertEquals(results.get(0).getData().getProperties().getDisplayName(), "OAuth Server 1");
  }

  @Test
  public void testBatchLoadMultipleServers() throws Exception {
    List<String> urns = ImmutableList.of(SERVER_URN_1, SERVER_URN_2);
    Urn serverUrn1 = UrnUtils.getUrn(SERVER_URN_1);
    Urn serverUrn2 = UrnUtils.getUrn(SERVER_URN_2);

    OAuthAuthorizationServerProperties props1 = new OAuthAuthorizationServerProperties();
    props1.setDisplayName("OAuth Server 1");
    OAuthAuthorizationServerProperties props2 = new OAuthAuthorizationServerProperties();
    props2.setDisplayName("OAuth Server 2");
    EntityResponse response1 = createEntityResponse(serverUrn1, props1);
    EntityResponse response2 = createEntityResponse(serverUrn2, props2);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(ImmutableMap.of(serverUrn1, response1, serverUrn2, response2));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<OAuthAuthorizationServer>> results =
        serverType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    assertEquals(results.get(0).getData().getUrn(), SERVER_URN_1);
    assertEquals(results.get(1).getData().getUrn(), SERVER_URN_2);
  }

  @Test
  public void testBatchLoadEmptyList() throws Exception {
    List<String> urns = Collections.emptyList();

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(Collections.emptyMap());

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<OAuthAuthorizationServer>> results =
        serverType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 0);
  }

  @Test
  public void testBatchLoadServerNotFound() throws Exception {
    List<String> urns = ImmutableList.of(SERVER_URN_1);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(Collections.emptyMap());

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<OAuthAuthorizationServer>> results =
        serverType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertNull(results.get(0));
  }

  @Test
  public void testBatchLoadPartialResults() throws Exception {
    List<String> urns = ImmutableList.of(SERVER_URN_1, SERVER_URN_2);
    Urn serverUrn1 = UrnUtils.getUrn(SERVER_URN_1);

    OAuthAuthorizationServerProperties props1 = new OAuthAuthorizationServerProperties();
    props1.setDisplayName("OAuth Server 1");
    EntityResponse response1 = createEntityResponse(serverUrn1, props1);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(ImmutableMap.of(serverUrn1, response1));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<OAuthAuthorizationServer>> results =
        serverType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    assertNotNull(results.get(0));
    assertNotNull(results.get(0).getData());
    assertNull(results.get(1));
  }

  @Test
  public void testBatchLoadThrowsException() throws Exception {
    List<String> urns = ImmutableList.of(SERVER_URN_1);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenThrow(new RemoteInvocationException("Batch get failed"));

    // Use batchLoadWithoutAuthorization to test exception handling
    assertThrows(
        RuntimeException.class, () -> serverType.batchLoadWithoutAuthorization(urns, queryContext));
  }

  @Test
  public void testBatchLoadPreservesOrder() throws Exception {
    List<String> urns = ImmutableList.of(SERVER_URN_2, SERVER_URN_1);
    Urn serverUrn1 = UrnUtils.getUrn(SERVER_URN_1);
    Urn serverUrn2 = UrnUtils.getUrn(SERVER_URN_2);

    OAuthAuthorizationServerProperties props1 = new OAuthAuthorizationServerProperties();
    props1.setDisplayName("OAuth Server 1");
    OAuthAuthorizationServerProperties props2 = new OAuthAuthorizationServerProperties();
    props2.setDisplayName("OAuth Server 2");
    EntityResponse response1 = createEntityResponse(serverUrn1, props1);
    EntityResponse response2 = createEntityResponse(serverUrn2, props2);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(ImmutableMap.of(serverUrn1, response1, serverUrn2, response2));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<OAuthAuthorizationServer>> results =
        serverType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 2);
    assertEquals(results.get(0).getData().getUrn(), SERVER_URN_2);
    assertEquals(results.get(1).getData().getUrn(), SERVER_URN_1);
  }

  @Test
  public void testBatchLoadWithFullProperties() throws Exception {
    List<String> urns = ImmutableList.of(SERVER_URN_1);
    Urn serverUrn = UrnUtils.getUrn(SERVER_URN_1);

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Full OAuth Server");
    props.setDescription("A fully configured OAuth server");
    props.setClientId("client-123");
    props.setAuthorizationUrl("https://auth.example.com/authorize");
    props.setTokenUrl("https://auth.example.com/token");

    EntityResponse response = createEntityResponse(serverUrn, props);

    when(entityClient.batchGetV2(
            any(OperationContext.class),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(ImmutableMap.of(serverUrn, response));

    // Use batchLoadWithoutAuthorization to test raw data fetching
    List<DataFetcherResult<OAuthAuthorizationServer>> results =
        serverType.batchLoadWithoutAuthorization(urns, queryContext);

    assertNotNull(results);
    assertEquals(results.size(), 1);
    OAuthAuthorizationServer server = results.get(0).getData();
    assertNotNull(server.getProperties());
    assertEquals(server.getProperties().getDisplayName(), "Full OAuth Server");
    assertEquals(server.getProperties().getDescription(), "A fully configured OAuth server");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  private EntityResponse createEntityResponse(Urn urn, OAuthAuthorizationServerProperties props) {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    response.setAspects(aspects);
    return response;
  }
}
