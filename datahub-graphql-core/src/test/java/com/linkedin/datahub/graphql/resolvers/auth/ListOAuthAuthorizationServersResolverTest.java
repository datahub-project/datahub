package com.linkedin.datahub.graphql.resolvers.auth;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListOAuthAuthorizationServersInput;
import com.linkedin.datahub.graphql.generated.ListOAuthAuthorizationServersResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.oauth.OAuthAuthorizationServerProperties;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListOAuthAuthorizationServersResolverTest {

  private static final String SERVER_URN_1 = "urn:li:oauthAuthorizationServer:server-1";
  private static final String SERVER_URN_2 = "urn:li:oauthAuthorizationServer:server-2";
  private static final Urn SERVER_URN_1_OBJ = UrnUtils.getUrn(SERVER_URN_1);
  private static final Urn SERVER_URN_2_OBJ = UrnUtils.getUrn(SERVER_URN_2);

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;

  private ListOAuthAuthorizationServersResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new ListOAuthAuthorizationServersResolver(entityClient);
  }

  @Test
  public void testConstructorNullCheck() {
    assertThrows(NullPointerException.class, () -> new ListOAuthAuthorizationServersResolver(null));
  }

  @Test
  public void testListServersSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(new ListOAuthAuthorizationServersInput());

    // Mock search result
    SearchResult searchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(2)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(
                        new SearchEntity().setEntity(SERVER_URN_1_OBJ),
                        new SearchEntity().setEntity(SERVER_URN_2_OBJ))))
            .setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            eq(""),
            any(),
            any(),
            eq(0),
            eq(20)))
        .thenReturn(searchResult);

    // Mock entity responses
    Map<Urn, EntityResponse> entityResponses = new HashMap<>();
    entityResponses.put(SERVER_URN_1_OBJ, createEntityResponse(SERVER_URN_1_OBJ, "Server 1"));
    entityResponses.put(SERVER_URN_2_OBJ, createEntityResponse(SERVER_URN_2_OBJ, "Server 2"));

    when(entityClient.batchGetV2(
            any(), eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME), any(), any()))
        .thenReturn(entityResponses);

    // Execute
    ListOAuthAuthorizationServersResult result = resolver.get(environment).join();

    // Verify
    assertNotNull(result);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getAuthorizationServers().size(), 2);
  }

  @Test
  public void testListServersWithQuery() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    ListOAuthAuthorizationServersInput input = new ListOAuthAuthorizationServersInput();
    input.setQuery("glean");
    when(environment.getArgument("input")).thenReturn(input);

    // Mock search result
    SearchResult searchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(20)
            .setNumEntities(1)
            .setEntities(
                new SearchEntityArray(
                    ImmutableList.of(new SearchEntity().setEntity(SERVER_URN_1_OBJ))))
            .setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            eq("glean"),
            any(),
            any(),
            eq(0),
            eq(20)))
        .thenReturn(searchResult);

    // Mock entity response
    Map<Urn, EntityResponse> entityResponses = new HashMap<>();
    entityResponses.put(SERVER_URN_1_OBJ, createEntityResponse(SERVER_URN_1_OBJ, "Glean Server"));
    when(entityClient.batchGetV2(
            any(), eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME), any(), any()))
        .thenReturn(entityResponses);

    // Execute
    ListOAuthAuthorizationServersResult result = resolver.get(environment).join();

    // Verify query was passed correctly
    assertNotNull(result);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getAuthorizationServers().size(), 1);
  }

  @Test
  public void testListServersWithPagination() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    ListOAuthAuthorizationServersInput input = new ListOAuthAuthorizationServersInput();
    input.setStart(10);
    input.setCount(5);
    when(environment.getArgument("input")).thenReturn(input);

    // Mock search result
    SearchResult searchResult =
        new SearchResult()
            .setFrom(10)
            .setPageSize(5)
            .setNumEntities(15)
            .setEntities(new SearchEntityArray())
            .setMetadata(new SearchResultMetadata());

    when(entityClient.search(
            any(),
            eq(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME),
            eq(""),
            any(),
            any(),
            eq(10),
            eq(5)))
        .thenReturn(searchResult);

    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(new HashMap<>());

    // Execute
    ListOAuthAuthorizationServersResult result = resolver.get(environment).join();

    // Verify pagination
    assertNotNull(result);
    assertEquals(result.getStart(), 10);
    assertEquals(result.getCount(), 5);
    assertEquals(result.getTotal(), 15);
  }

  @Test
  public void testListServersEmpty() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(new ListOAuthAuthorizationServersInput());

    // Mock empty search result
    SearchResult searchResult =
        new SearchResult()
            .setFrom(0)
            .setPageSize(20)
            .setNumEntities(0)
            .setEntities(new SearchEntityArray())
            .setMetadata(new SearchResultMetadata());

    when(entityClient.search(any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(new HashMap<>());

    // Execute
    ListOAuthAuthorizationServersResult result = resolver.get(environment).join();

    // Verify
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertEquals(result.getAuthorizationServers().size(), 0);
  }

  @Test
  public void testListServersUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(new ListOAuthAuthorizationServersInput());

    // Expect AuthorizationException
    assertThrows(AuthorizationException.class, () -> resolver.get(environment));

    // Verify no search occurred
    verify(entityClient, never()).search(any(), any(), any(), any(), any(), anyInt(), anyInt());
  }

  @Test
  public void testListServersSearchException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(new ListOAuthAuthorizationServersInput());

    when(entityClient.search(any(), any(), any(), any(), any(), anyInt(), anyInt()))
        .thenThrow(new RemoteInvocationException("Search failed"));

    // Should throw wrapped exception
    assertThrows(CompletionException.class, () -> resolver.get(environment).join());
  }

  /**
   * Test that null input throws NPE (resolver requires input to be non-null). This documents the
   * current behavior.
   */
  @Test
  public void testListServersNullInputThrowsNPE() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("input")).thenReturn(null);

    // Execute - should throw NPE since input is required
    assertThrows(NullPointerException.class, () -> resolver.get(environment).join());
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  private EntityResponse createEntityResponse(Urn urn, String displayName) {
    OAuthAuthorizationServerProperties properties = new OAuthAuthorizationServerProperties();
    properties.setDisplayName(displayName);

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(properties.data())));

    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    response.setAspects(aspects);
    return response;
  }
}
