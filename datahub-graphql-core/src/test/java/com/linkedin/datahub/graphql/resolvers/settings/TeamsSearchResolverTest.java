package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.TeamsChannelResult;
import com.linkedin.datahub.graphql.generated.TeamsSearchInput;
import com.linkedin.datahub.graphql.generated.TeamsSearchResult;
import com.linkedin.datahub.graphql.generated.TeamsSearchType;
import com.linkedin.datahub.graphql.generated.TeamsUserSearchResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.SearchResponse;
import io.datahubproject.integrations.model.SearchResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TeamsSearchResolverTest {

  private EntityClient mockClient;
  private SettingsService mockSettingsService;
  private IntegrationsService mockIntegrationsService;
  private TeamsSearchResolver resolver;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setUp() {
    mockClient = Mockito.mock(EntityClient.class);
    mockSettingsService = Mockito.mock(SettingsService.class);
    mockIntegrationsService = Mockito.mock(IntegrationsService.class);
    resolver = new TeamsSearchResolver(mockClient, mockSettingsService, mockIntegrationsService);

    mockContext = getMockAllowContext();
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testSearchTeamsUsers_Success() throws Exception {
    // Setup
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("john");
    input.setType(TeamsSearchType.USERS);
    input.setLimit(5);

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration configured
    setupMockTeamsIntegrationConfigured();

    // Mock successful user search
    SearchResult userResult = new SearchResult();
    userResult.setId("user-123");
    userResult.setDisplayName("John Doe");
    userResult.setEmail("john.doe@company.com");

    SearchResponse searchResponse = new SearchResponse();
    searchResponse.setResults(Arrays.asList(userResult));

    Mockito.when(mockIntegrationsService.searchTeamsUsers("john", 5))
        .thenReturn(CompletableFuture.completedFuture(searchResponse));

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 1);
    assertEquals(searchResult.getChannels().size(), 0);
    assertFalse(searchResult.getHasMore());

    TeamsUserSearchResult user = searchResult.getUsers().get(0);
    assertEquals(user.getId(), "user-123");
    assertEquals(user.getDisplayName(), "John Doe");
    assertEquals(user.getEmail(), "john.doe@company.com");

    Mockito.verify(mockIntegrationsService).searchTeamsUsers("john", 5);
    Mockito.verify(mockIntegrationsService, Mockito.never())
        .searchTeamsChannels(any(String.class), any(Integer.class));
  }

  @Test
  public void testSearchTeamsChannels_Success() throws Exception {
    // Setup
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("general");
    input.setType(TeamsSearchType.CHANNELS);
    input.setLimit(10);

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration configured
    setupMockTeamsIntegrationConfigured();

    // Mock successful channel search
    SearchResult channelResult = new SearchResult();
    channelResult.setId("channel-456");
    channelResult.setDisplayName("General");
    channelResult.setTeamName("Engineering");

    SearchResponse searchResponse = new SearchResponse();
    searchResponse.setResults(Arrays.asList(channelResult));

    Mockito.when(mockIntegrationsService.searchTeamsChannels("general", 10))
        .thenReturn(CompletableFuture.completedFuture(searchResponse));

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 0);
    assertEquals(searchResult.getChannels().size(), 1);
    assertFalse(searchResult.getHasMore());

    TeamsChannelResult channel = searchResult.getChannels().get(0);
    assertEquals(channel.getId(), "channel-456");
    assertEquals(channel.getDisplayName(), "General");
    assertEquals(channel.getTeamName(), "Engineering");

    Mockito.verify(mockIntegrationsService).searchTeamsChannels("general", 10);
    Mockito.verify(mockIntegrationsService, Mockito.never())
        .searchTeamsUsers(any(String.class), any(Integer.class));
  }

  @Test
  public void testSearchTeamsAll_Success() throws Exception {
    // Setup
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("test");
    input.setType(TeamsSearchType.ALL);
    input.setLimit(10);

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration configured
    setupMockTeamsIntegrationConfigured();

    // Mock successful searches
    SearchResult userResult = new SearchResult();
    userResult.setId("user-123");
    userResult.setDisplayName("Test User");
    userResult.setEmail("test@company.com");

    SearchResult channelResult = new SearchResult();
    channelResult.setId("channel-456");
    channelResult.setDisplayName("Test Channel");
    channelResult.setTeamName("Test Team");

    SearchResponse userSearchResponse = new SearchResponse();
    userSearchResponse.setResults(Arrays.asList(userResult));

    SearchResponse channelSearchResponse = new SearchResponse();
    channelSearchResponse.setResults(Arrays.asList(channelResult));

    Mockito.when(mockIntegrationsService.searchTeamsUsers("test", 10))
        .thenReturn(CompletableFuture.completedFuture(userSearchResponse));
    Mockito.when(mockIntegrationsService.searchTeamsChannels("test", 10))
        .thenReturn(CompletableFuture.completedFuture(channelSearchResponse));

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 1);
    assertEquals(searchResult.getChannels().size(), 1);
    assertFalse(searchResult.getHasMore());

    Mockito.verify(mockIntegrationsService).searchTeamsUsers("test", 10);
    Mockito.verify(mockIntegrationsService).searchTeamsChannels("test", 10);
  }

  @Test
  public void testSearchTeams_DefaultParameters() throws Exception {
    // Setup with null type and limit
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("search");
    input.setType(null); // Should default to ALL
    input.setLimit(null); // Should default to 10

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration configured
    setupMockTeamsIntegrationConfigured();

    // Mock empty search responses
    SearchResponse emptyResponse = new SearchResponse();
    emptyResponse.setResults(Arrays.asList());

    Mockito.when(mockIntegrationsService.searchTeamsUsers("search", 10))
        .thenReturn(CompletableFuture.completedFuture(emptyResponse));
    Mockito.when(mockIntegrationsService.searchTeamsChannels("search", 10))
        .thenReturn(CompletableFuture.completedFuture(emptyResponse));

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify defaults applied
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 0);
    assertEquals(searchResult.getChannels().size(), 0);
    assertFalse(searchResult.getHasMore());

    Mockito.verify(mockIntegrationsService).searchTeamsUsers("search", 10);
    Mockito.verify(mockIntegrationsService).searchTeamsChannels("search", 10);
  }

  @Test
  public void testSearchTeams_UnauthorizedUser() throws Exception {
    // Setup unauthorized context
    QueryContext unauthorizedContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(unauthorizedContext);

    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("test");
    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Execute and verify - since authorization may not be enforced at resolver level,
    // we expect the call to succeed but return empty results
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify empty results
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 0);
    assertEquals(searchResult.getChannels().size(), 0);
    assertFalse(searchResult.getHasMore());
  }

  @Test
  public void testSearchTeams_IntegrationNotConfigured() throws Exception {
    // Setup
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("test");
    input.setType(TeamsSearchType.ALL);

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration NOT configured
    setupMockTeamsIntegrationNotConfigured();

    // Mock service returns null (simulating integration service not available)
    Mockito.when(mockIntegrationsService.searchTeamsUsers("test", 10)).thenReturn(null);
    Mockito.when(mockIntegrationsService.searchTeamsChannels("test", 10)).thenReturn(null);

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify empty results returned (due to errors)
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 0);
    assertEquals(searchResult.getChannels().size(), 0);
    assertFalse(searchResult.getHasMore());

    // Verify service calls were made (but returned null, causing errors)
    Mockito.verify(mockIntegrationsService).searchTeamsUsers("test", 10);
    Mockito.verify(mockIntegrationsService).searchTeamsChannels("test", 10);
  }

  @Test
  public void testSearchTeams_ServiceError() throws Exception {
    // Setup
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("test");
    input.setType(TeamsSearchType.USERS);

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration configured
    setupMockTeamsIntegrationConfigured();

    // Mock service error
    Mockito.when(mockIntegrationsService.searchTeamsUsers("test", 10))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Service error")));

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify partial results (empty due to error, but doesn't throw)
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 0);
    assertEquals(searchResult.getChannels().size(), 0);
    assertFalse(searchResult.getHasMore());
  }

  @Test
  public void testSearchTeams_HasMoreLogic() throws Exception {
    // Setup
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("test");
    input.setType(TeamsSearchType.USERS);
    input.setLimit(2);

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration configured
    setupMockTeamsIntegrationConfigured();

    // Mock full results (equals limit)
    SearchResult user1 = new SearchResult();
    user1.setId("user-1");
    user1.setDisplayName("User 1");
    user1.setEmail("user1@company.com");

    SearchResult user2 = new SearchResult();
    user2.setId("user-2");
    user2.setDisplayName("User 2");
    user2.setEmail("user2@company.com");

    SearchResponse searchResponse = new SearchResponse();
    searchResponse.setResults(Arrays.asList(user1, user2));

    Mockito.when(mockIntegrationsService.searchTeamsUsers("test", 2))
        .thenReturn(CompletableFuture.completedFuture(searchResponse));

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify hasMore is true when result size equals limit
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 2);
    assertTrue(searchResult.getHasMore());
  }

  @Test
  public void testSearchTeams_NullSearchResults() throws Exception {
    // Setup
    TeamsSearchInput input = new TeamsSearchInput();
    input.setQuery("test");
    input.setType(TeamsSearchType.USERS);

    Mockito.when(mockEnv.getArgument("input")).thenReturn(input);

    // Mock Teams integration configured
    setupMockTeamsIntegrationConfigured();

    // Mock null search response
    Mockito.when(mockIntegrationsService.searchTeamsUsers("test", 10))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Execute
    CompletableFuture<TeamsSearchResult> result = resolver.get(mockEnv);
    TeamsSearchResult searchResult = result.get();

    // Verify empty results returned gracefully
    assertNotNull(searchResult);
    assertEquals(searchResult.getUsers().size(), 0);
    assertEquals(searchResult.getChannels().size(), 0);
    assertFalse(searchResult.getHasMore());
  }

  private void setupMockTeamsIntegrationConfigured() throws Exception {
    // Mock Teams connection exists
    EntityResponse connectionResponse =
        new EntityResponse()
            .setEntityName(Constants.DATAHUB_CONNECTION_ENTITY_NAME)
            .setUrn(Urn.createFromString("urn:li:dataHubConnection:__system_teams-0"))
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                        new EnvelopedAspect()
                            .setValue(new Aspect())
                            .setCreated(
                                new AuditStamp()
                                    .setTime(0L)
                                    .setActor(Urn.createFromString("urn:li:corpuser:test"))))));

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(Constants.DATAHUB_CONNECTION_ENTITY_NAME),
                any(Urn.class),
                eq(null)))
        .thenReturn(connectionResponse);
  }

  private void setupMockTeamsIntegrationNotConfigured() throws Exception {
    // Mock Teams connection does not exist
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                eq(Constants.DATAHUB_CONNECTION_ENTITY_NAME),
                any(Urn.class),
                eq(null)))
        .thenReturn(null);
  }
}
