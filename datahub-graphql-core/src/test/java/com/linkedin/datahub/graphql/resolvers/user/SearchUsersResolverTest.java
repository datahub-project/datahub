package com.linkedin.datahub.graphql.resolvers.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchUsersResolverTest {

  private static final Urn TEST_USER_URN = Urn.createFromTuple("corpuser", "test");
  private static final String SERVICE_ACCOUNT_SUB_TYPE = "SERVICE_ACCOUNT";

  private EntityClient mockClient;
  private ViewService mockViewService;
  private FormService mockFormService;
  private SearchUsersResolver resolver;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setup() {
    mockClient = Mockito.mock(EntityClient.class);
    mockViewService = Mockito.mock(ViewService.class);
    mockFormService = Mockito.mock(FormService.class);
    resolver = new SearchUsersResolver(mockClient, mockViewService, mockFormService);
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
  }

  @Test
  public void testGetSuccess() throws Exception {
    // Mock search result
    SearchResult searchResult = createMockSearchResult(0, 10, 1);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), anyString(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("test");
    input.setStart(0);
    input.setCount(10);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Data Assertions
    SearchResults result = resolver.get(mockEnv).get();
    assertNotNull(result);
    assertEquals((int) result.getStart(), 0);
    assertEquals((int) result.getCount(), 10);
  }

  @Test
  public void testServiceAccountExclusionFilterApplied() throws Exception {
    // Mock search result
    SearchResult searchResult = createMockSearchResult(0, 10, 0);

    // Capture the filter argument to verify service account exclusion is applied
    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), anyString(), filterCaptor.capture(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("*");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Verify the filter was captured
    Filter capturedFilter = filterCaptor.getValue();
    assertNotNull(capturedFilter);

    // Verify that the filter contains a service account exclusion criterion
    boolean hasServiceAccountExclusion = false;
    if (capturedFilter.hasOr()) {
      for (ConjunctiveCriterion conjunctive : capturedFilter.getOr()) {
        for (Criterion criterion : conjunctive.getAnd()) {
          if ("typeNames".equals(criterion.getField())
              && criterion.getValues().contains(SERVICE_ACCOUNT_SUB_TYPE)
              && criterion.isNegated()) {
            hasServiceAccountExclusion = true;
            break;
          }
        }
      }
    }

    assertTrue(
        hasServiceAccountExclusion,
        "Expected filter to contain service account exclusion (typeNames != SERVICE_ACCOUNT)");
  }

  @Test
  public void testServiceAccountExclusionWithUserFilters() throws Exception {
    // Mock search result
    SearchResult searchResult = createMockSearchResult(0, 10, 0);

    // Capture the filter argument
    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), anyString(), filterCaptor.capture(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // Execute resolver with user-provided filters
    QueryContext mockContext = getMockAllowContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("test");
    input.setFilters(
        ImmutableList.of(
            new FacetFilterInput("status", null, ImmutableList.of("ACTIVE"), false, null)));

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Verify the filter was captured
    Filter capturedFilter = filterCaptor.getValue();
    assertNotNull(capturedFilter);

    // Verify that the filter contains BOTH the user filter AND service account exclusion
    boolean hasStatusFilter = false;
    boolean hasServiceAccountExclusion = false;

    if (capturedFilter.hasOr()) {
      for (ConjunctiveCriterion conjunctive : capturedFilter.getOr()) {
        for (Criterion criterion : conjunctive.getAnd()) {
          if ("status".equals(criterion.getField()) && criterion.getValues().contains("ACTIVE")) {
            hasStatusFilter = true;
          }
          if ("typeNames".equals(criterion.getField())
              && criterion.getValues().contains(SERVICE_ACCOUNT_SUB_TYPE)
              && criterion.isNegated()) {
            hasServiceAccountExclusion = true;
          }
        }
      }
    }

    assertTrue(hasStatusFilter, "Expected filter to contain user-provided status filter");
    assertTrue(
        hasServiceAccountExclusion,
        "Expected filter to contain service account exclusion even with user filters");
  }

  @Test
  public void testDefaultQueryValue() throws Exception {
    // Mock search result
    SearchResult searchResult = createMockSearchResult(0, 10, 0);

    // Capture the query argument
    ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), any(), queryCaptor.capture(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // Execute resolver with null query
    QueryContext mockContext = getMockAllowContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    // No query set - should default to "*"

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Verify the query defaulted to "*"
    assertEquals(queryCaptor.getValue(), "*");
  }

  @Test
  public void testDefaultPagination() throws Exception {
    // Mock search result
    SearchResult searchResult = createMockSearchResult(0, 10, 0);

    // Capture pagination arguments
    ArgumentCaptor<Integer> startCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> countCaptor = ArgumentCaptor.forClass(Integer.class);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(),
                any(),
                anyString(),
                any(),
                startCaptor.capture(),
                countCaptor.capture(),
                any()))
        .thenReturn(searchResult);

    // Execute resolver with no pagination set
    QueryContext mockContext = getMockAllowContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("test");
    // No start/count set - should use defaults

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Verify defaults were used
    assertEquals((int) startCaptor.getValue(), 0);
    assertEquals((int) countCaptor.getValue(), 10);
  }

  @Test
  public void testEntityClientException() throws Exception {
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .searchAcrossEntities(any(), any(), anyString(), any(), anyInt(), anyInt(), any());

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("test");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testUnauthorizedUser() throws Exception {
    // Execute resolver with unauthorized context
    QueryContext mockContext = getMockDenyContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("test");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv));
  }

  @Test
  public void testSearchesOnlyCorpUserEntity() throws Exception {
    // Mock search result
    SearchResult searchResult = createMockSearchResult(0, 10, 0);

    // Capture the entity names argument
    ArgumentCaptor<List<String>> entityNamesCaptor = ArgumentCaptor.forClass(List.class);

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(), entityNamesCaptor.capture(), anyString(), any(), anyInt(), anyInt(), any()))
        .thenReturn(searchResult);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("test");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Verify only CORP_USER entity type is searched
    List<String> capturedEntityNames = entityNamesCaptor.getValue();
    assertEquals(capturedEntityNames.size(), 1);
    assertEquals(capturedEntityNames.get(0), CORP_USER_ENTITY_NAME);
  }

  private SearchResult createMockSearchResult(int from, int pageSize, int numEntities) {
    return new SearchResult()
        .setFrom(from)
        .setPageSize(pageSize)
        .setNumEntities(numEntities)
        .setEntities(new SearchEntityArray(Collections.emptyList()))
        .setMetadata(new SearchResultMetadata());
  }
}
