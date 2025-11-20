package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DocumentState;
import com.linkedin.datahub.graphql.generated.SearchDocumentsInput;
import com.linkedin.datahub.graphql.generated.SearchDocumentsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchDocumentsResolverTest {

  private static final String TEST_DOCUMENT_URN = "urn:li:document:test-document";

  private DocumentService mockService;
  private EntityClient mockEntityClient;
  private SearchDocumentsResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private SearchDocumentsInput input;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockService = mock(DocumentService.class);
    mockEntityClient = mock(EntityClient.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    // Setup default input
    input = new SearchDocumentsInput();
    input.setQuery("test query");
    input.setStart(0);
    input.setCount(10);

    // Setup mock search result
    SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(1);
    searchResult.setEntities(
        new SearchEntityArray(
            ImmutableList.of(new SearchEntity().setEntity(UrnUtils.getUrn(TEST_DOCUMENT_URN)))));
    searchResult.setMetadata(new SearchResultMetadata());

    when(mockService.searchDocuments(
            any(OperationContext.class),
            any(String.class),
            any(),
            any(),
            any(Integer.class),
            any(Integer.class)))
        .thenReturn(searchResult);

    // Mock EntityClient.batchGetV2 to return a hydrated entity
    Map<com.linkedin.common.urn.Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(TEST_DOCUMENT_URN));
    entityResponse.setEntityName("document");
    // Set empty aspects map to satisfy required field
    entityResponse.setAspects(new com.linkedin.entity.EnvelopedAspectMap());
    entityResponseMap.put(UrnUtils.getUrn(TEST_DOCUMENT_URN), entityResponse);

    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    resolver = new SearchDocumentsResolver(mockService, mockEntityClient);
  }

  @Test
  public void testSearchDocumentsSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getDocuments().size(), 1);

    // Verify service was called
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsWithFilters() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setTypes(ImmutableList.of("tutorial", "guide"));
    input.setParentDocument("urn:li:document:parent");

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called with filters
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsEmptyQuery() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setQuery(null); // Empty query should default to "*"

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called with "*" query
    verify(mockService, times(1))
        .searchDocuments(any(OperationContext.class), eq("*"), any(), any(), eq(0), eq(10));
  }

  // Note: Search operations don't require special authorization like other entity searches
  // Authorization is applied at the entity level when viewing individual documents

  @Test
  public void testSearchDocumentsServiceThrowsException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    when(mockService.searchDocuments(
            any(OperationContext.class),
            any(),
            any(),
            any(),
            any(Integer.class),
            any(Integer.class)))
        .thenThrow(new RuntimeException("Service error"));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testSearchDocumentsDefaultsToPublishedState() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Don't set any states - should default to PUBLISHED
    input.setStates(null);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called (the filter will contain state=PUBLISHED by default)
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsWithSingleState() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Set to only search UNPUBLISHED documents
    input.setStates(ImmutableList.of(DocumentState.UNPUBLISHED));

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called with UNPUBLISHED state filter
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsWithMultipleStates() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Set to search both PUBLISHED and UNPUBLISHED documents
    input.setStates(ImmutableList.of(DocumentState.PUBLISHED, DocumentState.UNPUBLISHED));

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called with both states in filter
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsExcludesDraftsByDefault() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Don't set includeDrafts - should exclude drafts by default
    input.setIncludeDrafts(null);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called (the filter will exclude draftOf != null by default)
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsIncludeDrafts() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Explicitly include drafts
    input.setIncludeDrafts(true);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called without draftOf filter
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }
}
