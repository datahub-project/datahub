package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.group.GroupService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DocumentSourceType;
import com.linkedin.datahub.graphql.generated.SearchDocumentsInput;
import com.linkedin.datahub.graphql.generated.SearchDocumentsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchDocumentsResolverTest {

  private static final String TEST_DOCUMENT_URN = "urn:li:document:test-document";
  private static final String TEST_USER_URN = "urn:li:corpuser:test";
  private static final String TEST_GROUP_URN = "urn:li:corpGroup:testGroup";
  private static final String OTHER_USER_URN = "urn:li:corpuser:other";

  private DocumentService mockService;
  private EntityClient mockEntityClient;
  private GroupService mockGroupService;
  private SearchDocumentsResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private SearchDocumentsInput input;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockService = mock(DocumentService.class);
    mockEntityClient = mock(EntityClient.class);
    mockGroupService = mock(GroupService.class);
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

    // Mock EntityClient.batchGetV2 to return a hydrated PUBLISHED entity by default
    Map<com.linkedin.common.urn.Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse =
        createPublishedDocumentResponse(TEST_DOCUMENT_URN, TEST_USER_URN);
    entityResponseMap.put(UrnUtils.getUrn(TEST_DOCUMENT_URN), entityResponse);

    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    // Mock GroupService to return empty groups by default
    when(mockGroupService.getGroupsForUser(any(OperationContext.class), any(Urn.class)))
        .thenReturn(Collections.emptyList());

    resolver = new SearchDocumentsResolver(mockService, mockEntityClient, mockGroupService);
  }

  private EntityResponse createPublishedDocumentResponse(String documentUrn, String ownerUrn) {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(documentUrn));
    entityResponse.setEntityName("document");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    // Add DocumentInfo with PUBLISHED state
    DocumentInfo docInfo = new DocumentInfo();
    DocumentStatus status = new DocumentStatus();
    status.setState(com.linkedin.knowledge.DocumentState.PUBLISHED);
    docInfo.setStatus(status);
    // Add required contents field
    com.linkedin.knowledge.DocumentContents contents =
        new com.linkedin.knowledge.DocumentContents();
    contents.setText("Test content");
    docInfo.setContents(contents);
    // Add required created and lastModified fields
    com.linkedin.common.AuditStamp created = new com.linkedin.common.AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn(ownerUrn));
    docInfo.setCreated(created);
    com.linkedin.common.AuditStamp lastModified = new com.linkedin.common.AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(UrnUtils.getUrn(ownerUrn));
    docInfo.setLastModified(lastModified);
    aspects.put(
        Constants.DOCUMENT_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(docInfo.data())));

    // Add Ownership
    Ownership ownership = new Ownership();
    Owner owner = new Owner();
    owner.setOwner(UrnUtils.getUrn(ownerUrn));
    owner.setType(com.linkedin.common.OwnershipType.TECHNICAL_OWNER);
    ownership.setOwners(new OwnerArray(owner));
    aspects.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(ownership.data())));

    entityResponse.setAspects(aspects);
    return entityResponse;
  }

  private EntityResponse createUnpublishedDocumentResponse(String documentUrn, String ownerUrn) {
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(documentUrn));
    entityResponse.setEntityName("document");

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();

    // Add DocumentInfo with UNPUBLISHED state
    DocumentInfo docInfo = new DocumentInfo();
    DocumentStatus status = new DocumentStatus();
    status.setState(com.linkedin.knowledge.DocumentState.UNPUBLISHED);
    docInfo.setStatus(status);
    // Add required contents field
    com.linkedin.knowledge.DocumentContents contents =
        new com.linkedin.knowledge.DocumentContents();
    contents.setText("Test content");
    docInfo.setContents(contents);
    // Add required created and lastModified fields
    com.linkedin.common.AuditStamp created = new com.linkedin.common.AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn(ownerUrn));
    docInfo.setCreated(created);
    com.linkedin.common.AuditStamp lastModified = new com.linkedin.common.AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(UrnUtils.getUrn(ownerUrn));
    docInfo.setLastModified(lastModified);
    aspects.put(
        Constants.DOCUMENT_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(docInfo.data())));

    // Add Ownership
    Ownership ownership = new Ownership();
    Owner owner = new Owner();
    owner.setOwner(UrnUtils.getUrn(ownerUrn));
    owner.setType(com.linkedin.common.OwnershipType.TECHNICAL_OWNER);
    ownership.setOwners(new OwnerArray(owner));
    aspects.put(
        Constants.OWNERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(ownership.data())));

    entityResponse.setAspects(aspects);
    return entityResponse;
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
    input.setParentDocuments(ImmutableList.of("urn:li:document:parent"));

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called with filters
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsWithRootOnly() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setRootOnly(true);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called with rootOnly filter
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsWithParentDocumentsAndRootOnly() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // When both are set, parentDocuments takes precedence
    input.setParentDocuments(ImmutableList.of("urn:li:document:parent"));
    input.setRootOnly(true);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called (parentDocuments filter should be used, not rootOnly)
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

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called (the filter will contain state=PUBLISHED by default)
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("test query"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testSearchDocumentsWithSourceType() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setSourceType(DocumentSourceType.NATIVE);

    resolver.get(mockEnv).get();

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class),
            eq("test query"),
            filterCaptor.capture(),
            any(),
            eq(0),
            eq(10));

    Filter filter = filterCaptor.getValue();
    assertNotNull(filter, "Filter should not be null");
    assertNotNull(filter.getOr(), "Filter OR clause should not be null");

    boolean hasSourceType =
        filter.getOr().stream()
            .flatMap(cc -> cc.getAnd().stream())
            .anyMatch(
                c ->
                    "sourceType".equals(c.getField())
                        && c.getValues() != null
                        && c.getValues().contains("NATIVE")
                        && c.getCondition() == Condition.EQUAL);

    assertTrue(hasSourceType, "Filter should contain sourceType=NATIVE");
  }

  @Test
  public void testSearchDocumentsWithRelatedAssets() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setRelatedAssets(ImmutableList.of("urn:li:dataset:test"));

    resolver.get(mockEnv).get();

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class),
            eq("test query"),
            filterCaptor.capture(),
            any(),
            eq(0),
            eq(10));

    Filter filter = filterCaptor.getValue();
    assertNotNull(filter, "Filter should not be null");

    boolean hasRelatedAssets =
        filter.getOr().stream()
            .flatMap(cc -> cc.getAnd().stream())
            .anyMatch(
                c ->
                    "relatedAssets".equals(c.getField())
                        && c.getValues() != null
                        && c.getValues().contains("urn:li:dataset:test")
                        && c.getCondition() == Condition.EQUAL);

    assertTrue(hasRelatedAssets, "Filter should contain relatedAssets=urn:li:dataset:test");
  }

  @Test
  public void testSearchDocumentsDefaultSourceType() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setSourceType(null);

    resolver.get(mockEnv).get();

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class),
            eq("test query"),
            filterCaptor.capture(),
            any(),
            eq(0),
            eq(10));

    Filter filter = filterCaptor.getValue();
    if (filter != null) {
      boolean hasSourceType =
          filter.getOr().stream()
              .flatMap(cc -> cc.getAnd().stream())
              .anyMatch(c -> "sourceType".equals(c.getField()));

      assertFalse(hasSourceType, "Filter should NOT contain sourceType when not provided");
    }
  }

  @Test
  public void testPublishedDocumentShownToAllUsers() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Document is PUBLISHED and owned by TEST_USER
    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    entityResponseMap.put(
        UrnUtils.getUrn(TEST_DOCUMENT_URN),
        createPublishedDocumentResponse(TEST_DOCUMENT_URN, TEST_USER_URN));
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getDocuments().size(), 1, "PUBLISHED document should be shown to user");
  }

  @Test
  public void testUnpublishedDocumentShownToOwner() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Document is UNPUBLISHED and owned by TEST_USER (current user)
    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    entityResponseMap.put(
        UrnUtils.getUrn(TEST_DOCUMENT_URN),
        createUnpublishedDocumentResponse(TEST_DOCUMENT_URN, TEST_USER_URN));
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getDocuments().size(), 1, "UNPUBLISHED document should be shown to owner");
  }

  @Test
  public void testUnpublishedDocumentHiddenFromNonOwner() throws Exception {
    QueryContext mockContext = getMockAllowContext(OTHER_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // With filter-based ownership, the search service itself filters out unpublished docs
    // not owned by the user, so we mock an empty search result
    SearchResult emptySearchResult = new SearchResult();
    emptySearchResult.setFrom(0);
    emptySearchResult.setPageSize(10);
    emptySearchResult.setNumEntities(0);
    emptySearchResult.setEntities(new SearchEntityArray());
    emptySearchResult.setMetadata(new SearchResultMetadata());

    when(mockService.searchDocuments(
            any(OperationContext.class),
            any(String.class),
            any(),
            any(),
            any(Integer.class),
            any(Integer.class)))
        .thenReturn(emptySearchResult);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(
        result.getDocuments().size(),
        0,
        "UNPUBLISHED document should be hidden from non-owner via search filtering");
  }

  @Test
  public void testUnpublishedDocumentShownToGroupMember() throws Exception {
    QueryContext mockContext = getMockAllowContext(OTHER_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Document is UNPUBLISHED and owned by a GROUP that the current user belongs to
    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse =
        createUnpublishedDocumentResponse(TEST_DOCUMENT_URN, TEST_GROUP_URN);
    entityResponseMap.put(UrnUtils.getUrn(TEST_DOCUMENT_URN), entityResponse);
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    // Mock that the current user is part of TEST_GROUP
    when(mockGroupService.getGroupsForUser(
            any(OperationContext.class), eq(UrnUtils.getUrn(OTHER_USER_URN))))
        .thenReturn(ImmutableList.of(UrnUtils.getUrn(TEST_GROUP_URN)));

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(
        result.getDocuments().size(), 1, "UNPUBLISHED document should be shown to group member");
  }

  @Test
  public void testPublishedDocumentShownEvenIfNotOwner() throws Exception {
    QueryContext mockContext = getMockAllowContext(OTHER_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Document is PUBLISHED and owned by TEST_USER (not the current user)
    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    entityResponseMap.put(
        UrnUtils.getUrn(TEST_DOCUMENT_URN),
        createPublishedDocumentResponse(TEST_DOCUMENT_URN, TEST_USER_URN));
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    SearchDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(
        result.getDocuments().size(),
        1,
        "PUBLISHED document should be shown to all users regardless of ownership");
  }
}
