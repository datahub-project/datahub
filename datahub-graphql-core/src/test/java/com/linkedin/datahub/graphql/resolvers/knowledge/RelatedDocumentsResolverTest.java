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
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RelatedDocumentsInput;
import com.linkedin.datahub.graphql.generated.RelatedDocumentsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
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
import java.util.concurrent.ExecutionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RelatedDocumentsResolverTest {

  private static final String TEST_DOCUMENT_URN = "urn:li:document:test-document";
  private static final String TEST_PARENT_ENTITY_URN = "urn:li:dataset:test-dataset";
  private static final String TEST_USER_URN = "urn:li:corpuser:test";
  private static final String TEST_GROUP_URN = "urn:li:corpGroup:testGroup";
  private static final String OTHER_USER_URN = "urn:li:corpuser:other";

  private DocumentService mockService;
  private EntityClient mockEntityClient;
  private GroupService mockGroupService;
  private RelatedDocumentsResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private RelatedDocumentsInput input;
  private Entity mockParentEntity;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockService = mock(DocumentService.class);
    mockEntityClient = mock(EntityClient.class);
    mockGroupService = mock(GroupService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    // Setup mock parent entity
    mockParentEntity = mock(Entity.class);
    when(mockParentEntity.getUrn()).thenReturn(TEST_PARENT_ENTITY_URN);
    when(mockParentEntity.getType()).thenReturn(EntityType.DATASET);
    when(mockEnv.getSource()).thenReturn(mockParentEntity);

    // Setup default input
    input = new RelatedDocumentsInput();
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

    resolver = new RelatedDocumentsResolver(mockService, mockEntityClient, mockGroupService);
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
  public void testContextDocumentsSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getDocuments().size(), 1);

    // Verify service was called with "*" query (no semantic search)
    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(mockService, times(1))
        .searchDocuments(
            any(OperationContext.class), eq("*"), filterCaptor.capture(), any(), eq(0), eq(10));

    // Verify that relatedAssets filter was automatically added with parent entity URN
    Filter capturedFilter = filterCaptor.getValue();
    assertNotNull(capturedFilter);
    assertNotNull(capturedFilter.getOr());
    assertEquals(capturedFilter.getOr().size(), 2); // PUBLISHED OR UNPUBLISHED owned

    // Check that relatedAssets filter is in both clauses
    boolean foundRelatedAssetsFilter = false;
    for (com.linkedin.metadata.query.filter.ConjunctiveCriterion conj : capturedFilter.getOr()) {
      if (conj.getAnd() != null) {
        for (Criterion criterion : conj.getAnd()) {
          if ("relatedAssets".equals(criterion.getField())
              && Condition.EQUAL.equals(criterion.getCondition())
              && criterion.getValues() != null
              && criterion.getValues().contains(TEST_PARENT_ENTITY_URN)) {
            foundRelatedAssetsFilter = true;
            break;
          }
        }
      }
    }
    assertTrue(foundRelatedAssetsFilter, "relatedAssets filter with parent URN should be present");
  }

  @Test
  public void testContextDocumentsWithAdditionalFilters() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setTypes(ImmutableList.of("tutorial", "guide"));
    input.setParentDocuments(ImmutableList.of("urn:li:document:parent"));
    input.setDomains(ImmutableList.of("urn:li:domain:test"));

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);

    // Verify service was called
    verify(mockService, times(1))
        .searchDocuments(any(OperationContext.class), eq("*"), any(), any(), eq(0), eq(10));
  }

  @Test
  public void testContextDocumentsDefaultValues() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Don't set start/count - should use defaults
    input.setStart(null);
    input.setCount(null);

    // Mock should return SearchResult with pageSize matching the default count (100)
    SearchResult defaultSearchResult = new SearchResult();
    defaultSearchResult.setFrom(0);
    defaultSearchResult.setPageSize(100); // Match DEFAULT_COUNT
    defaultSearchResult.setNumEntities(1);
    defaultSearchResult.setEntities(
        new SearchEntityArray(
            ImmutableList.of(new SearchEntity().setEntity(UrnUtils.getUrn(TEST_DOCUMENT_URN)))));
    defaultSearchResult.setMetadata(new SearchResultMetadata());

    when(mockService.searchDocuments(
            any(OperationContext.class),
            any(String.class),
            any(),
            any(),
            eq(0),
            eq(100))) // Default count
        .thenReturn(defaultSearchResult);

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getStart(), 0); // Default start
    assertEquals(result.getCount(), 100); // Default count

    // Verify service was called with defaults
    verify(mockService, times(1))
        .searchDocuments(any(OperationContext.class), eq("*"), any(), any(), eq(0), eq(100));
  }

  @Test
  public void testContextDocumentsIncludesPublishedDocuments() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(OTHER_USER_URN); // Different user
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Create published document owned by different user
    Map<com.linkedin.common.urn.Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse =
        createPublishedDocumentResponse(TEST_DOCUMENT_URN, OTHER_USER_URN);
    entityResponseMap.put(UrnUtils.getUrn(TEST_DOCUMENT_URN), entityResponse);
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getDocuments().size(), 1); // Published docs visible to all users
  }

  @Test
  public void testContextDocumentsIncludesOwnedUnpublishedDocuments() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Create unpublished document owned by current user
    Map<com.linkedin.common.urn.Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse =
        createUnpublishedDocumentResponse(TEST_DOCUMENT_URN, TEST_USER_URN);
    entityResponseMap.put(UrnUtils.getUrn(TEST_DOCUMENT_URN), entityResponse);
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getDocuments().size(), 1); // Owned unpublished docs visible to owner
  }

  @Test
  public void testContextDocumentsExcludesOtherUsersUnpublishedDocuments() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Create unpublished document owned by different user
    Map<com.linkedin.common.urn.Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse =
        createUnpublishedDocumentResponse(TEST_DOCUMENT_URN, OTHER_USER_URN);
    entityResponseMap.put(UrnUtils.getUrn(TEST_DOCUMENT_URN), entityResponse);
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    // Mock search to return empty results (filter excludes unpublished docs not owned by user)
    SearchResult emptySearchResult = new SearchResult();
    emptySearchResult.setFrom(0);
    emptySearchResult.setPageSize(10);
    emptySearchResult.setNumEntities(0);
    emptySearchResult.setEntities(new SearchEntityArray(Collections.emptyList()));
    emptySearchResult.setMetadata(new SearchResultMetadata());

    when(mockService.searchDocuments(
            any(OperationContext.class),
            any(String.class),
            any(),
            any(),
            any(Integer.class),
            any(Integer.class)))
        .thenReturn(emptySearchResult);

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(
        result.getDocuments().size(), 0); // Unpublished docs not owned by user are excluded
  }

  @Test
  public void testContextDocumentsWithGroupOwnership() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Mock user belongs to a group
    when(mockGroupService.getGroupsForUser(any(OperationContext.class), any(Urn.class)))
        .thenReturn(ImmutableList.of(UrnUtils.getUrn(TEST_GROUP_URN)));

    // Create unpublished document owned by user's group
    Map<com.linkedin.common.urn.Urn, EntityResponse> entityResponseMap = new HashMap<>();
    EntityResponse entityResponse =
        createUnpublishedDocumentResponse(TEST_DOCUMENT_URN, TEST_GROUP_URN);
    entityResponseMap.put(UrnUtils.getUrn(TEST_DOCUMENT_URN), entityResponse);
    when(mockEntityClient.batchGetV2(any(OperationContext.class), any(String.class), any(), any()))
        .thenReturn(entityResponseMap);

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(
        result.getDocuments().size(), 1); // Unpublished docs owned by user's group are visible
  }

  @Test(expectedExceptions = ExecutionException.class)
  public void testContextDocumentsMissingParentEntity() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getSource()).thenReturn(null); // No parent entity

    try {
      resolver.get(mockEnv).get(); // Should throw ExecutionException wrapping RuntimeException
    } catch (ExecutionException e) {
      // Verify the cause is RuntimeException (wrapped by catch block)
      assertTrue(e.getCause() instanceof RuntimeException);
      // The original exception is wrapped: RuntimeException("Failed to fetch related documents",
      // originalException)
      // So we need to check the nested cause
      assertTrue(e.getCause().getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getCause().getMessage().contains("Parent entity URN is required"));
      throw e; // Re-throw to satisfy expectedExceptions
    }
  }

  @Test(expectedExceptions = ExecutionException.class)
  public void testContextDocumentsMissingParentEntityUrn() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    Entity entityWithoutUrn = mock(Entity.class);
    when(entityWithoutUrn.getUrn()).thenReturn(null);
    when(mockEnv.getSource()).thenReturn(entityWithoutUrn);

    try {
      resolver.get(mockEnv).get(); // Should throw ExecutionException wrapping RuntimeException
    } catch (ExecutionException e) {
      // Verify the cause is RuntimeException (wrapped by catch block)
      assertTrue(e.getCause() instanceof RuntimeException);
      // The original exception is wrapped: RuntimeException("Failed to fetch related documents",
      // originalException)
      // So we need to check the nested cause
      assertTrue(e.getCause().getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getCause().getMessage().contains("Parent entity URN is required"));
      throw e; // Re-throw to satisfy expectedExceptions
    }
  }

  @Test
  public void testContextDocumentsWithRootOnlyFilter() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setRootOnly(true);

    RelatedDocumentsResult result = resolver.get(mockEnv).get();

    assertNotNull(result);
    // Verify service was called
    verify(mockService, times(1))
        .searchDocuments(any(OperationContext.class), eq("*"), any(), any(), eq(0), eq(10));
  }
}
