package com.linkedin.metadata.service;

import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.SemanticText;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.knowledge.ParentDocument;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DocumentServiceTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:test-document");
  private static final Urn TEST_PARENT_URN = UrnUtils.getUrn("urn:li:document:parent-document");
  private static final Urn TEST_ASSET_URN = UrnUtils.getUrn("urn:li:dataset:test-dataset");
  private static final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final OperationContext USER_OP_CONTEXT =
      TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

  @Test
  public void testCreateDocumentDeniedWithoutAuthorization() {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final DocumentService service = new DocumentService(mockClient);

    Assert.assertThrows(
        ServiceAuthorizationException.class,
        () ->
            service.createDocument(
                USER_OP_CONTEXT,
                "unauthorized-document",
                List.of("tutorial"),
                "Title",
                null,
                null,
                "Content",
                null,
                null,
                null,
                null,
                TEST_USER_URN,
                SearchIndexMode.SYNC));
    verifyNoInteractions(mockClient);
  }

  @Test
  public void testCreateDocumentWithCreateOnlyAuthorizationIncludesOwnership() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);
    final DocumentService service = new DocumentService(mockClient);
    final Owner owner = new Owner().setOwner(TEST_USER_URN).setType(OwnershipType.TECHNICAL_OWNER);

    try (MockedStatic<AuthUtil> authUtilMock = Mockito.mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAuthorizedEntityUrns(
                      USER_OP_CONTEXT, CREATE, List.of(TEST_DOCUMENT_URN)))
          .thenReturn(true);

      service.createDocument(
          USER_OP_CONTEXT,
          "test-document",
          List.of("tutorial"),
          "Title",
          null,
          null,
          "Content",
          null,
          null,
          null,
          null,
          List.of(owner),
          TEST_USER_URN,
          SearchIndexMode.SYNC);

      authUtilMock.verify(
          () ->
              AuthUtil.isAuthorizedEntityUrns(USER_OP_CONTEXT, CREATE, List.of(TEST_DOCUMENT_URN)));
      authUtilMock.verify(
          () ->
              AuthUtil.isAuthorizedEntityUrns(USER_OP_CONTEXT, UPDATE, List.of(TEST_DOCUMENT_URN)),
          Mockito.never());
    }

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockClient)
        .batchIngestProposals(eq(USER_OP_CONTEXT), proposalsCaptor.capture(), eq(false));

    final MetadataChangeProposal ownershipProposal =
        proposalsCaptor.getValue().stream()
            .filter(proposal -> Constants.OWNERSHIP_ASPECT_NAME.equals(proposal.getAspectName()))
            .findFirst()
            .orElseThrow();
    final Ownership ownership =
        GenericRecordUtils.deserializeAspect(
            ownershipProposal.getAspect().getValue(),
            ownershipProposal.getAspect().getContentType(),
            Ownership.class);
    Assert.assertEquals(ownership.getOwners(), List.of(owner));
  }

  @Test
  public void testGetDocumentDeniedWithoutAuthorization() {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final DocumentService service = new DocumentService(mockClient);

    Assert.assertThrows(
        ServiceAuthorizationException.class,
        () -> service.getDocumentInfo(USER_OP_CONTEXT, TEST_DOCUMENT_URN));
    verifyNoInteractions(mockClient);
  }

  @Test
  public void testUpdateDocumentDeniedWithoutAuthorization() {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final DocumentService service = new DocumentService(mockClient);

    Assert.assertThrows(
        ServiceAuthorizationException.class,
        () ->
            service.updateDocumentContents(
                USER_OP_CONTEXT,
                TEST_DOCUMENT_URN,
                "Updated content",
                null,
                null,
                TEST_USER_URN,
                SearchIndexMode.SYNC));
    verifyNoInteractions(mockClient);
  }

  @Test
  public void testDeleteDocumentDeniedWithoutAuthorization() {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final DocumentService service = new DocumentService(mockClient);

    Assert.assertThrows(
        ServiceAuthorizationException.class,
        () -> service.deleteDocument(USER_OP_CONTEXT, TEST_DOCUMENT_URN, SearchIndexMode.SYNC));
    verifyNoInteractions(mockClient);
  }

  @Test
  public void testCreateArticleSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test creating an document
    final Urn documentUrn =
        service.createDocument(
            opContext,
            null, // auto-generate ID
            java.util.Collections.singletonList("tutorial"), // subTypes
            "How to Use DataHub",
            null, // source
            null, // no initial state (will default to DRAFT)
            "This is the content",
            null, // no parent
            null, // no related assets
            null, // no related documents
            null, // showInGlobalContext defaults to true
            TEST_USER_URN,
            SearchIndexMode.SYNC);

    // Verify the URN was created
    Assert.assertNotNull(documentUrn);
    Assert.assertEquals(documentUrn.getEntityType(), Constants.DOCUMENT_ENTITY_NAME);

    // Verify ingest was called once (info aspect only, no relationships)
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(List.class), eq(false));
  }

  @Test
  public void testCreateArticleWithRelationships() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_PARENT_URN), eq(false)))
        .thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test creating an document with relationships
    final Urn documentUrn =
        service.createDocument(
            opContext,
            "custom-id",
            java.util.Collections.singletonList("tutorial"), // subTypes
            "Advanced Tutorial",
            null, // source
            com.linkedin.knowledge.DocumentState.PUBLISHED, // explicit state
            "Content with custom ID",
            TEST_PARENT_URN,
            Arrays.asList(TEST_ASSET_URN),
            Arrays.asList(TEST_DOCUMENT_URN),
            null, // showInGlobalContext defaults to true
            TEST_USER_URN,
            SearchIndexMode.SYNC);

    // Verify the URN was created with custom ID
    Assert.assertNotNull(documentUrn);
    Assert.assertTrue(documentUrn.toString().contains("custom-id"));

    // Verify ingest was called (should batch both info and relationships)
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(List.class), eq(false));
    verify(mockClient).exists(any(OperationContext.class), eq(TEST_PARENT_URN), eq(false));
  }

  @Test
  public void testCreateDocumentRejectsParentThatIsNotLive() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_PARENT_URN), eq(false)))
        .thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    try {
      service.createDocument(
          opContext,
          "child-id",
          java.util.Collections.singletonList("tutorial"),
          "Title",
          null,
          null,
          "Content",
          TEST_PARENT_URN,
          null,
          null,
          null,
          TEST_USER_URN,
          SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
    }
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testCreateDocumentRejectsSelfAsParent() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    try {
      service.createDocument(
          opContext,
          "parent-document",
          java.util.Collections.singletonList("tutorial"),
          "Title",
          null,
          null,
          "Content",
          TEST_PARENT_URN,
          null,
          null,
          null,
          TEST_USER_URN,
          SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("itself"));
    }
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testCreateArticleAlreadyExists() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test creating an document that already exists
    try {
      service.createDocument(
          opContext,
          "existing-id",
          java.util.Collections.singletonList("tutorial"), // subTypes
          "Title",
          null, // source
          null, // no initial state
          "Content",
          null,
          null,
          null,
          null, // showInGlobalContext
          TEST_USER_URN,
          SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("already exists"));
    }
  }

  @Test
  public void testGetArticleInfoSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    // Test getting an document info
    final DocumentInfo documentInfo = service.getDocumentInfo(opContext, TEST_DOCUMENT_URN);

    // Verify the document was returned
    Assert.assertNotNull(documentInfo);

    // Verify getV2 was called
    verify(mockClient, times(1))
        .getV2(
            any(OperationContext.class),
            eq(Constants.DOCUMENT_ENTITY_NAME),
            eq(TEST_DOCUMENT_URN),
            any(Set.class));
  }

  @Test
  public void testGetArticleInfoNotFound() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.getV2(
            any(OperationContext.class), any(String.class), any(Urn.class), any(Set.class)))
        .thenReturn(null);

    final DocumentService service = new DocumentService(mockClient);

    // Test getting a non-existent document
    final DocumentInfo documentInfo = service.getDocumentInfo(opContext, TEST_DOCUMENT_URN);

    // Verify null was returned
    Assert.assertNull(documentInfo);
  }

  @Test
  public void testUpdateArticleContentsSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    // Test updating document contents
    service.updateDocumentContents(
        opContext,
        TEST_DOCUMENT_URN,
        "New content",
        "Updated Title",
        null,
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    // Verify batch ingest was called
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testUpdateArticleContentsDoesNotWriteSemanticTextUnlessProvided() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    service.updateDocumentContents(
        opContext,
        TEST_DOCUMENT_URN,
        "New content",
        null,
        null,
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockClient, times(1))
        .batchIngestProposals(eq(opContext), proposalsCaptor.capture(), eq(false));

    final MetadataChangeProposal infoProposal =
        proposalsCaptor.getValue().stream()
            .filter(
                proposal -> Constants.DOCUMENT_INFO_ASPECT_NAME.equals(proposal.getAspectName()))
            .findFirst()
            .orElseThrow();
    final DocumentInfo updatedInfo =
        GenericRecordUtils.deserializeAspect(
            infoProposal.getAspect().getValue(),
            infoProposal.getAspect().getContentType(),
            DocumentInfo.class);
    Assert.assertEquals(updatedInfo.getContents().getText(), "New content");
    Assert.assertTrue(
        proposalsCaptor.getValue().stream()
            .noneMatch(
                proposal -> Constants.SEMANTIC_TEXT_ASPECT_NAME.equals(proposal.getAspectName())));
  }

  @Test
  public void testUpdateArticleContentsUpdatesSemanticTextWhenProvided() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    service.updateDocumentContents(
        opContext,
        TEST_DOCUMENT_URN,
        "User-owned content",
        "User-owned content",
        null,
        null,
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockClient, times(1))
        .batchIngestProposals(eq(opContext), proposalsCaptor.capture(), eq(false));

    final MetadataChangeProposal semanticTextProposal =
        proposalsCaptor.getValue().stream()
            .filter(
                proposal -> Constants.SEMANTIC_TEXT_ASPECT_NAME.equals(proposal.getAspectName()))
            .findFirst()
            .orElseThrow();
    final SemanticText updatedSemanticText =
        GenericRecordUtils.deserializeAspect(
            semanticTextProposal.getAspect().getValue(),
            semanticTextProposal.getAspect().getContentType(),
            SemanticText.class);
    Assert.assertEquals(updatedSemanticText.getText(), "User-owned content");
  }

  @Test
  public void testUpdateArticleContentsNotFound() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.getV2(
            any(OperationContext.class), any(String.class), any(Urn.class), any(Set.class)))
        .thenReturn(null);

    final DocumentService service = new DocumentService(mockClient);

    // Test updating a non-existent document
    try {
      service.updateDocumentContents(
          opContext, TEST_DOCUMENT_URN, "Content", null, null, TEST_USER_URN, SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testUpdateArticleContentsWithSubType() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    // Test updating document contents with subType
    service.updateDocumentContents(
        opContext,
        TEST_DOCUMENT_URN,
        "New content",
        "Updated Title",
        Arrays.asList("FAQ"),
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    // Verify batch ingest was called with 2 proposals (info + subTypes)
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testUpdateArticleRelatedEntitiesSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithRelationships();
    final DocumentService service = new DocumentService(mockClient);

    // Test updating related entities
    service.updateDocumentRelatedEntities(
        opContext,
        TEST_DOCUMENT_URN,
        Arrays.asList(TEST_ASSET_URN),
        null,
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    // Verify ingest was called
    verify(mockClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testMoveArticleSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithRelationships();
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class), eq(false)))
        .thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test moving document to new parent
    service.moveDocument(
        opContext, TEST_DOCUMENT_URN, TEST_PARENT_URN, TEST_USER_URN, SearchIndexMode.SYNC);

    // Verify ingest was called
    verify(mockClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testMoveArticleToRoot() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithRelationships();
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test moving document to root (no parent)
    service.moveDocument(opContext, TEST_DOCUMENT_URN, null, TEST_USER_URN, SearchIndexMode.SYNC);

    // Verify ingest was called
    verify(mockClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testMoveArticleToItself() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class), eq(false)))
        .thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test moving document to itself (should fail)
    try {
      service.moveDocument(
          opContext, TEST_DOCUMENT_URN, TEST_DOCUMENT_URN, TEST_USER_URN, SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot move"));
    }
  }

  @Test
  public void testMoveDocumentRejectsParentThatIsNotLive() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_PARENT_URN), eq(false)))
        .thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    try {
      service.moveDocument(
          opContext, TEST_DOCUMENT_URN, TEST_PARENT_URN, TEST_USER_URN, SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
    }
    verify(mockClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testDeleteArticleSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    stubChildScroll(mockClient, Map.of());

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    final List<MetadataChangeProposal> proposals = captureDeleteProposals(mockClient, opContext);
    Assert.assertEquals(proposals.size(), 1);
    Assert.assertEquals(proposals.get(0).getEntityUrn(), TEST_DOCUMENT_URN);
    assertRemoved(proposals.get(0));
    Assert.assertEquals(deleted.urns(), List.of(TEST_DOCUMENT_URN));
    Assert.assertEquals(deleted.descendantCount(), 0);
    verify(mockClient, never())
        .search(
            any(OperationContext.class),
            anyString(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt());
  }

  @Test
  public void testDeleteArticleNotFound() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test deleting a non-existent document
    try {
      service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
      verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
    }
  }

  @Test
  public void testSearchArticlesSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithSearchResults();
    final DocumentService service = new DocumentService(mockClient);

    // Test searching documents
    final SearchResult result = service.searchDocuments(opContext, "tutorial", null, null, 0, 10);

    // Verify search was called
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getNumEntities(), 5);

    // Verify search method was called
    verify(mockClient, times(1))
        .search(
            any(OperationContext.class),
            eq(Constants.DOCUMENT_ENTITY_NAME),
            eq("tutorial"),
            any(),
            any(List.class),
            eq(0),
            eq(10));
  }

  // Helper methods to create mock EntityClients

  private SystemEntityClient createMockEntityClientWithInfo() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final DocumentInfo info = new DocumentInfo();
    info.setTitle("Test Article");
    info.setContents(new DocumentContents().setText("Test content"));

    final EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(info.data()));

    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(Constants.DOCUMENT_INFO_ASPECT_NAME, aspect);

    final EntityResponse response = new EntityResponse();
    response.setUrn(TEST_DOCUMENT_URN);
    response.setAspects(aspectMap);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DOCUMENT_ENTITY_NAME),
            any(Urn.class),
            any(Set.class)))
        .thenReturn(response);

    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    return mockClient;
  }

  private SystemEntityClient createMockEntityClientWithRelationships() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    // Create a basic DocumentInfo with some sample data
    final DocumentInfo info = new DocumentInfo();
    info.setTitle("Test Article");
    final com.linkedin.knowledge.DocumentContents contents =
        new com.linkedin.knowledge.DocumentContents();
    contents.setText("Test content");
    info.setContents(contents);
    info.setCreated(
        new com.linkedin.common.AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(UrnUtils.getUrn("urn:li:corpuser:test")));

    final EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(
        new com.linkedin.entity.Aspect(GenericRecordUtils.serializeAspect(info).data()));

    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(Constants.DOCUMENT_INFO_ASPECT_NAME, aspect);

    final EntityResponse response = new EntityResponse();
    response.setUrn(TEST_DOCUMENT_URN);
    response.setAspects(aspectMap);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DOCUMENT_ENTITY_NAME),
            any(Urn.class),
            any(Set.class)))
        .thenReturn(response);

    return mockClient;
  }

  private SystemEntityClient createMockEntityClientWithSearchResults() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final SearchResult searchResult = new SearchResult();
    searchResult.setFrom(0);
    searchResult.setPageSize(10);
    searchResult.setNumEntities(5);

    final SearchEntityArray entities = new SearchEntityArray();
    for (int i = 0; i < 5; i++) {
      final SearchEntity entity = new SearchEntity();
      entity.setEntity(UrnUtils.getUrn("urn:li:document:document-" + i));
      entities.add(entity);
    }
    searchResult.setEntities(entities);
    searchResult.setMetadata(new SearchResultMetadata());

    when(mockClient.search(
            any(OperationContext.class),
            eq(Constants.DOCUMENT_ENTITY_NAME),
            any(String.class),
            any(),
            any(List.class),
            any(Integer.class),
            any(Integer.class)))
        .thenReturn(searchResult);

    return mockClient;
  }

  @Test
  public void testUpdateArticleStatusSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    // Test updating document status
    service.updateDocumentStatus(
        opContext,
        TEST_DOCUMENT_URN,
        com.linkedin.knowledge.DocumentState.PUBLISHED,
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    // Verify ingest was called to update the info
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testUpdateArticleStatusNotFound() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test updating status for a non-existent document
    try {
      service.updateDocumentStatus(
          opContext,
          TEST_DOCUMENT_URN,
          com.linkedin.knowledge.DocumentState.PUBLISHED,
          TEST_USER_URN,
          SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testSetArticleOwnershipSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final DocumentService service = new DocumentService(mockClient);

    // Create a list of owners
    final Owner owner1 = new Owner();
    owner1.setOwner(TEST_USER_URN);
    owner1.setType(OwnershipType.TECHNICAL_OWNER);

    final Urn owner2Urn = UrnUtils.getUrn("urn:li:corpuser:owner2");
    final Owner owner2 = new Owner();
    owner2.setOwner(owner2Urn);
    owner2.setType(OwnershipType.BUSINESS_OWNER);

    final List<Owner> owners = Arrays.asList(owner1, owner2);

    // Test setting ownership
    service.setDocumentOwnership(
        opContext, TEST_DOCUMENT_URN, owners, TEST_USER_URN, SearchIndexMode.SYNC);

    // Verify that ingestProposal was called once with ownership aspect
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testSetArticleOwnershipEmptyList() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final DocumentService service = new DocumentService(mockClient);

    // Test setting ownership with empty list (should still work)
    service.setDocumentOwnership(
        opContext,
        TEST_DOCUMENT_URN,
        java.util.Collections.emptyList(),
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    // Verify that ingestProposal was called once
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testUpdateDocumentSubTypeSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    // Test updating document subType
    service.updateDocumentSubType(
        opContext, TEST_DOCUMENT_URN, "faq", TEST_USER_URN, SearchIndexMode.SYNC);

    // Verify batch ingest was called (subTypes + info with updated lastModified)
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(List.class), eq(false));
  }

  @Test
  public void testUpdateDocumentSubTypeNotFound() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test updating subType for a non-existent document
    try {
      service.updateDocumentSubType(
          opContext, TEST_DOCUMENT_URN, "faq", TEST_USER_URN, SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testCircularReferenceDetectionSimple() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    // Create a simple circular reference: doc1 -> doc2 -> doc1
    final Urn doc1Urn = UrnUtils.getUrn("urn:li:document:doc1");
    final Urn doc2Urn = UrnUtils.getUrn("urn:li:document:doc2");

    // Mock doc2 with parent doc1 (creates the cycle when we try to make doc1's parent = doc2)
    final DocumentInfo doc2Info = new DocumentInfo();

    // Set ALL required fields first (contents, created, lastModified, status)
    final com.linkedin.knowledge.DocumentContents doc2Contents =
        new com.linkedin.knowledge.DocumentContents();
    doc2Contents.setText("doc2");
    doc2Info.setContents(doc2Contents);

    final com.linkedin.common.AuditStamp doc2Created = new com.linkedin.common.AuditStamp();
    doc2Created.setTime(System.currentTimeMillis());
    doc2Created.setActor(TEST_USER_URN);
    doc2Info.setCreated(doc2Created);

    final com.linkedin.common.AuditStamp doc2Modified = new com.linkedin.common.AuditStamp();
    doc2Modified.setTime(System.currentTimeMillis());
    doc2Modified.setActor(TEST_USER_URN);
    doc2Info.setLastModified(doc2Modified);

    final com.linkedin.knowledge.DocumentStatus doc2Status =
        new com.linkedin.knowledge.DocumentStatus();
    doc2Status.setState(com.linkedin.knowledge.DocumentState.PUBLISHED);
    doc2Info.setStatus(doc2Status);

    // Now set the parent document (optional field) - use regular setter
    final com.linkedin.knowledge.ParentDocument doc2Parent =
        new com.linkedin.knowledge.ParentDocument();
    doc2Parent.setDocument(doc1Urn);
    doc2Info.setParentDocument(doc2Parent);

    final EnvelopedAspect doc2Aspect = new EnvelopedAspect();
    doc2Aspect.setValue(new com.linkedin.entity.Aspect(doc2Info.data()));
    final EnvelopedAspectMap doc2AspectMap = new EnvelopedAspectMap();
    doc2AspectMap.put(Constants.DOCUMENT_INFO_ASPECT_NAME, doc2Aspect);
    final EntityResponse doc2Response = new EntityResponse();
    doc2Response.setUrn(doc2Urn);
    doc2Response.setAspects(doc2AspectMap);

    // Mock doc1 info (will be updated to have parent doc2)
    final DocumentInfo doc1Info = new DocumentInfo();

    final com.linkedin.knowledge.DocumentContents doc1Contents =
        new com.linkedin.knowledge.DocumentContents();
    doc1Contents.setText("doc1");
    doc1Info.setContents(doc1Contents);

    final com.linkedin.common.AuditStamp doc1Created = new com.linkedin.common.AuditStamp();
    doc1Created.setTime(System.currentTimeMillis());
    doc1Created.setActor(TEST_USER_URN);
    doc1Info.setCreated(doc1Created);

    final com.linkedin.common.AuditStamp doc1Modified = new com.linkedin.common.AuditStamp();
    doc1Modified.setTime(System.currentTimeMillis());
    doc1Modified.setActor(TEST_USER_URN);
    doc1Info.setLastModified(doc1Modified);

    final com.linkedin.knowledge.DocumentStatus doc1Status =
        new com.linkedin.knowledge.DocumentStatus();
    doc1Status.setState(com.linkedin.knowledge.DocumentState.PUBLISHED);
    doc1Info.setStatus(doc1Status);

    final EnvelopedAspect doc1Aspect = new EnvelopedAspect();
    doc1Aspect.setValue(new com.linkedin.entity.Aspect(doc1Info.data()));
    final EnvelopedAspectMap doc1AspectMap = new EnvelopedAspectMap();
    doc1AspectMap.put(Constants.DOCUMENT_INFO_ASPECT_NAME, doc1Aspect);
    final EntityResponse doc1Response = new EntityResponse();
    doc1Response.setUrn(doc1Urn);
    doc1Response.setAspects(doc1AspectMap);

    // Setup mocks
    when(mockClient.exists(any(OperationContext.class), eq(doc1Urn))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(doc2Urn))).thenReturn(true);
    when(mockClient.exists(any(OperationContext.class), eq(doc2Urn), eq(false))).thenReturn(true);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DOCUMENT_ENTITY_NAME),
            eq(doc1Urn),
            any(Set.class)))
        .thenReturn(doc1Response);

    when(mockClient.getV2(
            any(OperationContext.class),
            eq(Constants.DOCUMENT_ENTITY_NAME),
            eq(doc2Urn),
            any(Set.class)))
        .thenReturn(doc2Response);

    final DocumentService service = new DocumentService(mockClient);

    // Test moving doc1 to have parent doc2 (which would create a circular reference doc1 -> doc2 ->
    // doc1)
    try {
      service.moveDocument(opContext, doc1Urn, doc2Urn, TEST_USER_URN, SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException for circular reference");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("circular"));
    }
  }

  @Test
  public void testBuildParentDocumentFilter() {
    final Urn parentUrn = UrnUtils.getUrn("urn:li:document:parent");

    // Test the static filter builder
    final com.linkedin.metadata.query.filter.Filter filter =
        DocumentService.buildParentDocumentFilter(parentUrn);

    Assert.assertNotNull(filter);
    Assert.assertNotNull(filter.getOr());
    Assert.assertEquals(filter.getOr().size(), 1);
    Assert.assertEquals(filter.getOr().get(0).getAnd().size(), 1);
    Assert.assertEquals(filter.getOr().get(0).getAnd().get(0).getField(), "parentDocument");
    Assert.assertEquals(
        filter.getOr().get(0).getAnd().get(0).getValues().get(0), parentUrn.toString());
  }

  @Test
  public void testBuildParentDocumentFilterNull() {
    // Test with null parent
    final com.linkedin.metadata.query.filter.Filter filter =
        DocumentService.buildParentDocumentFilter(null);

    Assert.assertNull(filter);
  }

  @Test
  public void testCreateDocumentWithCustomSettings() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test creating a document with showInGlobalContext=false
    final Urn documentUrn =
        service.createDocument(
            opContext,
            null, // auto-generate ID
            java.util.Collections.singletonList("tutorial"),
            "Private Context Document",
            null, // source
            null, // state
            "This is a private context document",
            null, // no parent
            null, // no related assets
            null, // no related documents
            new com.linkedin.knowledge.DocumentSettings()
                .setShowInGlobalContext(false), // showInGlobalContext = false
            TEST_USER_URN,
            SearchIndexMode.SYNC);

    // Verify the URN was created
    Assert.assertNotNull(documentUrn);
    Assert.assertEquals(documentUrn.getEntityType(), Constants.DOCUMENT_ENTITY_NAME);

    // Verify ingest was called (documentInfo + subTypes + documentSettings)
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(List.class), eq(false));
  }

  @Test
  public void testCreateDocumentWithDefaultSettings() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test creating a document with null settings (should default to showInGlobalContext=true)
    final Urn documentUrn =
        service.createDocument(
            opContext,
            null, // auto-generate ID
            java.util.Collections.singletonList("tutorial"),
            "Public Document",
            null, // source
            null, // state
            "This is a public document",
            null, // no parent
            null, // no related assets
            null, // no related documents
            null, // showInGlobalContext defaults to true
            TEST_USER_URN,
            SearchIndexMode.SYNC);

    // Verify the URN was created
    Assert.assertNotNull(documentUrn);
    Assert.assertEquals(documentUrn.getEntityType(), Constants.DOCUMENT_ENTITY_NAME);

    // Verify ingest was called
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(List.class), eq(false));
  }

  @Test
  public void testUpdateDocumentSettings() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    // Test updating document settings
    service.updateDocumentSettings(
        opContext,
        TEST_DOCUMENT_URN,
        new com.linkedin.knowledge.DocumentSettings().setShowInGlobalContext(false),
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    // Verify batch ingest was called (settings + documentInfo for lastModified)
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(List.class), eq(false));
  }

  @Test
  public void testUpdateDocumentSettingsNotFound() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test updating settings for a non-existent document
    try {
      service.updateDocumentSettings(
          opContext,
          TEST_DOCUMENT_URN,
          new com.linkedin.knowledge.DocumentSettings().setShowInGlobalContext(true),
          TEST_USER_URN,
          SearchIndexMode.SYNC);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
    }
  }

  @Test
  public void testCreateDocumentAllowsWithCreatePrivilege() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      authUtils
          .when(() -> DocumentAuthorizationUtils.assertCanCreate(eq(userContext), any(Urn.class)))
          .thenAnswer(invocation -> null);

      final Urn documentUrn =
          service.createDocument(
              userContext,
              "auth-create-ok",
              java.util.Collections.singletonList("tutorial"),
              "Authorized Create",
              null,
              null,
              "content",
              null,
              null,
              null,
              null,
              TEST_USER_URN,
              SearchIndexMode.SYNC);

      Assert.assertEquals(documentUrn, UrnUtils.getUrn("urn:li:document:auth-create-ok"));
      authUtils.verify(
          () -> DocumentAuthorizationUtils.assertCanCreate(eq(userContext), eq(documentUrn)));
      verify(mockClient, times(1))
          .batchIngestProposals(eq(userContext), any(List.class), eq(false));
    }
  }

  @Test
  public void testCreateDocumentIncludesOwnershipWithoutUpdateAuthorization() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);
    final Owner owner = new Owner().setOwner(TEST_USER_URN).setType(OwnershipType.TECHNICAL_OWNER);

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      service.createDocument(
          userContext,
          "test-document",
          List.of("tutorial"),
          "Title",
          null,
          null,
          "Content",
          null,
          null,
          null,
          null,
          List.of(owner),
          TEST_USER_URN,
          SearchIndexMode.SYNC);

      authUtils.verify(
          () -> DocumentAuthorizationUtils.assertCanCreate(eq(userContext), eq(TEST_DOCUMENT_URN)));
      authUtils.verify(() -> DocumentAuthorizationUtils.assertCanUpdate(any(), any()), never());
    }

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockClient).batchIngestProposals(eq(userContext), proposalsCaptor.capture(), eq(false));

    Assert.assertEquals(
        proposalsCaptor.getValue().stream().map(MetadataChangeProposal::getAspectName).toList(),
        List.of(
            Constants.DOCUMENT_INFO_ASPECT_NAME,
            Constants.SUB_TYPES_ASPECT_NAME,
            Constants.DOCUMENT_SETTINGS_ASPECT_NAME,
            Constants.OWNERSHIP_ASPECT_NAME));
    final MetadataChangeProposal ownershipProposal =
        proposalsCaptor.getValue().stream()
            .filter(proposal -> Constants.OWNERSHIP_ASPECT_NAME.equals(proposal.getAspectName()))
            .findFirst()
            .orElseThrow();
    final Ownership ownership =
        GenericRecordUtils.deserializeAspect(
            ownershipProposal.getAspect().getValue(),
            ownershipProposal.getAspect().getContentType(),
            Ownership.class);
    Assert.assertEquals(ownership.getOwners(), List.of(owner));
  }

  @Test
  public void testCreateDocumentDeniesWithoutCreatePrivilege() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      authUtils
          .when(() -> DocumentAuthorizationUtils.assertCanCreate(eq(userContext), any(Urn.class)))
          .thenThrow(new ServiceAuthorizationException("Unauthorized to create document"));

      Assert.expectThrows(
          ServiceAuthorizationException.class,
          () ->
              service.createDocument(
                  userContext,
                  "auth-create-denied",
                  java.util.Collections.singletonList("tutorial"),
                  "Denied Create",
                  null,
                  null,
                  "content",
                  null,
                  null,
                  null,
                  null,
                  TEST_USER_URN,
                  SearchIndexMode.SYNC));

      verify(mockClient, times(0))
          .batchIngestProposals(any(OperationContext.class), any(), eq(false));
    }
  }

  @Test
  public void testUpdateDocumentContentsAllowsWithUpdatePrivilege() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      service.updateDocumentContents(
          userContext,
          TEST_DOCUMENT_URN,
          "New content",
          null,
          null,
          TEST_USER_URN,
          SearchIndexMode.SYNC);

      authUtils.verify(
          () -> DocumentAuthorizationUtils.assertCanUpdate(eq(userContext), eq(TEST_DOCUMENT_URN)));
      verify(mockClient, times(1))
          .batchIngestProposals(eq(userContext), any(List.class), eq(false));
    }
  }

  @Test
  public void testUpdateDocumentContentsDeniesWithoutUpdatePrivilege() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      authUtils
          .when(
              () ->
                  DocumentAuthorizationUtils.assertCanUpdate(
                      eq(userContext), eq(TEST_DOCUMENT_URN)))
          .thenThrow(new ServiceAuthorizationException("Unauthorized to update document"));

      Assert.expectThrows(
          ServiceAuthorizationException.class,
          () ->
              service.updateDocumentContents(
                  userContext,
                  TEST_DOCUMENT_URN,
                  "New content",
                  null,
                  null,
                  TEST_USER_URN,
                  SearchIndexMode.SYNC));

      verify(mockClient, times(0))
          .batchIngestProposals(any(OperationContext.class), any(), eq(false));
    }
  }

  @Test
  public void testDeleteDocumentAllowsWithDeletePrivilege() throws Exception {
    final Urn child = UrnUtils.getUrn("urn:li:document:nested-child");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

    stubChildScroll(mockClient, Map.of(TEST_DOCUMENT_URN, List.of(List.of(child))));

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      service.deleteDocument(userContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

      authUtils.verify(
          () -> DocumentAuthorizationUtils.assertCanDelete(eq(userContext), eq(TEST_DOCUMENT_URN)));
      authUtils.verify(
          () -> DocumentAuthorizationUtils.assertCanDelete(eq(userContext), eq(child)), never());
      Assert.assertEquals(
          proposalUrns(captureDeleteProposals(mockClient, userContext)),
          List.of(child, TEST_DOCUMENT_URN));
    }
  }

  @Test
  public void testDeleteDocumentDeniesWithoutDeletePrivilege() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      authUtils
          .when(
              () ->
                  DocumentAuthorizationUtils.assertCanDelete(
                      eq(userContext), eq(TEST_DOCUMENT_URN)))
          .thenThrow(new ServiceAuthorizationException("Unauthorized to delete document"));

      Assert.expectThrows(
          ServiceAuthorizationException.class,
          () -> service.deleteDocument(userContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC));

      verify(mockClient, never()).exists(any(OperationContext.class), any(Urn.class));
      verify(mockClient, never())
          .ingestProposal(
              any(OperationContext.class), any(MetadataChangeProposal.class), eq(false));
      verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
    }
  }

  // SYNC and ASYNC must apply uniformly to every proposal a mutation emits. Splitting one
  // document's writes across the two index writers (GMS pre-process vs MAE consumer) lets a
  // stale create-time search document overwrite later updates (e.g. relatedAssets, status).
  @Test
  public void testSyncIndexModeStampsAllProposals() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);
    final DocumentService service = new DocumentService(mockClient);

    service.createDocument(
        opContext,
        "sync-mode-document",
        List.of("tutorial"),
        "Title",
        null,
        null,
        "Content",
        null,
        null,
        null,
        null,
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<MetadataChangeProposal>> proposalsCaptor =
        ArgumentCaptor.forClass(List.class);
    verify(mockClient).batchIngestProposals(eq(opContext), proposalsCaptor.capture(), eq(false));
    for (MetadataChangeProposal proposal : proposalsCaptor.getValue()) {
      Assert.assertNotNull(
          proposal.getSystemMetadata(), proposal.getAspectName() + " missing system metadata");
      Assert.assertEquals(
          proposal.getSystemMetadata().getProperties().get(Constants.APP_SOURCE),
          Constants.UI_SOURCE,
          proposal.getAspectName() + " not marked for synchronous index update");
    }
  }

  @Test
  public void testSyncIndexModeStampsUpdateProposals() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    final DocumentService service = new DocumentService(mockClient);

    service.updateDocumentStatus(
        opContext,
        TEST_DOCUMENT_URN,
        com.linkedin.knowledge.DocumentState.PUBLISHED,
        TEST_USER_URN,
        SearchIndexMode.SYNC);
    service.updateDocumentRelatedEntities(
        opContext,
        TEST_DOCUMENT_URN,
        List.of(TEST_ASSET_URN),
        null,
        TEST_USER_URN,
        SearchIndexMode.SYNC);

    final ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient, times(2)).ingestProposal(eq(opContext), proposalCaptor.capture(), eq(false));
    for (MetadataChangeProposal proposal : proposalCaptor.getAllValues()) {
      Assert.assertNotNull(proposal.getSystemMetadata());
      Assert.assertEquals(
          proposal.getSystemMetadata().getProperties().get(Constants.APP_SOURCE),
          Constants.UI_SOURCE);
    }
  }

  @Test
  public void testAsyncIndexModeLeavesProposalsUnstamped() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithInfo();
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    final DocumentService service = new DocumentService(mockClient);

    service.updateDocumentStatus(
        opContext,
        TEST_DOCUMENT_URN,
        com.linkedin.knowledge.DocumentState.PUBLISHED,
        TEST_USER_URN,
        SearchIndexMode.ASYNC);

    final ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockClient).ingestProposal(eq(opContext), proposalCaptor.capture(), eq(false));
    final MetadataChangeProposal proposal = proposalCaptor.getValue();
    Assert.assertTrue(
        proposal.getSystemMetadata() == null
            || proposal.getSystemMetadata().getProperties() == null
            || !Constants.UI_SOURCE.equals(
                proposal.getSystemMetadata().getProperties().get(Constants.APP_SOURCE)));
  }

  @Test
  public void testDeleteDocumentRemovesNestedDocumentsRootLast() throws Exception {
    final Urn child = UrnUtils.getUrn("urn:li:document:child");
    final Urn grandchild = UrnUtils.getUrn("urn:li:document:grandchild");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    stubChildScroll(
        mockClient,
        Map.of(
            TEST_DOCUMENT_URN, List.of(List.of(child)),
            child, List.of(List.of(grandchild))),
        Map.of(
            child,
            new Status()
                .setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:published"))
                .setLifecycleLastUpdated(new AuditStamp().setTime(5L).setActor(TEST_USER_URN))));
    final DocumentService service = new DocumentService(mockClient);

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    final List<MetadataChangeProposal> proposals = captureDeleteProposals(mockClient, opContext);
    final List<Urn> written = proposalUrns(proposals);
    Assert.assertEquals(written, List.of(grandchild, child, TEST_DOCUMENT_URN));
    Assert.assertEquals(deleted.urns(), written);
    Assert.assertEquals(deleted.descendantCount(), 2);
    final Status childStatus = statusOf(proposals.get(1));
    Assert.assertTrue(childStatus.isRemoved());
    Assert.assertEquals(
        childStatus.getLifecycleStage(), UrnUtils.getUrn("urn:li:lifecycleStageType:published"));
    Assert.assertEquals(childStatus.getLifecycleLastUpdated().getTime(), Long.valueOf(5L));
  }

  @Test
  public void testDeleteDocumentFollowsScrollPages() throws Exception {
    final Urn first = UrnUtils.getUrn("urn:li:document:page-1");
    final Urn second = UrnUtils.getUrn("urn:li:document:page-2");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    stubChildScroll(
        mockClient, Map.of(TEST_DOCUMENT_URN, List.of(List.of(first), List.of(second))));
    final DocumentService service = new DocumentService(mockClient);

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    Assert.assertEquals(
        proposalUrns(captureDeleteProposals(mockClient, opContext)),
        List.of(first, second, TEST_DOCUMENT_URN));
    Assert.assertEquals(deleted.descendantCount(), 2);
  }

  @Test
  public void testDeleteDocumentScrollsSiblingsInOneCall() throws Exception {
    final Urn childA = UrnUtils.getUrn("urn:li:document:child-a");
    final Urn childB = UrnUtils.getUrn("urn:li:document:child-b");
    final Urn grandA = UrnUtils.getUrn("urn:li:document:grand-a");
    final Urn grandB = UrnUtils.getUrn("urn:li:document:grand-b");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    stubChildScroll(
        mockClient,
        Map.of(
            TEST_DOCUMENT_URN, List.of(List.of(childA, childB)),
            childA, List.of(List.of(grandA)),
            childB, List.of(List.of(grandB))));
    final DocumentService service = new DocumentService(mockClient);

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    Assert.assertEquals(
        proposalUrns(captureDeleteProposals(mockClient, opContext)),
        List.of(grandA, grandB, childA, childB, TEST_DOCUMENT_URN));
    Assert.assertEquals(deleted.descendantCount(), 4);
    final ArgumentCaptor<Filter> filters = ArgumentCaptor.forClass(Filter.class);
    verify(mockClient, times(3))
        .scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            filters.capture(),
            nullable(String.class),
            nullable(String.class),
            nullable(List.class),
            any(),
            anyList());
    Assert.assertEquals(
        Set.copyOf(filters.getAllValues().get(1).getOr().get(0).getAnd().get(0).getValues()),
        Set.of(childA.toString(), childB.toString()));
  }

  @Test
  public void testDeleteDocumentDeletesChildStoredUnderAnotherParentInTheSameChunk()
      throws Exception {
    final Urn childA = UrnUtils.getUrn("urn:li:document:child-a");
    final Urn childB = UrnUtils.getUrn("urn:li:document:child-b");
    final Urn moved = UrnUtils.getUrn("urn:li:document:moved");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    stubChildScroll(
        mockClient,
        Map.of(
            TEST_DOCUMENT_URN, List.of(List.of(childA, childB)),
            childA, List.of(List.of(moved))),
        Map.of(),
        Map.of(moved, childB));
    final DocumentService service = new DocumentService(mockClient);

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    Assert.assertEquals(
        proposalUrns(captureDeleteProposals(mockClient, opContext)),
        List.of(moved, childA, childB, TEST_DOCUMENT_URN));
    Assert.assertEquals(deleted.descendantCount(), 3);
  }

  @Test
  public void testDeleteDocumentWritesEachReachableDocumentOnce() throws Exception {
    final Urn childA = UrnUtils.getUrn("urn:li:document:child-a");
    final Urn childB = UrnUtils.getUrn("urn:li:document:child-b");
    final Urn shared = UrnUtils.getUrn("urn:li:document:shared");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    // childA points back at the root (cycle) and both children point at shared (diamond).
    stubChildScroll(
        mockClient,
        Map.of(
            TEST_DOCUMENT_URN, List.of(List.of(childA, childB)),
            childA, List.of(List.of(shared, TEST_DOCUMENT_URN)),
            childB, List.of(List.of(shared))));
    final DocumentService service = new DocumentService(mockClient);

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    final List<Urn> written = proposalUrns(captureDeleteProposals(mockClient, opContext));
    Assert.assertEquals(written.size(), 4);
    Assert.assertEquals(Set.copyOf(written).size(), 4);
    Assert.assertEquals(written.get(written.size() - 1), TEST_DOCUMENT_URN);
    Assert.assertEquals(written.get(0), shared);
    Assert.assertEquals(deleted.descendantCount(), 3);
  }

  @Test
  public void testDeleteDocumentRejectsDescendantPastCapWithoutWriting() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    final List<Urn> children = new ArrayList<>();
    for (int i = 0; i < DocumentDeleteResult.MAX_DESCENDANTS + 1; i++) {
      children.add(UrnUtils.getUrn("urn:li:document:child-" + i));
    }
    stubChildScroll(mockClient, Map.of(TEST_DOCUMENT_URN, List.of(children)));
    final DocumentService service = new DocumentService(mockClient);

    Assert.expectThrows(
        DocumentDeleteLimitException.class,
        () -> service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC));
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testDeleteDocumentStopsPagingOnceDescendantCapIsExceeded() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    final List<List<Urn>> pages = new ArrayList<>();
    int childNumber = 0;
    // One page past the cap, so a scroll that keeps going would request it.
    for (int page = 0; page < 12; page++) {
      final List<Urn> children = new ArrayList<>();
      for (int i = 0; i < DocumentDeleteResult.SCROLL_PAGE_SIZE; i++) {
        childNumber++;
        children.add(UrnUtils.getUrn("urn:li:document:child-" + childNumber));
      }
      pages.add(children);
    }
    stubChildScroll(mockClient, Map.of(TEST_DOCUMENT_URN, pages));
    final DocumentService service = new DocumentService(mockClient);

    Assert.expectThrows(
        DocumentDeleteLimitException.class,
        () -> service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC));

    // Ten full pages are the cap. The next page contains descendant 10,001 and is the last fetch.
    verify(mockClient, times(11))
        .scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            nullable(String.class),
            nullable(String.class),
            nullable(List.class),
            any(),
            anyList());
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testDeleteDocumentSkipsChildWhoseStoredParentDiffers() throws Exception {
    final Urn child = UrnUtils.getUrn("urn:li:document:moved-child");
    final Urn storedParent = UrnUtils.getUrn("urn:li:document:other-parent");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    stubChildScroll(
        mockClient,
        Map.of(
            TEST_DOCUMENT_URN, List.of(List.of(child)),
            child, List.of(List.of(UrnUtils.getUrn("urn:li:document:grandchild")))));
    when(mockClient.batchGetV2(
            any(OperationContext.class),
            anyString(),
            anySet(),
            nullable(Set.class),
            nullable(Boolean.class)))
        .thenReturn(Map.of(child, documentWithParent(child, storedParent)));
    final DocumentService service = new DocumentService(mockClient);

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    Assert.assertEquals(
        proposalUrns(captureDeleteProposals(mockClient, opContext)), List.of(TEST_DOCUMENT_URN));
    Assert.assertEquals(deleted.descendantCount(), 0);
    verify(mockClient, times(1))
        .scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            nullable(String.class),
            nullable(String.class),
            nullable(List.class),
            any(),
            anyList());
  }

  @Test
  public void testDeleteDocumentRejectsDepthPastCapWithoutWriting() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    final Map<Urn, List<List<Urn>>> pages = new HashMap<>();
    Urn parent = TEST_DOCUMENT_URN;
    for (int depth = 1; depth <= DocumentDeleteResult.MAX_DEPTH + 1; depth++) {
      final Urn child = UrnUtils.getUrn("urn:li:document:depth-" + depth);
      pages.put(parent, List.of(List.of(child)));
      parent = child;
    }
    stubChildScroll(mockClient, pages);
    final DocumentService service = new DocumentService(mockClient);

    Assert.expectThrows(
        DocumentDeleteLimitException.class,
        () -> service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC));
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testDeleteAlreadyRemovedRootRemovesLiveGrandchild() throws Exception {
    final Urn child = UrnUtils.getUrn("urn:li:document:child");
    final Urn grandchild = UrnUtils.getUrn("urn:li:document:grandchild");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    stubChildScroll(
        mockClient,
        Map.of(
            TEST_DOCUMENT_URN, List.of(List.of(child)),
            child, List.of(List.of(grandchild))));
    final DocumentService service = new DocumentService(mockClient);

    final DocumentDeleteResult deleted =
        service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);

    Assert.assertEquals(
        proposalUrns(captureDeleteProposals(mockClient, opContext)),
        List.of(grandchild, child, TEST_DOCUMENT_URN));
    Assert.assertEquals(deleted.descendantCount(), 2);
  }

  @Test
  public void testDeleteDocumentScrollIncludesDraftAndNonGlobalChildren() throws Exception {
    final Urn child = UrnUtils.getUrn("urn:li:document:draft-child");
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    stubChildScroll(mockClient, Map.of(TEST_DOCUMENT_URN, List.of(List.of(child))));
    final DocumentService service = new DocumentService(mockClient);
    final OperationContext userContext =
        TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

    try (MockedStatic<DocumentAuthorizationUtils> authUtils =
        mockStatic(DocumentAuthorizationUtils.class)) {
      service.deleteDocument(userContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC);
    }

    final ArgumentCaptor<OperationContext> scrollContext =
        ArgumentCaptor.forClass(OperationContext.class);
    verify(mockClient, atLeastOnce())
        .scrollAcrossEntities(
            scrollContext.capture(),
            eq(List.of(Constants.DOCUMENT_ENTITY_NAME)),
            eq("*"),
            any(Filter.class),
            nullable(String.class),
            eq(DocumentDeleteResult.SCROLL_KEEP_ALIVE),
            nullable(List.class),
            eq(DocumentDeleteResult.SCROLL_PAGE_SIZE),
            eq(List.of()));
    for (OperationContext captured : scrollContext.getAllValues()) {
      Assert.assertTrue(captured.isSystemAuth());
      assertLiveSubtreeFlags(captured);
    }
    Assert.assertEquals(
        proposalUrns(captureDeleteProposals(mockClient, userContext)),
        List.of(child, TEST_DOCUMENT_URN));
    verify(mockClient, never())
        .search(
            any(OperationContext.class),
            anyString(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt());
  }

  @Test
  public void testDeleteDocumentRejectsEmptyScrollPageWithScrollId() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    when(mockClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            nullable(String.class),
            nullable(String.class),
            nullable(List.class),
            any(),
            anyList()))
        .thenReturn(
            new ScrollResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setPageSize(DocumentDeleteResult.SCROLL_PAGE_SIZE)
                .setScrollId("more"));
    final DocumentService service = new DocumentService(mockClient);

    Assert.expectThrows(
        IllegalStateException.class,
        () -> service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC));
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testDeleteDocumentRejectsNullScrollPageWithoutWriting() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), eq(TEST_DOCUMENT_URN))).thenReturn(true);
    when(mockClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            nullable(String.class),
            nullable(String.class),
            nullable(List.class),
            any(),
            anyList()))
        .thenReturn(null);
    final DocumentService service = new DocumentService(mockClient);

    Assert.expectThrows(
        IllegalStateException.class,
        () -> service.deleteDocument(opContext, TEST_DOCUMENT_URN, SearchIndexMode.SYNC));
    verify(mockClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  private static void stubChildScroll(
      SystemEntityClient mockClient, Map<Urn, List<List<Urn>>> pagesByParent) throws Exception {
    stubChildScroll(mockClient, pagesByParent, Map.of());
  }

  private static void stubChildScroll(
      SystemEntityClient mockClient,
      Map<Urn, List<List<Urn>>> pagesByParent,
      Map<Urn, Status> statusByUrn)
      throws Exception {
    stubChildScroll(mockClient, pagesByParent, statusByUrn, Map.of());
  }

  private static void stubChildScroll(
      SystemEntityClient mockClient,
      Map<Urn, List<List<Urn>>> pagesByParent,
      Map<Urn, Status> statusByUrn,
      Map<Urn, Urn> storedParentOverrides)
      throws Exception {
    final Map<Urn, Urn> storedParentByChild = new HashMap<>();
    for (Map.Entry<Urn, List<List<Urn>>> entry : pagesByParent.entrySet()) {
      for (List<Urn> page : entry.getValue()) {
        for (Urn child : page) {
          storedParentByChild.putIfAbsent(child, entry.getKey());
        }
      }
    }
    when(mockClient.batchGetV2(
            any(OperationContext.class),
            anyString(),
            anySet(),
            nullable(Set.class),
            nullable(Boolean.class)))
        .thenAnswer(
            invocation -> {
              final Set<?> aspects = invocation.getArgument(3);
              final Set<Urn> urns = invocation.getArgument(2);
              final boolean statusRead =
                  aspects != null && aspects.contains(Constants.STATUS_ASPECT_NAME);
              final Map<Urn, EntityResponse> stored = new HashMap<>();
              for (Urn urn : urns) {
                if (statusRead) {
                  final Status status = statusByUrn.get(urn);
                  if (status != null) {
                    stored.put(urn, entityWithAspect(urn, Constants.STATUS_ASPECT_NAME, status));
                  }
                } else {
                  final Urn override = storedParentOverrides.get(urn);
                  final Urn parent = override != null ? override : storedParentByChild.get(urn);
                  if (parent != null) {
                    stored.put(urn, documentWithParent(urn, parent));
                  }
                }
              }
              return stored;
            });
    when(mockClient.scrollAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            nullable(String.class),
            nullable(String.class),
            nullable(List.class),
            any(),
            anyList()))
        .thenAnswer(
            invocation -> {
              final Filter filter = invocation.getArgument(3);
              final String scrollId = invocation.getArgument(4);
              final com.linkedin.metadata.query.filter.Criterion criterion =
                  filter.getOr().get(0).getAnd().get(0);
              Assert.assertEquals(criterion.getField(), "parentDocument");
              Assert.assertEquals(criterion.getCondition(), Condition.EQUAL);
              final List<String> parentValues = criterion.getValues();
              final int pageIndex = scrollId == null ? 0 : Integer.parseInt(scrollId);
              final List<Urn> page = new ArrayList<>();
              boolean hasNext = false;
              for (String parentValue : parentValues) {
                final List<List<Urn>> parentPages =
                    pagesByParent.getOrDefault(UrnUtils.getUrn(parentValue), List.of());
                if (pageIndex < parentPages.size()) {
                  page.addAll(parentPages.get(pageIndex));
                }
                if (pageIndex + 1 < parentPages.size()) {
                  hasNext = true;
                }
              }
              if (page.isEmpty()) {
                return new ScrollResult()
                    .setEntities(new SearchEntityArray())
                    .setNumEntities(0)
                    .setPageSize(DocumentDeleteResult.SCROLL_PAGE_SIZE);
              }
              final SearchEntityArray entities = new SearchEntityArray();
              for (Urn child : page) {
                entities.add(new SearchEntity().setEntity(child));
              }
              final ScrollResult result =
                  new ScrollResult()
                      .setEntities(entities)
                      .setNumEntities(page.size())
                      .setPageSize(DocumentDeleteResult.SCROLL_PAGE_SIZE);
              if (hasNext) {
                result.setScrollId(Integer.toString(pageIndex + 1));
              }
              return result;
            });
  }

  private static EntityResponse documentWithParent(Urn urn, Urn parent) {
    final DocumentInfo info = new DocumentInfo();
    info.setContents(new DocumentContents().setText(""));
    final AuditStamp stamp = new AuditStamp().setTime(0L).setActor(TEST_USER_URN);
    info.setCreated(stamp);
    info.setLastModified(stamp);
    info.setStatus(new DocumentStatus().setState(DocumentState.PUBLISHED));
    info.setParentDocument(new ParentDocument().setDocument(parent));
    return entityWithAspect(urn, Constants.DOCUMENT_INFO_ASPECT_NAME, info);
  }

  private static EntityResponse entityWithAspect(
      Urn urn, String aspectName, com.linkedin.data.template.RecordTemplate aspect) {
    final EnvelopedAspect enveloped = new EnvelopedAspect();
    enveloped.setValue(new com.linkedin.entity.Aspect(aspect.data()));
    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(aspectName, enveloped);
    final EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setAspects(aspects);
    return response;
  }

  private static Status statusOf(MetadataChangeProposal proposal) throws Exception {
    return GenericRecordUtils.deserializeAspect(
        proposal.getAspect().getValue(), proposal.getAspect().getContentType(), Status.class);
  }

  private static List<MetadataChangeProposal> captureDeleteProposals(
      SystemEntityClient mockClient, OperationContext writeContext) throws Exception {
    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockClient).batchIngestProposals(eq(writeContext), captor.capture(), eq(false));
    verify(mockClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
    for (MetadataChangeProposal proposal : captor.getValue()) {
      assertRemoved(proposal);
    }
    return captor.getValue();
  }

  private static List<Urn> proposalUrns(List<MetadataChangeProposal> proposals) {
    return proposals.stream()
        .map(MetadataChangeProposal::getEntityUrn)
        .collect(Collectors.toList());
  }

  private static void assertRemoved(MetadataChangeProposal proposal) throws Exception {
    final Status status =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(), proposal.getAspect().getContentType(), Status.class);
    Assert.assertTrue(status.isRemoved());
    Assert.assertEquals(proposal.getAspectName(), Constants.STATUS_ASPECT_NAME);
  }

  private static void assertLiveSubtreeFlags(OperationContext scrollContext) {
    final SearchFlags flags = scrollContext.getSearchContext().getSearchFlags();
    Assert.assertFalse(flags.isIncludeSoftDeleted());
    Assert.assertTrue(flags.isIncludeHiddenLifecycleStages());
    Assert.assertFalse(flags.isRewriteQuery());
    Assert.assertTrue(flags.isSkipCache());
    Assert.assertTrue(flags.isSkipHighlighting());
    Assert.assertTrue(flags.isSkipAggregates());
  }
}
