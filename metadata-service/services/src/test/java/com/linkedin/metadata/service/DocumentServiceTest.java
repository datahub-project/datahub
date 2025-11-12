package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DocumentServiceTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:test-document");
  private static final Urn TEST_PARENT_URN = UrnUtils.getUrn("urn:li:document:parent-document");
  private static final Urn TEST_ASSET_URN = UrnUtils.getUrn("urn:li:dataset:test-dataset");
  private static final OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

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
            null, // no draftOfUrn
            TEST_USER_URN);

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
            null, // no draftOfUrn
            TEST_USER_URN);

    // Verify the URN was created with custom ID
    Assert.assertNotNull(documentUrn);
    Assert.assertTrue(documentUrn.toString().contains("custom-id"));

    // Verify ingest was called (should batch both info and relationships)
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(List.class), eq(false));
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
          null, // no draftOfUrn
          TEST_USER_URN);
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
        opContext, TEST_DOCUMENT_URN, "New content", "Updated Title", null, TEST_USER_URN);

    // Verify batch ingest was called
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));
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
          opContext, TEST_DOCUMENT_URN, "Content", null, null, TEST_USER_URN);
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
        TEST_USER_URN);

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
        opContext, TEST_DOCUMENT_URN, Arrays.asList(TEST_ASSET_URN), null, TEST_USER_URN);

    // Verify ingest was called
    verify(mockClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testMoveArticleSuccess() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithRelationships();
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test moving document to new parent
    service.moveDocument(opContext, TEST_DOCUMENT_URN, TEST_PARENT_URN, TEST_USER_URN);

    // Verify ingest was called
    verify(mockClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testMoveArticleToRoot() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClientWithRelationships();
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test moving document to root (no parent)
    service.moveDocument(opContext, TEST_DOCUMENT_URN, null, TEST_USER_URN);

    // Verify ingest was called
    verify(mockClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testMoveArticleToItself() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test moving document to itself (should fail)
    try {
      service.moveDocument(opContext, TEST_DOCUMENT_URN, TEST_DOCUMENT_URN, TEST_USER_URN);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot move"));
    }
  }

  @Test
  public void testDeleteArticleSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    final DocumentService service = new DocumentService(mockClient);

    // Test soft deleting a document
    service.deleteDocument(opContext, TEST_DOCUMENT_URN);

    // Verify ingestProposal was called to set Status aspect with removed=true
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testDeleteArticleNotFound() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    final DocumentService service = new DocumentService(mockClient);

    // Test deleting a non-existent document
    try {
      service.deleteDocument(opContext, TEST_DOCUMENT_URN);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("does not exist"));
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
        TEST_USER_URN);

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
          TEST_USER_URN);
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
    service.setDocumentOwnership(opContext, TEST_DOCUMENT_URN, owners, TEST_USER_URN);

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
        opContext, TEST_DOCUMENT_URN, java.util.Collections.emptyList(), TEST_USER_URN);

    // Verify that ingestProposal was called once
    verify(mockClient, times(1))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class), eq(false));
  }
}
