package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ParentDocumentsResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.ParentDocument;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ParentDocumentsResolverTest {
  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn documentUrn = Urn.createFromString("urn:li:document:test-doc");
    Document documentEntity = new Document();
    documentEntity.setUrn(documentUrn.toString());
    documentEntity.setType(EntityType.DOCUMENT);
    Mockito.when(mockEnv.getSource()).thenReturn(documentEntity);

    Urn parentDoc1Urn = Urn.createFromString("urn:li:document:parent-doc-1");
    Urn parentDoc2Urn = Urn.createFromString("urn:li:document:parent-doc-2");

    // Create document info with parent reference
    final ParentDocument parentDoc1Ref = new ParentDocument().setDocument(parentDoc1Urn);
    final ParentDocument parentDoc2Ref = new ParentDocument().setDocument(parentDoc2Urn);

    // Document content (required field)
    final DocumentContents content = new DocumentContents().setText("Test content");

    // Audit stamp (required fields)
    final AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testUser"));

    // Child document has parent1 as parent
    final DocumentInfo childDocInfo =
        new DocumentInfo()
            .setContents(content)
            .setCreated(auditStamp)
            .setLastModified(auditStamp)
            .setParentDocument(parentDoc1Ref);

    // Parent1 document has parent2 as parent
    final DocumentInfo parent1DocInfo =
        new DocumentInfo()
            .setContents(content)
            .setCreated(auditStamp)
            .setLastModified(auditStamp)
            .setParentDocument(parentDoc2Ref);

    // Parent2 document has no parent (root level)
    final DocumentInfo parent2DocInfo =
        new DocumentInfo().setContents(content).setCreated(auditStamp).setLastModified(auditStamp);

    Map<String, EnvelopedAspect> childDocAspects = new HashMap<>();
    childDocAspects.put(
        DOCUMENT_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(childDocInfo.data())));

    Map<String, EnvelopedAspect> parent1DocAspects = new HashMap<>();
    parent1DocAspects.put(
        DOCUMENT_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parent1DocInfo.data())));

    Map<String, EnvelopedAspect> parent2DocAspects = new HashMap<>();
    parent2DocAspects.put(
        DOCUMENT_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(parent2DocInfo.data())));

    // Mock client responses for fetching document info aspects
    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(DOCUMENT_ENTITY_NAME),
                Mockito.eq(documentUrn),
                Mockito.eq(Collections.singleton(DOCUMENT_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(documentUrn)
                .setAspects(new EnvelopedAspectMap(childDocAspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(DOCUMENT_ENTITY_NAME),
                Mockito.eq(parentDoc1Urn),
                Mockito.eq(Collections.singleton(DOCUMENT_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(parentDoc1Urn)
                .setAspects(new EnvelopedAspectMap(parent1DocAspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(DOCUMENT_ENTITY_NAME),
                Mockito.eq(parentDoc2Urn),
                Mockito.eq(Collections.singleton(DOCUMENT_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(parentDoc2Urn)
                .setAspects(new EnvelopedAspectMap(parent2DocAspects)));

    // Mock client responses for fetching full parent documents (with null aspects param)
    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(DOCUMENT_ENTITY_NAME),
                Mockito.eq(parentDoc1Urn),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setUrn(parentDoc1Urn)
                .setAspects(new EnvelopedAspectMap(parent1DocAspects)));

    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(DOCUMENT_ENTITY_NAME),
                Mockito.eq(parentDoc2Urn),
                Mockito.eq(null)))
        .thenReturn(
            new EntityResponse()
                .setUrn(parentDoc2Urn)
                .setAspects(new EnvelopedAspectMap(parent2DocAspects)));

    ParentDocumentsResolver resolver = new ParentDocumentsResolver(mockClient);
    ParentDocumentsResult result = resolver.get(mockEnv).get();

    // Should have called getV2 five times:
    // 1. Get child doc info (with aspect)
    // 2. Get parent1 full doc (null aspects)
    // 3. Get parent1 doc info (with aspect)
    // 4. Get parent2 full doc (null aspects)
    // 5. Get parent2 doc info (with aspect)
    Mockito.verify(mockClient, Mockito.times(5))
        .getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    assertEquals(result.getCount(), 2);
    assertEquals(result.getDocuments().get(0).getUrn(), parentDoc1Urn.toString());
    assertEquals(result.getDocuments().get(1).getUrn(), parentDoc2Urn.toString());
  }

  @Test
  public void testGetSuccessNoParents() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Urn documentUrn = Urn.createFromString("urn:li:document:root-doc");
    Document documentEntity = new Document();
    documentEntity.setUrn(documentUrn.toString());
    documentEntity.setType(EntityType.DOCUMENT);
    Mockito.when(mockEnv.getSource()).thenReturn(documentEntity);

    // Document content (required field)
    final DocumentContents content = new DocumentContents().setText("Test content");

    // Audit stamp (required fields)
    final AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(Urn.createFromString("urn:li:corpuser:testUser"));

    // Root document has no parent
    final DocumentInfo rootDocInfo =
        new DocumentInfo().setContents(content).setCreated(auditStamp).setLastModified(auditStamp);

    Map<String, EnvelopedAspect> rootDocAspects = new HashMap<>();
    rootDocAspects.put(
        DOCUMENT_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(rootDocInfo.data())));

    // Mock client response for fetching root document info
    Mockito.when(
            mockClient.getV2(
                any(),
                Mockito.eq(DOCUMENT_ENTITY_NAME),
                Mockito.eq(documentUrn),
                Mockito.eq(Collections.singleton(DOCUMENT_INFO_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(documentUrn)
                .setAspects(new EnvelopedAspectMap(rootDocAspects)));

    ParentDocumentsResolver resolver = new ParentDocumentsResolver(mockClient);
    ParentDocumentsResult result = resolver.get(mockEnv).get();

    // Should have called getV2 once to get the document info
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    assertEquals(result.getCount(), 0);
    assertEquals(result.getDocuments().size(), 0);
  }
}
