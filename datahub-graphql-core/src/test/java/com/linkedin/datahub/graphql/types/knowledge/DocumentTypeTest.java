/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DocumentKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DocumentTypeTest {

  private static final String TEST_DOCUMENT_1_URN = "urn:li:document:document-1";
  private static final DocumentKey TEST_DOCUMENT_1_KEY = new DocumentKey().setId("document-1");

  private static final DocumentInfo TEST_DOCUMENT_1_INFO = createTestDocumentInfo();

  private static final Ownership TEST_DOCUMENT_1_OWNERSHIP =
      new Ownership()
          .setOwners(
              new OwnerArray(
                  ImmutableList.of(
                      new Owner()
                          .setType(OwnershipType.DATAOWNER)
                          .setOwner(Urn.createFromTuple("corpuser", "test")))));

  private static final String TEST_DOCUMENT_2_URN = "urn:li:document:document-2";

  private static DocumentInfo createTestDocumentInfo() {
    DocumentInfo info = new DocumentInfo();
    // Type is now stored in subTypes aspect, not in info
    info.setTitle("Test Tutorial");

    DocumentContents contents = new DocumentContents();
    contents.setText("Test content");
    info.setContents(contents);

    AuditStamp createdStamp = new AuditStamp();
    createdStamp.setTime(1640995200000L);
    createdStamp.setActor(Urn.createFromTuple("corpuser", "testuser"));
    info.setCreated(createdStamp);

    AuditStamp lastModifiedStamp = new AuditStamp();
    lastModifiedStamp.setTime(1640995200000L);
    lastModifiedStamp.setActor(Urn.createFromTuple("corpuser", "testuser"));
    info.setLastModified(lastModifiedStamp);

    return info;
  }

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn documentUrn1 = Urn.createFromString(TEST_DOCUMENT_1_URN);
    Urn documentUrn2 = Urn.createFromString(TEST_DOCUMENT_2_URN);

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.DOCUMENT_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(documentUrn1, documentUrn2))),
                Mockito.eq(DocumentType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                documentUrn1,
                new EntityResponse()
                    .setEntityName(Constants.DOCUMENT_ENTITY_NAME)
                    .setUrn(documentUrn1)
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOCUMENT_KEY_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DOCUMENT_1_KEY.data())),
                                Constants.DOCUMENT_INFO_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DOCUMENT_1_INFO.data())),
                                Constants.OWNERSHIP_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(TEST_DOCUMENT_1_OWNERSHIP.data())))))));

    DocumentType type = new DocumentType(client);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<Document>> result =
        type.batchLoad(ImmutableList.of(TEST_DOCUMENT_1_URN, TEST_DOCUMENT_2_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.DOCUMENT_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(documentUrn1, documentUrn2)),
            Mockito.eq(DocumentType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    Document document1 = result.get(0).getData();
    assertEquals(document1.getUrn(), TEST_DOCUMENT_1_URN);
    assertEquals(document1.getType(), EntityType.DOCUMENT);
    assertEquals(document1.getOwnership().getOwners().size(), 1);
    assertEquals(document1.getInfo().getTitle(), "Test Tutorial");
    assertEquals(document1.getInfo().getContents().getText(), "Test content");

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    DocumentType type = new DocumentType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_DOCUMENT_1_URN, TEST_DOCUMENT_2_URN), context));
  }

  @Test
  public void testEntityType() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DocumentType type = new DocumentType(mockClient);

    assertEquals(type.type(), EntityType.DOCUMENT);
  }

  @Test
  public void testObjectClass() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DocumentType type = new DocumentType(mockClient);

    assertEquals(type.objectClass(), Document.class);
  }

  @Test
  public void testAutoComplete() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock autocomplete result
    com.linkedin.metadata.query.AutoCompleteResult mockResult =
        new com.linkedin.metadata.query.AutoCompleteResult();
    mockResult.setQuery("test");
    mockResult.setSuggestions(new com.linkedin.data.template.StringArray());
    mockResult.setEntities(new com.linkedin.metadata.query.AutoCompleteEntityArray());

    Mockito.when(
            mockClient.autoComplete(
                any(),
                Mockito.eq(Constants.DOCUMENT_ENTITY_NAME),
                Mockito.eq("test"),
                Mockito.any(),
                Mockito.eq(10)))
        .thenReturn(mockResult);

    DocumentType type = new DocumentType(mockClient);
    QueryContext context = getMockAllowContext();

    // Execute autocomplete
    com.linkedin.datahub.graphql.generated.AutoCompleteResults result =
        type.autoComplete("test", null, null, 10, context);

    // Verify
    assertNotNull(result);
    assertEquals(result.getQuery(), "test");
    Mockito.verify(mockClient, Mockito.times(1))
        .autoComplete(
            any(),
            Mockito.eq(Constants.DOCUMENT_ENTITY_NAME),
            Mockito.eq("test"),
            Mockito.any(),
            Mockito.eq(10));
  }

  @Test(expectedExceptions = org.apache.commons.lang3.NotImplementedException.class)
  public void testSearchThrowsNotImplementedException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DocumentType type = new DocumentType(mockClient);
    QueryContext context = getMockAllowContext();

    // This should throw NotImplementedException
    type.search("test query", null, 0, 10, context);
  }

  @Test
  public void testGetKeyProvider() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DocumentType type = new DocumentType(mockClient);

    Document document = new Document();
    document.setUrn("urn:li:document:test");

    String key = type.getKeyProvider().apply(document);
    assertEquals(key, "urn:li:document:test");
  }
}
