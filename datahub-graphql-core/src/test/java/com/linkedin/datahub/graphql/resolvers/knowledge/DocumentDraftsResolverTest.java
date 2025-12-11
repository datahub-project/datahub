/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.knowledge;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DocumentDraftsResolverTest {

  private static final Urn TEST_PUBLISHED_URN =
      UrnUtils.getUrn("urn:li:document:published-document");
  private static final Urn TEST_DRAFT_1_URN = UrnUtils.getUrn("urn:li:document:draft-1");
  private static final Urn TEST_DRAFT_2_URN = UrnUtils.getUrn("urn:li:document:draft-2");

  private DocumentService mockService;
  private DocumentDraftsResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private QueryContext mockContext;
  private Document sourceDocument;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    // Setup source document
    sourceDocument = new Document();
    sourceDocument.setUrn(TEST_PUBLISHED_URN.toString());
    sourceDocument.setType(EntityType.DOCUMENT);

    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getSource()).thenReturn(sourceDocument);

    resolver = new DocumentDraftsResolver(mockService);
  }

  @Test
  public void testGetDraftsSuccess() throws Exception {
    // Mock search results
    SearchEntity draft1 = new SearchEntity();
    draft1.setEntity(TEST_DRAFT_1_URN);

    SearchEntity draft2 = new SearchEntity();
    draft2.setEntity(TEST_DRAFT_2_URN);

    SearchResult searchResult = new SearchResult();
    SearchEntityArray entities = new SearchEntityArray();
    entities.add(draft1);
    entities.add(draft2);
    searchResult.setEntities(entities);

    when(mockService.getDraftDocuments(
            any(OperationContext.class), any(Urn.class), anyInt(), anyInt()))
        .thenReturn(searchResult);

    List<Document> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getUrn(), TEST_DRAFT_1_URN.toString());
    assertEquals(result.get(0).getType(), EntityType.DOCUMENT);
    assertEquals(result.get(1).getUrn(), TEST_DRAFT_2_URN.toString());
    assertEquals(result.get(1).getType(), EntityType.DOCUMENT);

    // Verify service was called
    verify(mockService, times(1))
        .getDraftDocuments(any(OperationContext.class), any(Urn.class), anyInt(), anyInt());
  }

  @Test
  public void testGetDraftsNoDrafts() throws Exception {
    // Mock empty search results
    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(new SearchEntityArray());

    when(mockService.getDraftDocuments(
            any(OperationContext.class), any(Urn.class), anyInt(), anyInt()))
        .thenReturn(searchResult);

    List<Document> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetDraftsServiceThrowsException() throws Exception {
    when(mockService.getDraftDocuments(
            any(OperationContext.class), any(Urn.class), anyInt(), anyInt()))
        .thenThrow(new RuntimeException("Service error"));

    try {
      resolver.get(mockEnv).get();
      fail("Expected RuntimeException to be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to fetch draft documents"));
    }
  }
}
