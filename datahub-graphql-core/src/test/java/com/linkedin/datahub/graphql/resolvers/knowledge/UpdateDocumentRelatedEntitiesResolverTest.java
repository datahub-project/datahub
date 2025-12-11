/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateDocumentRelatedEntitiesInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDocumentRelatedEntitiesResolverTest {

  private static final String TEST_ARTICLE_URN = "urn:li:document:test-document";
  private static final String TEST_ASSET_URN = "urn:li:dataset:test-dataset";
  private static final String TEST_RELATED_ARTICLE_URN = "urn:li:document:related-document";

  private DocumentService mockService;
  private UpdateDocumentRelatedEntitiesResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private UpdateDocumentRelatedEntitiesInput input;

  @BeforeMethod
  public void setupTest() {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    // Setup default input
    input = new UpdateDocumentRelatedEntitiesInput();
    input.setUrn(TEST_ARTICLE_URN);
    input.setRelatedAssets(Arrays.asList(TEST_ASSET_URN));
    input.setRelatedDocuments(Arrays.asList(TEST_RELATED_ARTICLE_URN));

    resolver = new UpdateDocumentRelatedEntitiesResolver(mockService);
  }

  @Test
  public void testUpdateRelatedEntitiesSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify service was called
    verify(mockService, times(1))
        .updateDocumentRelatedEntities(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_ARTICLE_URN)),
            any(),
            any(),
            any(Urn.class));
  }

  @Test
  public void testUpdateOnlyRelatedAssets() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setRelatedDocuments(null); // Only update assets

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    verify(mockService, times(1))
        .updateDocumentRelatedEntities(
            any(OperationContext.class), any(), any(), eq(null), any(Urn.class));
  }

  @Test
  public void testClearAllRelatedEntities() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setRelatedAssets(Collections.emptyList());
    input.setRelatedDocuments(Collections.emptyList());

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);
  }

  @Test
  public void testUpdateRelatedEntitiesUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .updateDocumentRelatedEntities(any(OperationContext.class), any(), any(), any(), any());
  }

  @Test
  public void testUpdateRelatedEntitiesServiceThrowsException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .updateDocumentRelatedEntities(any(OperationContext.class), any(), any(), any(), any());

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
