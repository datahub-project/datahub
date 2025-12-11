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
import com.linkedin.datahub.graphql.generated.MoveDocumentInput;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MoveDocumentResolverTest {

  private static final String TEST_ARTICLE_URN = "urn:li:document:test-document";
  private static final String TEST_PARENT_URN = "urn:li:document:parent-document";

  private DocumentService mockService;
  private MoveDocumentResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private MoveDocumentInput input;

  @BeforeMethod
  public void setupTest() {
    mockService = mock(DocumentService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    // Setup default input
    input = new MoveDocumentInput();
    input.setUrn(TEST_ARTICLE_URN);
    input.setParentDocument(TEST_PARENT_URN);

    resolver = new MoveDocumentResolver(mockService);
  }

  @Test
  public void testMoveArticleSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify service was called
    verify(mockService, times(1))
        .moveDocument(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_ARTICLE_URN)),
            eq(UrnUtils.getUrn(TEST_PARENT_URN)),
            any(Urn.class));
  }

  @Test
  public void testMoveArticleToRoot() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    input.setParentDocument(null); // Move to root

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify service was called with null parent
    verify(mockService, times(1))
        .moveDocument(
            any(OperationContext.class),
            eq(UrnUtils.getUrn(TEST_ARTICLE_URN)),
            eq(null),
            any(Urn.class));
  }

  @Test
  public void testMoveArticleUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0)).moveDocument(any(OperationContext.class), any(), any(), any());
  }

  @Test
  public void testMoveArticleServiceThrowsException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .moveDocument(any(OperationContext.class), any(), any(), any());

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
