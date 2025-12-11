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
import com.linkedin.datahub.graphql.generated.MergeDraftInput;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DocumentService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MergeDraftResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_DRAFT_URN = UrnUtils.getUrn("urn:li:document:draft-document");

  private DocumentService mockService;
  private EntityService mockEntityService;
  private MergeDraftResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private MergeDraftInput input;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockService = mock(DocumentService.class);
    mockEntityService = mock(EntityService.class);
    mockEnv = mock(DataFetchingEnvironment.class);

    // Setup default input
    input = new MergeDraftInput();
    input.setDraftUrn(TEST_DRAFT_URN.toString());
    input.setDeleteDraft(true);

    resolver = new MergeDraftResolver(mockService, mockEntityService);
  }

  @Test
  public void testMergeDraftSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify service was called with correct parameters
    verify(mockService, times(1))
        .mergeDraftIntoParent(
            any(OperationContext.class),
            eq(TEST_DRAFT_URN),
            eq(true), // deleteDraft
            any(Urn.class)); // actor
  }

  @Test
  public void testMergeDraftWithoutDelete() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    input.setDeleteDraft(false);

    Boolean result = resolver.get(mockEnv).get();

    assertTrue(result);

    // Verify deleteDraft was false
    verify(mockService, times(1))
        .mergeDraftIntoParent(
            any(OperationContext.class), eq(TEST_DRAFT_URN), eq(false), any(Urn.class));
  }

  @Test
  public void testMergeDraftUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // Verify service was NOT called
    verify(mockService, times(0))
        .mergeDraftIntoParent(any(OperationContext.class), any(), any(Boolean.class), any());
  }

  @Test
  public void testMergeDraftServiceThrowsException() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockContext.getActorUrn()).thenReturn(TEST_USER_URN.toString());

    doThrow(new RuntimeException("Service error"))
        .when(mockService)
        .mergeDraftIntoParent(
            any(OperationContext.class), any(), any(Boolean.class), any(Urn.class));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
