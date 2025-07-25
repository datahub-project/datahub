package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReviewActionWorkflowFormRequestResolverTest {

  private static final String TEST_ACTOR_URN_STRING = "urn:li:corpuser:test";
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn(TEST_ACTOR_URN_STRING);
  private static final String TEST_ACTION_REQUEST_URN_STRING = "urn:li:actionRequest:test-request";
  private static final Urn TEST_ACTION_REQUEST_URN =
      UrnUtils.getUrn(TEST_ACTION_REQUEST_URN_STRING);

  @Mock private ActionWorkflowService mockActionWorkflowService;
  @Mock private DataFetchingEnvironment mockEnvironment;
  @Mock private QueryContext mockContext;
  @Mock private OperationContext mockOpContext;

  private ReviewActionWorkflowFormRequestResolver resolver;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
    resolver = new ReviewActionWorkflowFormRequestResolver(mockActionWorkflowService);

    // Setup common mocks
    when(mockEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN_STRING);
  }

  @AfterMethod
  public void teardown() throws Exception {
    mocks.close();
  }

  @Test
  public void testReviewActionRequestWorkflowRequestApprovalSuccess() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn(TEST_ACTION_REQUEST_URN_STRING);
    input.setResult(ActionRequestResult.ACCEPTED);
    input.setComment("Approved with comments");

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockActionWorkflowService.reviewActionWorkflowFormRequest(
            any(), any(), eq("ACCEPTED"), eq("Approved with comments"), any()))
        .thenReturn(true);

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    assertTrue(result.get());
    verify(mockActionWorkflowService)
        .reviewActionWorkflowFormRequest(
            eq(TEST_ACTION_REQUEST_URN),
            eq(TEST_ACTOR_URN),
            eq("ACCEPTED"),
            eq("Approved with comments"),
            eq(mockOpContext));
  }

  @Test
  public void testReviewActionRequestWorkflowRequestRejectionSuccess() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn(TEST_ACTION_REQUEST_URN_STRING);
    input.setResult(ActionRequestResult.REJECTED);
    input.setComment("Rejected due to insufficient information");

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockActionWorkflowService.reviewActionWorkflowFormRequest(
            any(), any(), eq("REJECTED"), eq("Rejected due to insufficient information"), any()))
        .thenReturn(true);

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    assertTrue(result.get());
    verify(mockActionWorkflowService)
        .reviewActionWorkflowFormRequest(
            eq(TEST_ACTION_REQUEST_URN),
            eq(TEST_ACTOR_URN),
            eq("REJECTED"),
            eq("Rejected due to insufficient information"),
            eq(mockOpContext));
  }

  @Test
  public void testReviewActionRequestWorkflowRequestWithoutComment() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn(TEST_ACTION_REQUEST_URN_STRING);
    input.setResult(ActionRequestResult.ACCEPTED);
    // No comment set

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockActionWorkflowService.reviewActionWorkflowFormRequest(
            any(), any(), eq("ACCEPTED"), isNull(), any()))
        .thenReturn(true);

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    assertTrue(result.get());
    verify(mockActionWorkflowService)
        .reviewActionWorkflowFormRequest(
            eq(TEST_ACTION_REQUEST_URN),
            eq(TEST_ACTOR_URN),
            eq("ACCEPTED"),
            isNull(),
            eq(mockOpContext));
  }

  @Test
  public void testReviewActionRequestWorkflowRequestServiceException() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn(TEST_ACTION_REQUEST_URN_STRING);
    input.setResult(ActionRequestResult.ACCEPTED);
    input.setComment("Should fail");

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockActionWorkflowService.reviewActionWorkflowFormRequest(
            any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("Service error"));

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    try {
      result.get();
      fail("Expected RuntimeException to be thrown");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals(e.getCause().getMessage(), "Failed to review action workflow request");
    }

    verify(mockActionWorkflowService)
        .reviewActionWorkflowFormRequest(
            eq(TEST_ACTION_REQUEST_URN),
            eq(TEST_ACTOR_URN),
            eq("ACCEPTED"),
            eq("Should fail"),
            eq(mockOpContext));
  }

  @Test
  public void testReviewActionRequestWorkflowRequestAuthorizationException() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn(TEST_ACTION_REQUEST_URN_STRING);
    input.setResult(ActionRequestResult.ACCEPTED);

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockActionWorkflowService.reviewActionWorkflowFormRequest(
            any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("User not authorized"));

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    try {
      result.get();
      fail("Expected RuntimeException to be thrown");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals(e.getCause().getMessage(), "Failed to review action workflow request");
    }
  }

  @Test
  public void testReviewActionRequestWorkflowRequestServiceReturnsFalse() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn(TEST_ACTION_REQUEST_URN_STRING);
    input.setResult(ActionRequestResult.ACCEPTED);

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockActionWorkflowService.reviewActionWorkflowFormRequest(
            any(), any(), any(), any(), any()))
        .thenReturn(false);

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    assertFalse(result.get());
    verify(mockActionWorkflowService)
        .reviewActionWorkflowFormRequest(
            eq(TEST_ACTION_REQUEST_URN),
            eq(TEST_ACTOR_URN),
            eq("ACCEPTED"),
            isNull(),
            eq(mockOpContext));
  }

  @Test
  public void testReviewActionRequestWorkflowRequestInvalidUrn() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn("invalid-urn");
    input.setResult(ActionRequestResult.ACCEPTED);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    try {
      result.get();
      fail("Expected RuntimeException to be thrown for invalid URN");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertEquals(e.getCause().getMessage(), "Failed to review action workflow request");
    }

    // Service should not be called with invalid URN
    verify(mockActionWorkflowService, never())
        .reviewActionWorkflowFormRequest(any(), any(), any(), any(), any());
  }

  @Test
  public void testReviewActionRequestWorkflowRequestNullInput() throws Exception {
    // GIVEN
    when(mockEnvironment.getArgument("input")).thenReturn(null);

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    try {
      result.get();
      fail("Expected RuntimeException to be thrown for null input");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
    }

    // Service should not be called with null input
    verify(mockActionWorkflowService, never())
        .reviewActionWorkflowFormRequest(any(), any(), any(), any(), any());
  }

  @Test
  public void testReviewActionRequestWorkflowRequestEmptyComment() throws Exception {
    // GIVEN
    ReviewActionWorkflowFormRequestInput input = new ReviewActionWorkflowFormRequestInput();
    input.setUrn(TEST_ACTION_REQUEST_URN_STRING);
    input.setResult(ActionRequestResult.ACCEPTED);
    input.setComment(""); // Empty comment

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockActionWorkflowService.reviewActionWorkflowFormRequest(
            any(), any(), eq("ACCEPTED"), eq(""), any()))
        .thenReturn(true);

    // WHEN
    CompletableFuture<Boolean> result = resolver.get(mockEnvironment);

    // THEN
    assertTrue(result.get());
    verify(mockActionWorkflowService)
        .reviewActionWorkflowFormRequest(
            eq(TEST_ACTION_REQUEST_URN),
            eq(TEST_ACTOR_URN),
            eq("ACCEPTED"),
            eq(""),
            eq(mockOpContext));
  }
}
