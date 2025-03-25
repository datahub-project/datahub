package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.metadata.authorization.PoliciesConfig.EDIT_ENTITY_PRIVILEGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.PROPOSE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ProposeTermsInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposeTermsResolverTest {

  @Mock private ActionRequestService mockActionRequestService;

  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;

  @Mock private QueryContext mockQueryContext;

  @Mock private OperationContext mockOperationContext;

  private ProposeTermsResolver resolver;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    resolver = new ProposeTermsResolver(mockActionRequestService);
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.any(String.class), Mockito.any(EntitySpec.class)))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, null));
    Mockito.when(mockQueryContext.getOperationContext()).thenReturn(mockOperationContext);
  }

  @Test
  public void testProposeSchemaFieldTermsSuccess() throws Exception {
    // GIVEN
    ProposeTermsInput input = new ProposeTermsInput();
    input.setTermUrns(
        Arrays.asList("urn:li:glossaryTerm:TestTerm", "urn:li:glossaryTerm:TestTerm2"));
    input.setResourceUrn("urn:li:dataset:123");
    input.setSubResourceType(SubResourceType.DATASET_FIELD);
    input.setSubResource("fieldName");

    // Mock the GraphQL environment
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the ActionRequestService for schema field glossaryTerms
    Urn proposedRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:999");
    when(mockActionRequestService.proposeSchemaFieldTerms(
            any(),
            any(Urn.class),
            eq("fieldName"),
            Mockito.eq(
                ImmutableList.of(
                    UrnUtils.getUrn("urn:li:glossaryTerm:TestTerm"),
                    UrnUtils.getUrn("urn:li:glossaryTerm:TestTerm2"))),
            eq(null)))
        .thenReturn(proposedRequestUrn);

    // WHEN
    CompletableFuture<String> futureResult = resolver.get(mockDataFetchingEnvironment);
    String result = futureResult.get(); // resolve the CompletableFuture

    // THEN
    Assert.assertEquals(result, "urn:li:actionRequest:999");
    verify(mockActionRequestService)
        .proposeSchemaFieldTerms(
            eq(mockQueryContext.getOperationContext()),
            eq(UrnUtils.getUrn("urn:li:dataset:123")),
            eq("fieldName"),
            anyList(),
            eq(null));
  }

  @Test
  public void testProposeEntityTermsSuccess() throws Exception {
    // GIVEN
    ProposeTermsInput input = new ProposeTermsInput();
    input.setTermUrns(
        Arrays.asList("urn:li:glossaryTerm:TestTerm2", "urn:li:glossaryTerm:TestTerm3"));
    input.setResourceUrn("urn:li:dataset:456");
    // No SubResourceType -> goes to proposeEntityTerms
    input.setSubResourceType(null);
    input.setSubResource(null);

    // Mock the GraphQL environment
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the ActionRequestService for entity glossaryTerms
    Urn proposedRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:888");
    when(mockActionRequestService.proposeEntityTerms(
            any(),
            any(Urn.class),
            Mockito.eq(
                ImmutableList.of(
                    UrnUtils.getUrn("urn:li:glossaryTerm:TestTerm2"),
                    UrnUtils.getUrn("urn:li:glossaryTerm:TestTerm3"))),
            eq(null)))
        .thenReturn(proposedRequestUrn);

    // WHEN
    CompletableFuture<String> futureResult = resolver.get(mockDataFetchingEnvironment);
    String result = futureResult.get();

    // THEN
    Assert.assertEquals(result, "urn:li:actionRequest:888");
    verify(mockActionRequestService)
        .proposeEntityTerms(
            eq(mockQueryContext.getOperationContext()),
            eq(UrnUtils.getUrn("urn:li:dataset:456")),
            anyList(),
            eq(null));
  }

  @Test
  public void testProposeTermsUnauthorized() {
    // GIVEN
    ProposeTermsInput input = new ProposeTermsInput();
    input.setTermUrns(Collections.singletonList("urn:li:glossaryTerm:TestTerm"));
    input.setResourceUrn("urn:li:dataset:789");
    input.setSubResourceType(SubResourceType.DATASET_FIELD);
    input.setSubResource("fieldName");

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.eq(PROPOSE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()),
                Mockito.any(EntitySpec.class)))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.DENY, null));
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.eq(EDIT_ENTITY_PRIVILEGE.getType()), Mockito.any(EntitySpec.class)))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.DENY, null));

    // We expect an AuthorizationException due to insufficient privileges
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected AuthorizationException to be thrown");
    } catch (Exception e) {
      // Because we're calling join(), the actual exception is wrapped in a CompletionException
      Throwable cause = e.getCause();
      Assert.assertNotNull(cause, "Expected cause to be non-null");
      Assert.assertTrue(cause instanceof AuthorizationException);
      Assert.assertTrue(cause.getMessage().contains("Unauthorized to perform this action"));
    }

    verifyNoInteractions(mockActionRequestService);
  }

  @Test
  public void testProposeTermsMalformedActionRequestException() throws Exception {
    // GIVEN
    ProposeTermsInput input = new ProposeTermsInput();
    input.setTermUrns(Arrays.asList("urn:li:glossaryTerm:BadTerm"));
    input.setResourceUrn("urn:li:dataset:000");
    input.setSubResourceType(SubResourceType.DATASET_FIELD);
    input.setSubResource("fieldName");

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the service to throw MalformedActionRequestException
    doThrow(new ActionRequestService.MalformedActionRequestException("Invalid request"))
        .when(mockActionRequestService)
        .proposeSchemaFieldTerms(any(), any(Urn.class), eq("fieldName"), anyList(), eq(null));

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException due to bad request");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;

      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException.getMessage().contains("Failed to propose terms: Invalid request"),
          "Exception message should indicate malformed request");
    }
  }

  @Test
  public void testProposeTermsRemoteInvocationException() throws Exception {
    // GIVEN
    ProposeTermsInput input = new ProposeTermsInput();
    input.setTermUrns(Arrays.asList("urn:li:glossaryTerm:TestTerm"));
    input.setResourceUrn("urn:li:dataset:999");
    input.setSubResourceType(SubResourceType.DATASET_FIELD);
    input.setSubResource("fieldName");

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the service to throw RemoteInvocationException
    doThrow(new RemoteInvocationException("Downstream service unreachable"))
        .when(mockActionRequestService)
        .proposeSchemaFieldTerms(any(), any(Urn.class), eq("fieldName"), anyList(), eq(null));

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException due to server error");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;

      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.SERVER_ERROR);
      Assert.assertTrue(
          graphQLException
              .getMessage()
              .contains("Encountered an error while attempting to reach the downstream service"),
          "Exception message should indicate remote invocation error");
    }
  }
}
