package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.metadata.authorization.PoliciesConfig.EDIT_ENTITY_PRIVILEGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.PROPOSE_ENTITY_DOMAINS_PRIVILEGE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ProposeDomainInput;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposeDomainResolverTest {

  @Mock private ActionRequestService mockActionRequestService;

  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;

  @Mock private QueryContext mockQueryContext;

  @Mock private OperationContext mockOperationContext;

  private ProposeDomainResolver resolver;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    resolver = new ProposeDomainResolver(mockActionRequestService);
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.any(String.class), Mockito.any(EntitySpec.class), Mockito.any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, null));
    Mockito.when(mockQueryContext.getOperationContext()).thenReturn(mockOperationContext);
  }

  @Test
  public void testProposeDomainSuccess() throws Exception {
    // GIVEN
    ProposeDomainInput input = new ProposeDomainInput();
    input.setDomainUrn("urn:li:domain:Test-Domain");
    input.setResourceUrn("urn:li:dataset:123");

    // Mock the GraphQL environment
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the ActionRequestService for schema field Domain
    Urn proposedRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:999");
    when(mockActionRequestService.proposeEntityDomain(
            any(), any(Urn.class), eq(UrnUtils.getUrn("urn:li:domain:Test-Domain")), eq(null)))
        .thenReturn(proposedRequestUrn);

    // WHEN
    CompletableFuture<String> futureResult = resolver.get(mockDataFetchingEnvironment);
    String result = futureResult.get(); // resolve the CompletableFuture

    // THEN
    Assert.assertEquals(result, "urn:li:actionRequest:999");
    verify(mockActionRequestService)
        .proposeEntityDomain(
            eq(mockQueryContext.getOperationContext()),
            eq(UrnUtils.getUrn("urn:li:dataset:123")),
            eq(UrnUtils.getUrn("urn:li:domain:Test-Domain")),
            eq(null));
  }

  @Test
  public void testProposeDomainUnauthorized() {
    // GIVEN
    ProposeDomainInput input = new ProposeDomainInput();
    input.setDomainUrn("urn:li:domain:Test-Domain");
    input.setResourceUrn("urn:li:dataset:789");

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.eq(PROPOSE_ENTITY_DOMAINS_PRIVILEGE.getType()),
                Mockito.any(EntitySpec.class),
                any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.DENY, null));
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.eq(EDIT_ENTITY_PRIVILEGE.getType()),
                Mockito.any(EntitySpec.class),
                Mockito.any()))
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
  public void testProposeDomainRemoteInvocationException() throws Exception {
    // GIVEN
    ProposeDomainInput input = new ProposeDomainInput();
    input.setDomainUrn("urn:li:domain:Test-Domain");
    input.setResourceUrn("urn:li:dataset:123");

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the service to throw RemoteInvocationException
    doThrow(new RemoteInvocationException("Downstream service unreachable"))
        .when(mockActionRequestService)
        .proposeEntityDomain(
            any(), any(Urn.class), eq(UrnUtils.getUrn("urn:li:domain:Test-Domain")), eq(null));

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
