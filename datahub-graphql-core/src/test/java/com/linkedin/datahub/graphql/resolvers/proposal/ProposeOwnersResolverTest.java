package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.metadata.authorization.PoliciesConfig.EDIT_ENTITY_PRIVILEGE;
import static com.linkedin.metadata.authorization.PoliciesConfig.PROPOSE_ENTITY_OWNERS_PRIVILEGE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.generated.ProposeOwnersInput;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.service.EntityDoesNotExistException;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposeOwnersResolverTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test1";
  private static final String TEST_GROUP_URN = "urn:li:corpGroup:test2";
  private static final String TEST_DATASET_URN = "urn:li:dataset:123";
  private static final String TEST_OWNERSHIP_TYPE_URN = "urn:li:ownershipType:test-ownership-type";

  @Mock private ActionRequestService mockActionRequestService;

  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;

  @Mock private QueryContext mockQueryContext;

  @Mock private OperationContext mockOperationContext;

  private ProposeOwnersResolver resolver;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    resolver = new ProposeOwnersResolver(mockActionRequestService);
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.any(String.class), Mockito.any(EntitySpec.class), Mockito.any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, null));
    Mockito.when(mockQueryContext.getOperationContext()).thenReturn(mockOperationContext);
  }

  @Test
  public void testProposeOwnersSuccess() throws Exception {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                TEST_USER_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER, null),
            new OwnerInput(
                TEST_GROUP_URN, OwnerEntityType.CORP_GROUP, null, TEST_OWNERSHIP_TYPE_URN)));
    input.setResourceUrn(TEST_DATASET_URN);

    // Mock the GraphQL environment
    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    List<Owner> expectedProposedOwners =
        ImmutableList.of(
            new Owner()
                .setOwner(UrnUtils.getUrn(TEST_USER_URN))
                .setType(com.linkedin.common.OwnershipType.TECHNICAL_OWNER)
                .setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL)),
            new Owner()
                .setOwner(UrnUtils.getUrn(TEST_GROUP_URN))
                .setType(com.linkedin.common.OwnershipType.CUSTOM)
                .setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL))
                .setTypeUrn(UrnUtils.getUrn(TEST_OWNERSHIP_TYPE_URN)));

    // Stub the ActionRequestService for schema field Domain
    Urn proposedRequestUrn = UrnUtils.getUrn("urn:li:actionRequest:999");
    when(mockActionRequestService.proposeEntityOwners(
            any(), eq(UrnUtils.getUrn(TEST_DATASET_URN)), eq(expectedProposedOwners), eq(null)))
        .thenReturn(proposedRequestUrn);

    // WHEN
    CompletableFuture<String> futureResult = resolver.get(mockDataFetchingEnvironment);
    String result = futureResult.get(); // resolve the CompletableFuture

    // THEN
    Assert.assertEquals(result, "urn:li:actionRequest:999");
    verify(mockActionRequestService)
        .proposeEntityOwners(
            eq(mockQueryContext.getOperationContext()),
            eq(UrnUtils.getUrn(TEST_DATASET_URN)),
            eq(expectedProposedOwners),
            eq(null));
  }

  @Test
  public void testProposeOwnersUnauthorized() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                TEST_USER_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER, null)));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);
    Mockito.when(
            mockOperationContext.authorize(
                Mockito.eq(PROPOSE_ENTITY_OWNERS_PRIVILEGE.getType()),
                Mockito.any(EntitySpec.class),
                Mockito.any()))
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
  public void testProposeOwnersRemoteInvocationException() throws Exception {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                TEST_USER_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER, null)));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the service to throw RemoteInvocationException
    doThrow(new RemoteInvocationException("Downstream service unreachable"))
        .when(mockActionRequestService)
        .proposeEntityOwners(any(), any(Urn.class), any(), eq(null));

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

  /** Test invalid owner URN format. */
  @Test
  public void testProposeOwnersInvalidOwnerUrn() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    // The user-provided URN is badly formed
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                "not-a-valid-urn",
                OwnerEntityType.CORP_USER,
                OwnershipType.TECHNICAL_OWNER,
                null)));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException due to invalid owner URN");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;
      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException.getMessage().contains("Invalid owner urn not-a-valid-urn provided."),
          "Exception message should mention invalid owner URN");
    }

    verifyNoInteractions(mockActionRequestService);
  }

  /** Test empty owners format. */
  @Test
  public void testProposeOwnersEmptyOwners() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    // The user-provided URN is badly formed
    input.setOwners(Collections.emptyList());
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException due to invalid owner URN");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;
      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException.getMessage().contains("At least one owner must be provided."),
          "Exception message should mention empty owners message");
    }

    verifyNoInteractions(mockActionRequestService);
  }

  /** Test owner entity type not corpuser or corpGroup. */
  @Test
  public void testProposeOwnersInvalidOwnerEntityType() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    // Pretend there's an "org" entity type (which isn't valid in the code).
    String invalidUrn = "urn:li:org:someorg";
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                invalidUrn, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER, null)));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException due to invalid entity type");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;
      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException
              .getMessage()
              .contains("All provided owners must be of type 'corpuser' or 'corpGroup'"),
          "Exception message should mention invalid entity type");
    }

    verifyNoInteractions(mockActionRequestService);
  }

  /** Test invalid ownership type URN format. */
  @Test
  public void testProposeOwnersInvalidOwnershipTypeUrn() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    // Provide an invalid ownershipType URN
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                TEST_USER_URN,
                OwnerEntityType.CORP_USER,
                OwnershipType.CUSTOM,
                "blah-invalid-ownershipType")));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException due to invalid ownership type URN");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;
      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException
              .getMessage()
              .contains("Invalid ownership type urn blah-invalid-ownershipType provided."),
          "Exception message should mention invalid ownership type URN");
    }

    verifyNoInteractions(mockActionRequestService);
  }

  /** Test ownership type URN is not of entityType 'ownershipType'. */
  @Test
  public void testProposeOwnersNonOwnershipTypeEntity() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    // Provide a well-formed URN but not the correct entity type
    String wrongEntityTypeUrn = "urn:li:dataset:999";
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                TEST_USER_URN,
                OwnerEntityType.CORP_USER,
                OwnershipType.CUSTOM,
                wrongEntityTypeUrn)));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail(
          "Expected DataHubGraphQLException because the ownership type entity is not 'ownershipType'");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;
      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException
              .getMessage()
              .contains("All provided ownership types must be of type 'ownershipType'"),
          "Exception message should mention invalid ownership type entity");
    }

    verifyNoInteractions(mockActionRequestService);
  }

  /** Test CUSTOM ownership type but missing the ownership type URN. */
  @Test
  public void testProposeOwnersMissingOwnershipTypeUrnForCustom() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    // Provide a custom type, but no ownershipTypeUrn
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(TEST_USER_URN, OwnerEntityType.CORP_USER, OwnershipType.CUSTOM, null)));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail(
          "Expected DataHubGraphQLException because ownershipTypeUrn must be supplied for CUSTOM type");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;
      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException
              .getMessage()
              .contains("Ownership type urn must be provided for custom owner type"),
          "Exception message should mention missing ownership type urn for CUSTOM");
    }

    verifyNoInteractions(mockActionRequestService);
  }

  /** Test invalid resource URN format. */
  @Test
  public void testProposeOwnersInvalidResourceUrn() {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                TEST_USER_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER, null)));
    // Provide an invalid resource URN
    input.setResourceUrn("invalid-resource-urn");

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException due to invalid resource URN");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;
      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
      Assert.assertTrue(
          graphQLException.getMessage().contains("Invalid resource urn provided."),
          "Exception message should mention invalid resource URN");
    }

    verifyNoInteractions(mockActionRequestService);
  }

  /** Test entity does not exist scenario. */
  @Test
  public void testProposeOwnersEntityDoesNotExist() throws Exception {
    // GIVEN
    ProposeOwnersInput input = new ProposeOwnersInput();
    input.setOwners(
        ImmutableList.of(
            new OwnerInput(
                TEST_USER_URN, OwnerEntityType.CORP_USER, OwnershipType.TECHNICAL_OWNER, null)));
    input.setResourceUrn(TEST_DATASET_URN);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // Stub the service to throw EntityDoesNotExistException
    doThrow(new EntityDoesNotExistException("Resource does not exist"))
        .when(mockActionRequestService)
        .proposeEntityOwners(any(), any(Urn.class), any(), eq(null));

    // WHEN & THEN
    try {
      resolver.get(mockDataFetchingEnvironment).join();
      Assert.fail("Expected DataHubGraphQLException with NOT_FOUND error code");
    } catch (Exception e) {
      Throwable cause = e.getCause();
      Assert.assertTrue(cause instanceof DataHubGraphQLException);
      DataHubGraphQLException graphQLException = (DataHubGraphQLException) cause;

      Assert.assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.NOT_FOUND);
      Assert.assertTrue(
          graphQLException
              .getMessage()
              .contains("Failed to create owners proposal: Resource does not exist"),
          "Exception message should mention missing entity");
    }
  }
}
