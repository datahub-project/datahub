package com.linkedin.datahub.graphql.resolvers.mutate.util;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@SuppressWarnings("null")
public class OwnerUtilsTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String ASSERTION_URN = "urn:li:assertion:test-guid";

  private static AssertionInfo datasetAssertionInfoFor(String datasetUrn) throws Exception {
    return new AssertionInfo()
        .setType(AssertionType.DATASET)
        .setDatasetAssertion(
            new DatasetAssertionInfo()
                .setDataset(Urn.createFromString(datasetUrn))
                .setScope(DatasetAssertionScope.DATASET_COLUMN)
                .setOperator(AssertionStdOperator.BETWEEN));
  }

  @Test
  public void testIsAuthorizedToUpdateAssertionOwnersOnEntityAllowed() throws Exception {
    QueryContext context = getMockAllowContext();
    assertTrue(
        OwnerUtils.isAuthorizedToUpdateAssertionOwnersOnEntity(
            context, Urn.createFromString(DATASET_URN)));
  }

  @Test
  public void testIsAuthorizedToUpdateAssertionOwnersOnEntityDenied() throws Exception {
    QueryContext context = getMockDenyContext();
    assertFalse(
        OwnerUtils.isAuthorizedToUpdateAssertionOwnersOnEntity(
            context, Urn.createFromString(DATASET_URN)));
  }

  @Test
  public void testValidateAuthorizedToUpdateOwnersAllowedDirectly() throws Exception {
    QueryContext context = getMockAllowContext();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    OwnerUtils.validateAuthorizedToUpdateOwners(
        context, Urn.createFromString(DATASET_URN), mockClient, mockService);
  }

  @Test(expectedExceptions = AuthorizationException.class)
  public void testValidateAuthorizedToUpdateOwnersDeniedOnNonAssertion() throws Exception {
    QueryContext context = getMockDenyContext();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    OwnerUtils.validateAuthorizedToUpdateOwners(
        context, Urn.createFromString(DATASET_URN), mockClient, mockService);
  }

  @Test
  public void testValidateAuthorizedToUpdateOwnersAssertionDelegationAllowed() throws Exception {
    QueryContext context = getMockDenyContext();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(ASSERTION_URN)),
                eq(Constants.ASSERTION_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(datasetAssertionInfoFor(DATASET_URN));

    try (MockedStatic<AuthorizationUtils> authorizationUtils =
        Mockito.mockStatic(AuthorizationUtils.class)) {
      authorizationUtils
          .when(() -> AuthorizationUtils.isAuthorized(eq(context), any(), any(), any()))
          .thenReturn(false, false, true);

      OwnerUtils.validateAuthorizedToUpdateOwners(
          context, Urn.createFromString(ASSERTION_URN), mockClient, mockService);
    }
  }

  @Test(expectedExceptions = AuthorizationException.class)
  public void testValidateAuthorizedToUpdateOwnersAssertionDelegationDenied() throws Exception {
    QueryContext context = getMockDenyContext();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(ASSERTION_URN)),
                eq(Constants.ASSERTION_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(datasetAssertionInfoFor(DATASET_URN));

    OwnerUtils.validateAuthorizedToUpdateOwners(
        context, Urn.createFromString(ASSERTION_URN), mockClient, mockService);
  }

  @Test
  public void testValidateAuthorizedToUpdateOwnersAssertionAllowedDirectly() throws Exception {
    QueryContext context = getMockAllowContext();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    OwnerUtils.validateAuthorizedToUpdateOwners(
        context, Urn.createFromString(ASSERTION_URN), mockClient, mockService);
  }

  @Test(expectedExceptions = AuthorizationException.class)
  public void testValidateAuthorizedToUpdateOwnersAssertionMissingInfo() throws Exception {
    QueryContext context = getMockDenyContext();
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(Urn.createFromString(ASSERTION_URN)),
                eq(Constants.ASSERTION_INFO_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);

    OwnerUtils.validateAuthorizedToUpdateOwners(
        context, Urn.createFromString(ASSERTION_URN), mockClient, mockService);
  }
}
