package com.linkedin.datahub.graphql.resolvers.share;

import static org.mockito.ArgumentMatchers.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Share;
import com.linkedin.common.ShareResultArray;
import com.linkedin.common.ShareResultState;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ShareEntityInput;
import com.linkedin.datahub.graphql.generated.ShareEntityResult;
import com.linkedin.datahub.graphql.generated.ShareLineageDirection;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.ShareService;
import com.linkedin.util.Pair;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.ExecuteShareResult;
import io.datahubproject.integrations.model.LineageDirection;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShareEntityResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_CONNECTION_URN =
      UrnUtils.getUrn("urn:li:dataHubConnection:afe3b0d3-11bb-46e8-9c0b-d7a2aa296a4a");
  private static final Urn TEST_CONNECTION_URN_2 =
      UrnUtils.getUrn("urn:li:dataHubConnection:afe3b0d3-11bb-46e8-9c0b-d7a2aa296a4b123");
  private static final Urn TEST_SHARER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final ShareLineageDirection TEST_SHARE_LINEAGE = ShareLineageDirection.DOWNSTREAM;
  private static final ShareEntityInput TEST_INPUT =
      new ShareEntityInput(
          TEST_DATASET_URN.toString(),
          TEST_SHARE_LINEAGE,
          TEST_CONNECTION_URN.toString(),
          new ArrayList<>());

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    ShareService mockService = initMockService(true);
    IntegrationsService mockIntegrationsService = initMockIntegrationsService(true);
    ShareEntityResolver resolver = new ShareEntityResolver(mockService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = TestUtils.getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    ShareEntityResult shareEntityResult = resolver.get(mockEnv).get();
    assertNotNull(shareEntityResult);

    // calls shareEntity on integrations service expected number of times
    Mockito.verify(mockIntegrationsService, Mockito.times(1))
        .shareEntity(
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_SHARER_URN),
            Mockito.eq(LineageDirection.fromValue(TEST_SHARE_LINEAGE.toString())));
    // fetches the new aspect on a success
    Mockito.verify(mockService, Mockito.times(1))
        .getShareOrDefault(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN));
    // does not upsert a failed result on success
    Mockito.verify(mockService, Mockito.times(0))
        .upsertShareResult(
            any(OperationContext.class),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE));
  }

  @Test
  public void testGetSuccessMultipleConnections() throws Exception {
    // Create resolver
    ShareService mockService = initMockService(true);
    IntegrationsService mockIntegrationsService = initMockIntegrationsService(true);
    ShareEntityResolver resolver = new ShareEntityResolver(mockService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = TestUtils.getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final ShareEntityInput input =
        new ShareEntityInput(
            TEST_DATASET_URN.toString(),
            TEST_SHARE_LINEAGE,
            null,
            ImmutableList.of(TEST_CONNECTION_URN.toString(), TEST_CONNECTION_URN_2.toString()));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    ShareEntityResult shareEntityResult = resolver.get(mockEnv).get();
    assertNotNull(shareEntityResult);

    // calls shareEntity on integrations service expected number of times
    Mockito.verify(mockIntegrationsService, Mockito.times(1))
        .shareEntity(
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_SHARER_URN),
            Mockito.eq(LineageDirection.fromValue(TEST_SHARE_LINEAGE.toString())));
    Mockito.verify(mockIntegrationsService, Mockito.times(1))
        .shareEntity(
            Mockito.eq(TEST_CONNECTION_URN_2),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_SHARER_URN),
            Mockito.eq(LineageDirection.fromValue(TEST_SHARE_LINEAGE.toString())));
    Mockito.verify(mockService, Mockito.times(1))
        .getShareOrDefault(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN));
    // does not upsert a failed result on success
    Mockito.verify(mockService, Mockito.times(0))
        .upsertShareResult(
            any(OperationContext.class),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE));
  }

  @Test
  public void testGetNotAuthorized() throws Exception {
    // Create resolver
    ShareService mockService = initMockService(true);
    IntegrationsService mockIntegrationsService = initMockIntegrationsService(true);
    ShareEntityResolver resolver = new ShareEntityResolver(mockService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = TestUtils.getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    Assert.assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());

    // never fetches data when no permissions
    Mockito.verify(mockService, Mockito.times(0))
        .getShareOrDefault(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN));
    // never hits mock service because auth check fails first
    Mockito.verify(mockService, Mockito.times(0))
        .upsertShareResult(
            any(OperationContext.class),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE));
  }

  @Test
  public void testGetServiceFailure() throws Exception {
    // Create resolver
    ShareService mockService = initMockService(false);
    IntegrationsService mockIntegrationsService = initMockIntegrationsService(false);
    ShareEntityResolver resolver = new ShareEntityResolver(mockService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = TestUtils.getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // when the service fails, still write a failed result to gms
    Mockito.verify(mockService, Mockito.times(1))
        .upsertShareResult(
            any(OperationContext.class),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE));
  }

  @Test
  public void testGetIntegrationsServiceFailure() throws Exception {
    // Create resolver
    ShareService mockService = initMockService(true);
    IntegrationsService mockIntegrationsService = initMockIntegrationsService(false);
    ShareEntityResolver resolver = new ShareEntityResolver(mockService, mockIntegrationsService);

    // Execute resolver
    QueryContext mockContext = TestUtils.getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    ShareEntityResult shareEntityResult = resolver.get(mockEnv).get();
    assertFalse(shareEntityResult.getSucceeded());

    // when the service fails, still write a failed result to gms
    Mockito.verify(mockService, Mockito.times(1))
        .upsertShareResult(
            any(OperationContext.class),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE));
  }

  private ShareService initMockService(boolean shouldSucceed) {
    ShareService service = Mockito.mock(ShareService.class);
    if (shouldSucceed) {
      Share shareAspect = new Share();
      shareAspect.setLastShareResults(new ShareResultArray());
      Mockito.when(
              service.upsertShareResult(
                  any(OperationContext.class),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(TEST_CONNECTION_URN),
                  Mockito.eq(ShareResultState.FAILURE)))
          .thenReturn(shareAspect);
      Mockito.when(
              service.getShareOrDefault(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN)))
          .thenReturn(shareAspect);
    } else {
      Mockito.when(
              service.upsertShareResult(
                  any(OperationContext.class),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(TEST_CONNECTION_URN),
                  Mockito.eq(ShareResultState.FAILURE)))
          .thenThrow(RuntimeException.class);
      Mockito.when(
              service.getShareOrDefault(any(OperationContext.class), Mockito.eq(TEST_DATASET_URN)))
          .thenThrow(RuntimeException.class);
    }

    return service;
  }

  private IntegrationsService initMockIntegrationsService(boolean shouldSucceed) {
    IntegrationsService service = Mockito.mock(IntegrationsService.class);
    if (shouldSucceed) {
      Mockito.when(
              service.shareEntity(
                  Mockito.eq(TEST_CONNECTION_URN),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(TEST_SHARER_URN),
                  Mockito.eq(LineageDirection.fromValue(TEST_SHARE_LINEAGE.toString()))))
          .thenReturn(
              CompletableFuture.completedFuture(
                  new Pair<>(TEST_CONNECTION_URN, new ExecuteShareResult())));
      Mockito.when(
              service.shareEntity(
                  Mockito.eq(TEST_CONNECTION_URN_2),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(TEST_SHARER_URN),
                  Mockito.eq(LineageDirection.fromValue(TEST_SHARE_LINEAGE.toString()))))
          .thenReturn(
              CompletableFuture.completedFuture(
                  new Pair<>(TEST_CONNECTION_URN_2, new ExecuteShareResult())));
    } else {
      Mockito.when(
              service.shareEntity(
                  Mockito.eq(TEST_CONNECTION_URN),
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(TEST_SHARER_URN),
                  Mockito.eq(LineageDirection.fromValue(TEST_SHARE_LINEAGE.toString()))))
          .thenReturn(CompletableFuture.completedFuture(new Pair<>(TEST_CONNECTION_URN, null)));
    }

    return service;
  }
}
