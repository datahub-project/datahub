package com.linkedin.datahub.graphql.resolvers.share;

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
import com.linkedin.datahub.graphql.generated.ShareEntityInput;
import com.linkedin.datahub.graphql.generated.ShareEntityResult;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.service.ShareService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.model.ExecuteShareResult;
import java.util.ArrayList;
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
  private static final ShareEntityInput TEST_INPUT =
      new ShareEntityInput(
          TEST_DATASET_URN.toString(), TEST_CONNECTION_URN.toString(), new ArrayList<>());

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

    // calls shareEntity on integrations service expected numner of times
    Mockito.verify(mockIntegrationsService, Mockito.times(1))
        .shareEntity(Mockito.eq(TEST_CONNECTION_URN), Mockito.eq(TEST_DATASET_URN));
    // fetches the new aspect on a success
    Mockito.verify(mockService, Mockito.times(1))
        .getShareOrDefault(Mockito.eq(TEST_DATASET_URN), Mockito.any(Authentication.class));
    // does not upsert a failed result on success
    Mockito.verify(mockService, Mockito.times(0))
        .upsertShareResult(
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE),
            Mockito.any(Authentication.class));
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
            null,
            ImmutableList.of(TEST_CONNECTION_URN.toString(), TEST_CONNECTION_URN_2.toString()));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));

    ShareEntityResult shareEntityResult = resolver.get(mockEnv).get();
    assertNotNull(shareEntityResult);

    // calls shareEntity on integrations service expected numner of times
    Mockito.verify(mockIntegrationsService, Mockito.times(1))
        .shareEntity(Mockito.eq(TEST_CONNECTION_URN), Mockito.eq(TEST_DATASET_URN));
    Mockito.verify(mockIntegrationsService, Mockito.times(1))
        .shareEntity(Mockito.eq(TEST_CONNECTION_URN_2), Mockito.eq(TEST_DATASET_URN));
    Mockito.verify(mockService, Mockito.times(1))
        .getShareOrDefault(Mockito.eq(TEST_DATASET_URN), Mockito.any(Authentication.class));
    // does not upsert a failed result on success
    Mockito.verify(mockService, Mockito.times(0))
        .upsertShareResult(
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE),
            Mockito.any(Authentication.class));
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

    Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // never fetches data when no permissions
    Mockito.verify(mockService, Mockito.times(0))
        .getShareOrDefault(Mockito.eq(TEST_DATASET_URN), Mockito.any(Authentication.class));
    // never hits mock service because auth check fails first
    Mockito.verify(mockService, Mockito.times(0))
        .upsertShareResult(
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE),
            Mockito.any(Authentication.class));
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
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE),
            Mockito.any(Authentication.class));
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
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_CONNECTION_URN),
            Mockito.eq(ShareResultState.FAILURE),
            Mockito.any(Authentication.class));
  }

  private ShareService initMockService(boolean shouldSucceed) {
    ShareService service = Mockito.mock(ShareService.class);
    if (shouldSucceed) {
      Share shareAspect = new Share();
      shareAspect.setLastShareResults(new ShareResultArray());
      Mockito.when(
              service.upsertShareResult(
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(TEST_CONNECTION_URN),
                  Mockito.eq(ShareResultState.FAILURE),
                  Mockito.any(Authentication.class)))
          .thenReturn(shareAspect);
      Mockito.when(
              service.getShareOrDefault(
                  Mockito.eq(TEST_DATASET_URN), Mockito.any(Authentication.class)))
          .thenReturn(shareAspect);
    } else {
      Mockito.when(
              service.upsertShareResult(
                  Mockito.eq(TEST_DATASET_URN),
                  Mockito.eq(TEST_CONNECTION_URN),
                  Mockito.eq(ShareResultState.FAILURE),
                  Mockito.any(Authentication.class)))
          .thenThrow(RuntimeException.class);
      Mockito.when(
              service.getShareOrDefault(
                  Mockito.eq(TEST_DATASET_URN), Mockito.any(Authentication.class)))
          .thenThrow(RuntimeException.class);
    }

    return service;
  }

  private IntegrationsService initMockIntegrationsService(boolean shouldSucceed) {
    IntegrationsService service = Mockito.mock(IntegrationsService.class);
    if (shouldSucceed) {
      Mockito.when(
              service.shareEntity(Mockito.eq(TEST_CONNECTION_URN), Mockito.eq(TEST_DATASET_URN)))
          .thenReturn(new ExecuteShareResult());
    } else {
      Mockito.when(
              service.shareEntity(Mockito.eq(TEST_CONNECTION_URN), Mockito.eq(TEST_DATASET_URN)))
          .thenReturn(null);
    }

    return service;
  }
}
