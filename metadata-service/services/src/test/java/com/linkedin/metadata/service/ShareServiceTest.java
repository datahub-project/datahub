package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Share;
import com.linkedin.common.ShareResult;
import com.linkedin.common.ShareResultArray;
import com.linkedin.common.ShareResultState;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ShareServiceTest {

  private static final Urn TEST_CONNECTION_URN =
      UrnUtils.getUrn("urn:li:dataHubConnection:afe3b0d3-11bb-46e8-9c0b-d7a2aa296a4a");
  private static final Urn TEST_CONNECTION_URN_2 =
      UrnUtils.getUrn("urn:li:dataHubConnection:afe3b0d3-11bb-46e8-9c0b-d7a2aa296a4b");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private OperationContext opContext;

  @BeforeTest
  private void setup() {
    opContext = TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);
  }

  @Test
  private void testUpsertShareResultSuccess() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    final ShareService service =
        new ShareService(mockClient, mock(OpenApiClient.class), objectMapper);

    final Share shareAspect =
        service.upsertShareResult(
            opContext, TEST_DATASET_URN, TEST_CONNECTION_URN, ShareResultState.SUCCESS);

    Assert.assertEquals(shareAspect.getLastShareResults().size(), 1);
    ShareResult shareResult = shareAspect.getLastShareResults().get(0);
    Assert.assertEquals(shareResult.getDestination(), TEST_CONNECTION_URN);
    Assert.assertEquals(shareResult.getStatus(), ShareResultState.SUCCESS);
    Assert.assertEquals(shareResult.getCreated().getActor(), TEST_USER_URN);
    Assert.assertNotNull(shareResult.getCreated().getTime());
    Assert.assertEquals(shareResult.getLastSuccess().getActor(), TEST_USER_URN);
    Assert.assertNotNull(shareResult.getLastSuccess().getTime());
    Assert.assertNull(shareResult.getImplicitShareEntity());

    // Ingests new aspect
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  private void testUpsertSuccessfulShareResultAfterSuccess() throws Exception {
    Long createdTime = System.currentTimeMillis();
    EntityResponse response = createShareAspectResponse(createdTime, TEST_CONNECTION_URN);

    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(TEST_DATASET_URN.getEntityType()),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of(Constants.SHARE_ASPECT_NAME))))
        .thenReturn(response);
    final ShareService service =
        new ShareService(mockClient, mock(OpenApiClient.class), objectMapper);

    final Share shareAspect =
        service.upsertShareResult(
            opContext, TEST_DATASET_URN, TEST_CONNECTION_URN, ShareResultState.SUCCESS);

    // ensure that we replace the old share result for the same connection, updating lastSuccess
    Assert.assertEquals(shareAspect.getLastShareResults().size(), 1);
    ShareResult shareResult = shareAspect.getLastShareResults().get(0);
    Assert.assertEquals(shareResult.getDestination(), TEST_CONNECTION_URN);
    Assert.assertEquals(shareResult.getStatus(), ShareResultState.SUCCESS);
    Assert.assertEquals(shareResult.getCreated().getActor(), TEST_USER_URN);
    Assert.assertEquals(shareResult.getCreated().getTime(), createdTime);
    Assert.assertEquals(shareResult.getLastSuccess().getActor(), TEST_USER_URN);
    // ensure the last success time is new
    Assert.assertNotEquals(shareResult.getLastSuccess().getTime(), createdTime);
    Assert.assertNotNull(shareResult.getLastSuccess().getTime());
    Assert.assertNull(shareResult.getImplicitShareEntity());

    // Ingests new aspect
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  private void testUpsertFailedShareResultAfterSuccess() throws Exception {
    Long createdTime = System.currentTimeMillis();
    EntityResponse response = createShareAspectResponse(createdTime, TEST_CONNECTION_URN);

    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(TEST_DATASET_URN.getEntityType()),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of(Constants.SHARE_ASPECT_NAME))))
        .thenReturn(response);
    final ShareService service =
        new ShareService(mockClient, mock(OpenApiClient.class), objectMapper);

    final Share shareAspect =
        service.upsertShareResult(
            opContext, TEST_DATASET_URN, TEST_CONNECTION_URN, ShareResultState.FAILURE);

    // ensure that we replace the old share result for the same connection, updating lastSuccess
    Assert.assertEquals(shareAspect.getLastShareResults().size(), 1);
    ShareResult shareResult = shareAspect.getLastShareResults().get(0);
    Assert.assertEquals(shareResult.getDestination(), TEST_CONNECTION_URN);
    Assert.assertEquals(shareResult.getStatus(), ShareResultState.FAILURE);
    Assert.assertEquals(shareResult.getCreated().getActor(), TEST_USER_URN);
    Assert.assertEquals(shareResult.getCreated().getTime(), createdTime);
    Assert.assertEquals(shareResult.getLastSuccess().getActor(), TEST_USER_URN);
    // ensure the last success time has not changed since the most recent share was a failure
    Assert.assertEquals(shareResult.getLastSuccess().getTime(), createdTime);
    Assert.assertNull(shareResult.getImplicitShareEntity());

    // Ingests new aspect
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  private void testUpsertShareResultNewInstance() throws Exception {
    Long createdTime = System.currentTimeMillis();
    // populate with a share result with instance 1
    EntityResponse response = createShareAspectResponse(createdTime, TEST_CONNECTION_URN);

    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(TEST_DATASET_URN.getEntityType()),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of(Constants.SHARE_ASPECT_NAME))))
        .thenReturn(response);
    final ShareService service =
        new ShareService(mockClient, mock(OpenApiClient.class), objectMapper);

    // create new share with instance 2
    final Share shareAspect =
        service.upsertShareResult(
            opContext, TEST_DATASET_URN, TEST_CONNECTION_URN_2, ShareResultState.FAILURE);

    // ensure that we create a new result entry for the new instance we share with
    Assert.assertEquals(shareAspect.getLastShareResults().size(), 2);
    ShareResult shareResult = shareAspect.getLastShareResults().get(1);
    Assert.assertEquals(shareResult.getDestination(), TEST_CONNECTION_URN_2);
    Assert.assertEquals(shareResult.getStatus(), ShareResultState.FAILURE);
    Assert.assertEquals(shareResult.getCreated().getActor(), TEST_USER_URN);
    Assert.assertNotNull(shareResult.getCreated().getTime());
    Assert.assertNull(shareResult.getLastSuccess());
    Assert.assertNull(shareResult.getImplicitShareEntity());

    // Ingests new aspect
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  private static EntityResponse createShareAspectResponse(Long time, Urn connectionUrn)
      throws Exception {
    ShareResult result = new ShareResult();
    result.setDestination(connectionUrn);
    result.setStatus(ShareResultState.SUCCESS);
    result.setCreated(new AuditStamp().setActor(TEST_USER_URN).setTime(time));
    result.setLastSuccess(new AuditStamp().setActor(TEST_USER_URN).setTime(time));
    ShareResultArray shareResults = new ShareResultArray();
    shareResults.add(result);
    Share share = new Share();
    share.setLastShareResults(shareResults);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.SHARE_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(share.data())));
    EntityResponse response = new EntityResponse();
    response.setAspects(aspectMap);

    return response;
  }
}
