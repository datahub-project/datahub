package com.linkedin.datahub.graphql.types.anomaly;

import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Anomaly;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.AnomalyKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AnomalyTypeTest {

  private static final Urn TEST_ANOMALY_URN;
  private static final Urn TEST_ANOMALY_URN_2;
  private static final Urn TEST_USER_URN;
  private static final Urn TEST_DATASET_URN;

  static {
    TEST_ANOMALY_URN = UrnUtils.getUrn("urn:li:anomaly:test");
    TEST_ANOMALY_URN_2 = UrnUtils.getUrn("urn:li:anomaly:guid-2");
    TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
    TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(test,test,test)");
  }

  private static final AnomalyKey TEST_ANOMALY_KEY = new AnomalyKey().setId("guid-1");

  private static final AnomalyInfo TEST_ANOMALY_INFO =
      new AnomalyInfo()
          .setType(com.linkedin.anomaly.AnomalyType.DATASET_COLUMN)
          .setEntity(TEST_DATASET_URN)
          .setStatus(
              new com.linkedin.anomaly.AnomalyStatus()
                  .setState(com.linkedin.anomaly.AnomalyState.ACTIVE)
                  .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN)))
          .setReview(
              new com.linkedin.anomaly.AnomalyReview()
                  .setState(com.linkedin.anomaly.AnomalyReviewState.PENDING)
                  .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN)))
          .setSource(
              new com.linkedin.anomaly.AnomalySource()
                  .setType(com.linkedin.anomaly.AnomalySourceType.INFERRED_ASSERTION_FAILURE))
          .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn anomalyUrn1 = TEST_ANOMALY_URN;
    Urn anomalyUrn2 = TEST_ANOMALY_URN_2;

    Map<String, EnvelopedAspect> anomaly1Aspects = new HashMap<>();
    anomaly1Aspects.put(
        Constants.ANOMALY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_ANOMALY_KEY.data())));
    anomaly1Aspects.put(
        Constants.ANOMALY_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_ANOMALY_INFO.data())));
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                Mockito.eq(Constants.ANOMALY_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(anomalyUrn1, anomalyUrn2))),
                Mockito.eq(
                    com.linkedin.datahub.graphql.types.anomaly.AnomalyType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                anomalyUrn1,
                new EntityResponse()
                    .setEntityName(Constants.ANOMALY_ENTITY_NAME)
                    .setUrn(anomalyUrn1)
                    .setAspects(new EnvelopedAspectMap(anomaly1Aspects))));

    com.linkedin.datahub.graphql.types.anomaly.AnomalyType type =
        new com.linkedin.datahub.graphql.types.anomaly.AnomalyType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    List<DataFetcherResult<Anomaly>> result =
        type.batchLoad(
            ImmutableList.of(TEST_ANOMALY_URN.toString(), TEST_ANOMALY_URN_2.toString()),
            mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class),
            Mockito.eq(Constants.ANOMALY_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(anomalyUrn1, anomalyUrn2)),
            Mockito.eq(com.linkedin.datahub.graphql.types.anomaly.AnomalyType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    Anomaly anomaly = result.get(0).getData();
    assertEquals(anomaly.getType(), EntityType.ANOMALY);
    assertEquals(anomaly.getAnomalyType().toString(), TEST_ANOMALY_INFO.getType().toString());
    assertEquals(anomaly.getCreated().getTime(), TEST_ANOMALY_INFO.getCreated().getTime());
    assertEquals(
        anomaly.getCreated().getActor(), TEST_ANOMALY_INFO.getCreated().getActor().toString());
    assertEquals(anomaly.getEntity().getUrn(), TEST_ANOMALY_INFO.getEntity().toString());
    assertEquals(
        anomaly.getStatus().getState().toString(),
        TEST_ANOMALY_INFO.getStatus().getState().toString());
    assertEquals(
        anomaly.getStatus().getLastUpdated().getTime(),
        TEST_ANOMALY_INFO.getStatus().getLastUpdated().getTime());
    assertEquals(
        anomaly.getStatus().getLastUpdated().getActor(),
        TEST_ANOMALY_INFO.getStatus().getLastUpdated().getActor().toString());
    assertEquals(
        anomaly.getSource().getType().toString(),
        TEST_ANOMALY_INFO.getSource().getType().toString());
    assertEquals(
        anomaly.getReview().getState().toString(),
        TEST_ANOMALY_INFO.getReview().getState().toString());
    assertEquals(
        anomaly.getReview().getLastUpdated().getTime(),
        TEST_ANOMALY_INFO.getReview().getLastUpdated().getTime());
    assertEquals(
        anomaly.getReview().getLastUpdated().getActor(),
        TEST_ANOMALY_INFO.getReview().getLastUpdated().getActor().toString());

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(
            nullable(OperationContext.class),
            Mockito.anyString(),
            Mockito.anySet(),
            Mockito.anySet());
    com.linkedin.datahub.graphql.types.anomaly.AnomalyType type =
        new com.linkedin.datahub.graphql.types.anomaly.AnomalyType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () ->
            type.batchLoad(
                ImmutableList.of(TEST_ANOMALY_URN.toString(), TEST_ANOMALY_URN_2.toString()),
                context));
  }
}
