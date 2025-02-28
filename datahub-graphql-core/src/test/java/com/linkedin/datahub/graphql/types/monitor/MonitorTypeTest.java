package com.linkedin.datahub.graphql.types.monitor;

import static org.mockito.ArgumentMatchers.nullable;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Monitor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.AssertionMonitorsConfiguration;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MonitorTypeTest {

  private static final Urn TEST_ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:test");
  private static final String TEST_MONITOR_URN =
      String.format("urn:li:monitor:(%s,guid-1)", TEST_ENTITY_URN);
  private static final MonitorKey TEST_MONITOR_KEY =
      new MonitorKey().setEntity(TEST_ENTITY_URN).setId("guid-1");
  private static final MonitorInfo TEST_MONITOR_INFO =
      new MonitorInfo()
          .setType(MonitorType.ASSERTION)
          .setAssertionMonitor(null, SetMode.IGNORE_NULL)
          .setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE))
          .setCustomProperties(new StringMap());
  private static final String TEST_MONITOR_URN_2 = "urn:li:monitor:guid-2";

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);
    AssertionMonitorsConfiguration assertionMonitorsConfiguration =
        Mockito.mock(AssertionMonitorsConfiguration.class);

    Urn monitorUrn1 = Urn.createFromString(TEST_MONITOR_URN);
    Urn monitorUrn2 = Urn.createFromString(TEST_MONITOR_URN_2);

    Map<String, EnvelopedAspect> monitor1Aspects = new HashMap<>();
    monitor1Aspects.put(
        Constants.MONITOR_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_MONITOR_KEY.data())));
    monitor1Aspects.put(
        Constants.MONITOR_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_MONITOR_INFO.data())));
    Mockito.when(
            client.batchGetV2(
                nullable(OperationContext.class),
                Mockito.eq(Constants.MONITOR_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(monitorUrn1, monitorUrn2))),
                Mockito.eq(
                    com.linkedin.datahub.graphql.types.monitor.MonitorType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                monitorUrn1,
                new EntityResponse()
                    .setEntityName(Constants.MONITOR_ENTITY_NAME)
                    .setUrn(monitorUrn1)
                    .setAspects(new EnvelopedAspectMap(monitor1Aspects))));

    com.linkedin.datahub.graphql.types.monitor.MonitorType type =
        new com.linkedin.datahub.graphql.types.monitor.MonitorType(
            client, assertionMonitorsConfiguration);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    List<DataFetcherResult<Monitor>> result =
        type.batchLoad(ImmutableList.of(TEST_MONITOR_URN, TEST_MONITOR_URN_2), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            nullable(OperationContext.class),
            Mockito.eq(Constants.MONITOR_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(monitorUrn1, monitorUrn2)),
            Mockito.eq(com.linkedin.datahub.graphql.types.monitor.MonitorType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    Monitor monitor = result.get(0).getData();
    assertEquals(monitor.getUrn(), TEST_MONITOR_URN);
    assertEquals(monitor.getType(), EntityType.MONITOR);
    assertEquals(monitor.getEntity().getUrn(), TEST_ENTITY_URN.toString());
    assertEquals(monitor.getInfo().getType().toString(), MonitorType.ASSERTION.toString());
    assertEquals(monitor.getInfo().getAssertionMonitor(), null);

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionMonitorsConfiguration assertionMonitorsConfiguration =
        Mockito.mock(AssertionMonitorsConfiguration.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(
            nullable(OperationContext.class),
            Mockito.anyString(),
            Mockito.anySet(),
            Mockito.anySet());
    com.linkedin.datahub.graphql.types.monitor.MonitorType type =
        new com.linkedin.datahub.graphql.types.monitor.MonitorType(
            mockClient, assertionMonitorsConfiguration);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_MONITOR_URN, TEST_MONITOR_URN_2), context));
  }
}
