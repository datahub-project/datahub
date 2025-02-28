package com.linkedin.datahub.graphql.types.remoteexecutor;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.executor.RemoteExecutorStatus;
import com.linkedin.metadata.AcrylConstants;
import org.testng.annotations.Test;

public class RemoteExecutorMapperTest {

  @Test
  public void testMap() throws Exception {
    EntityResponse entityResponse = new EntityResponse();
    Urn urn = Urn.createFromString("urn:li:remoteExecutor:test");
    entityResponse.setUrn(urn);

    EnvelopedAspect envelopedExecutorStatus = new EnvelopedAspect();
    RemoteExecutorStatus executorStatus =
        new RemoteExecutorStatus()
            .setExecutorPoolId("executor-1")
            .setExecutorReleaseVersion("1.0.0")
            .setExecutorAddress("localhost:8080")
            .setExecutorHostname("test-host")
            .setExecutorUptime(1000L)
            .setExecutorExpired(false)
            .setExecutorStopped(false)
            .setExecutorEmbedded(true)
            .setExecutorInternal(false)
            .setLogDeliveryEnabled(true)
            .setReportedAt(System.currentTimeMillis());

    envelopedExecutorStatus.setValue(new Aspect(executorStatus.data()));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                AcrylConstants.REMOTE_EXECUTOR_STATUS_ASPECT_NAME, envelopedExecutorStatus)));

    RemoteExecutor executor = RemoteExecutorMapper.map(null, entityResponse);

    assertNotNull(executor);
    assertEquals(executor.getUrn(), "urn:li:remoteExecutor:test");
    assertEquals(executor.getType(), EntityType.REMOTE_EXECUTOR);
    assertEquals(executor.getExecutorPoolId(), executorStatus.getExecutorPoolId());
    assertEquals(executor.getExecutorReleaseVersion(), executorStatus.getExecutorReleaseVersion());
    assertEquals(executor.getExecutorAddress(), executorStatus.getExecutorAddress());
    assertEquals(executor.getExecutorHostname(), executorStatus.getExecutorHostname());
    assertEquals(executor.getExecutorUptime(), (double) executorStatus.getExecutorUptime());
    assertEquals(executor.getExecutorExpired(), executorStatus.isExecutorExpired());
    assertEquals(executor.getExecutorStopped(), executorStatus.isExecutorStopped());
    assertEquals(executor.getExecutorEmbedded(), executorStatus.isExecutorEmbedded());
    assertEquals(executor.getExecutorInternal(), executorStatus.isExecutorInternal());
    assertEquals(executor.getLogDeliveryEnabled(), executorStatus.isLogDeliveryEnabled());
    assertEquals(executor.getReportedAt(), executorStatus.getReportedAt());
  }
}
