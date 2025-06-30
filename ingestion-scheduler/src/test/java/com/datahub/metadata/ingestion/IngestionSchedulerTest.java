package com.datahub.metadata.ingestion;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.query.ListResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionSchedulerTest {
  private IngestionScheduler ingestionScheduler;

  @BeforeMethod
  public void setupTest() throws Exception {

    // Init mocks.
    final Urn ingestionSourceUrn1 = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");
    final DataHubIngestionSourceInfo info1 = new DataHubIngestionSourceInfo();
    info1.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("0 0 * * 1")
            .setTimezone("America/Los Angeles")); // Run every monday
    info1.setType("mysql");
    info1.setName("My Test Source");
    info1.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));

    final EnvelopedAspect envelopedAspect1 = new EnvelopedAspect();
    envelopedAspect1.setName(Constants.INGESTION_INFO_ASPECT_NAME);
    envelopedAspect1.setValue(new Aspect(info1.data()));

    final EnvelopedAspectMap map1 = new EnvelopedAspectMap();
    map1.put(Constants.INGESTION_INFO_ASPECT_NAME, envelopedAspect1);

    final EntityResponse entityResponse1 = Mockito.mock(EntityResponse.class);
    Mockito.when(entityResponse1.getUrn()).thenReturn(ingestionSourceUrn1);
    Mockito.when(entityResponse1.getEntityName())
        .thenReturn(Constants.INGESTION_SOURCE_ENTITY_NAME);
    Mockito.when(entityResponse1.getAspects()).thenReturn(map1);

    final Urn ingestionSourceUrn2 = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:1");
    final DataHubIngestionSourceInfo info2 = new DataHubIngestionSourceInfo();
    info2.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("0 0 * * 1 BLUE GREEN")
            .setTimezone("America/Los Angeles")); // Run every monday
    info2.setType("invalid");
    info2.setName("My Invalid Source");
    info2.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));

    final EnvelopedAspect envelopedAspect2 = new EnvelopedAspect();
    envelopedAspect2.setName(Constants.INGESTION_INFO_ASPECT_NAME);
    envelopedAspect2.setValue(new Aspect(info2.data()));

    final EnvelopedAspectMap map2 = new EnvelopedAspectMap();
    map2.put(Constants.INGESTION_INFO_ASPECT_NAME, envelopedAspect2);

    final EntityResponse entityResponse2 = Mockito.mock(EntityResponse.class);
    Mockito.when(entityResponse2.getUrn()).thenReturn(ingestionSourceUrn2);
    Mockito.when(entityResponse2.getEntityName())
        .thenReturn(Constants.INGESTION_SOURCE_ENTITY_NAME);
    Mockito.when(entityResponse2.getAspects()).thenReturn(map2);

    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = Mockito.mock(IngestionConfiguration.class);
    // Set up mocks for ingestion source batch fetching
    Mockito.when(
            mockClient.list(
                any(),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq(Collections.emptyMap()),
                Mockito.eq(0),
                Mockito.eq(30)))
        .thenReturn(
            new ListResult()
                .setCount(30)
                .setTotal(2)
                .setStart(0)
                .setEntities(new UrnArray(ingestionSourceUrn1, ingestionSourceUrn2)));
    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(ingestionSourceUrn1, ingestionSourceUrn2)),
                Mockito.eq(ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                ingestionSourceUrn1, entityResponse1,
                ingestionSourceUrn2, entityResponse2));

    Mockito.when(ingestionConfiguration.getBatchRefreshCount()).thenReturn(30);

    ingestionScheduler =
        new IngestionScheduler(
            Mockito.mock(OperationContext.class), mockClient, ingestionConfiguration, 1, 1200);
    ingestionScheduler.init();
    Thread.sleep(2000); // Sleep so the runnable can execute. (not ideal)
  }

  @Test
  public void testInvokeUpdateExistingSchedule() throws Exception {
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    Urn ingestionSourceUrn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");
    Future<?> beforeFuture =
        ingestionScheduler.nextIngestionSourceExecutionCache.get(ingestionSourceUrn);

    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("0 1 1 * *")
            .setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));

    // Assert that the new source has been scheduled successfully.
    ingestionScheduler.scheduleNextIngestionSourceExecution(ingestionSourceUrn, newInfo);
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);
    Future<?> newFuture =
        ingestionScheduler.nextIngestionSourceExecutionCache.get(ingestionSourceUrn);

    // Ensure that there is an overwritten future.
    Assert.assertNotSame(beforeFuture, newFuture);
  }

  @Test
  public void testInvokeNewSchedule() throws Exception {
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    final Urn urn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:2");
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("0 1 1 * *")
            .setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));

    // Assert that the new source has been scheduled successfully.
    ingestionScheduler.scheduleNextIngestionSourceExecution(urn, newInfo);
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 2);
  }

  @Test
  public void testInvokeInvalidSchedule() throws Exception {
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    final Urn urn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:2");
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    // Invalid schedule set.
    newInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("NOT A SCHEDULE")
            .setTimezone("America/Los Angeles")); // Run every monday
    newInfo.setType("snowflake");
    newInfo.setName("My Snowflake Source");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));

    // Assert that no changes have been made to next execution cache.
    ingestionScheduler.scheduleNextIngestionSourceExecution(urn, newInfo);
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);
  }

  @Test
  public void testInvokeMissingSchedule() throws Exception {
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    final Urn urn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    // No schedule set.
    newInfo.setType("mysql");
    newInfo.setName("My Test Source");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));

    // Assert that the schedule has been removed.
    ingestionScheduler.scheduleNextIngestionSourceExecution(urn, newInfo);
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 0);
  }

  @Test
  public void testInvokeDelete() throws Exception {
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    // Attempt to delete an unscheduled urn
    final Urn urn1 = Urn.createFromString("urn:li:dataHubIngestionSource:not-scheduled");
    ingestionScheduler.unscheduleNextIngestionSourceExecution(urn1);
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    // Attempt to delete a scheduled urn
    final Urn urn2 = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");
    ingestionScheduler.unscheduleNextIngestionSourceExecution(urn2);
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 0);
  }

  @Test
  public void testSchedule() throws Exception {
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    final Urn urn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("* * * * *")
            .setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));

    ingestionScheduler.scheduleNextIngestionSourceExecution(urn, newInfo);

    ScheduledFuture<?> future = ingestionScheduler.nextIngestionSourceExecutionCache.get(urn);
    Assert.assertTrue(
        future.getDelay(TimeUnit.SECONDS)
            < 60); // Next execution must always be less than a minute away.
  }

  @Test
  public void testUnscheduleAll() throws Exception {
    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 1);

    final Urn urn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:3");
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("* * * * *")
            .setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source 2");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setRecipe("{ type: \"type\" }")
            .setVersion("0.8.18"));
    ingestionScheduler.scheduleNextIngestionSourceExecution(urn, newInfo);

    assertEquals(ingestionScheduler.nextIngestionSourceExecutionCache.size(), 2);

    // Get reference to schedules futures
    ScheduledFuture<?> future = ingestionScheduler.nextIngestionSourceExecutionCache.get(urn);

    // Unschedule all
    ingestionScheduler.unscheduleAll();

    // Ensure that the cache is empty
    Assert.assertTrue(ingestionScheduler.nextIngestionSourceExecutionCache.isEmpty());

    // And that the future is cancelled
    Assert.assertTrue(future.isCancelled());
  }
}
