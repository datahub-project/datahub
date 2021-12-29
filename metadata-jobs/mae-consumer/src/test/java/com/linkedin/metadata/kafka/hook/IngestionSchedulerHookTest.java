package com.linkedin.metadata.kafka.hook;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;

import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;


public class IngestionSchedulerHookTest {
  private IngestionSchedulerHook _ingestionSchedulerHook;

  @BeforeMethod
  public void setupTest() throws Exception {

    // Init mocks.
    final Urn ingestionSourceUrn1 = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");
    final DataHubIngestionSourceInfo info1 = new DataHubIngestionSourceInfo();
    info1.setSchedule(new DataHubIngestionSourceSchedule().setInterval("0 0 * * 1").setTimezone("America/Los Angeles")); // Run every monday
    info1.setType("mysql");
    info1.setName("My Test Source");
    info1.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );

    final EnvelopedAspect envelopedAspect1 = new EnvelopedAspect();
    envelopedAspect1.setName(INGESTION_INFO_ASPECT_NAME);
    envelopedAspect1.setValue(new Aspect(info1.data()));

    final EnvelopedAspectMap map1 = new EnvelopedAspectMap();
    map1.put(INGESTION_INFO_ASPECT_NAME, envelopedAspect1);

    final EntityResponse entityResponse1 = Mockito.mock(EntityResponse.class);
    Mockito.when(entityResponse1.getUrn()).thenReturn(ingestionSourceUrn1);
    Mockito.when(entityResponse1.getEntityName()).thenReturn(INGESTION_SOURCE_ENTITY_NAME);
    Mockito.when(entityResponse1.getAspects()).thenReturn(map1);

    final Urn ingestionSourceUrn2 = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:1");
    final DataHubIngestionSourceInfo info2 = new DataHubIngestionSourceInfo();
    info2.setSchedule(new DataHubIngestionSourceSchedule().setInterval("0 0 * * 1 BLUE GREEN").setTimezone("America/Los Angeles")); // Run every monday
    info2.setType("invalid");
    info2.setName("My Invalid Source");
    info2.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );

    final EnvelopedAspect envelopedAspect2 = new EnvelopedAspect();
    envelopedAspect2.setName(INGESTION_INFO_ASPECT_NAME);
    envelopedAspect2.setValue(new Aspect(info2.data()));

    final EnvelopedAspectMap map2 = new EnvelopedAspectMap();
    map2.put(INGESTION_INFO_ASPECT_NAME, envelopedAspect2);

    final EntityResponse entityResponse2 = Mockito.mock(EntityResponse.class);
    Mockito.when(entityResponse2.getUrn()).thenReturn(ingestionSourceUrn2);
    Mockito.when(entityResponse2.getEntityName()).thenReturn(INGESTION_SOURCE_ENTITY_NAME);
    Mockito.when(entityResponse2.getAspects()).thenReturn(map2);

    EntityRegistry registry = new ConfigEntityRegistry(
        IngestionSchedulerHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));

    RestliEntityClient mockClient = Mockito.mock(RestliEntityClient.class);

    // Set up mocks for ingestion source batch fetching
    Mockito.when(mockClient.list(
        Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
        Mockito.eq(Collections.emptyMap()),
        Mockito.eq(0),
        Mockito.eq(30),
        Mockito.any()
    )).thenReturn(new ListResult().setCount(30).setTotal(2).setStart(0).setEntities(
        new UrnArray(ingestionSourceUrn1, ingestionSourceUrn2)));
    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
        Mockito.eq(ImmutableSet.of(ingestionSourceUrn1, ingestionSourceUrn2)),
        Mockito.eq(ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME)),
        Mockito.any()
    )).thenReturn(ImmutableMap.of(
        ingestionSourceUrn1, entityResponse1,
        ingestionSourceUrn2, entityResponse2));

    ConfigurationProvider configurationProvider = new ConfigurationProvider();
    configurationProvider.setIngestion(new IngestionConfiguration(true, "0.1.1"));

    _ingestionSchedulerHook = new IngestionSchedulerHook(
        registry,
        Mockito.mock(Authentication.class),
        mockClient,
        configurationProvider,
        1,
        1200);
    Thread.sleep(2000); // Sleep so the runnable can execute. (not ideal)
  }

  @Test
  public void testInvokeUpdateExistingSchedule() throws Exception {
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);

    Urn ingestionSourceUrn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");

    Future<?> beforeFuture = _ingestionSchedulerHook._nextIngestionSourceExecutionCache.get(ingestionSourceUrn);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));

    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(new DataHubIngestionSourceSchedule().setInterval("0 1 1 * *").setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );
    event.setAspect(GenericAspectUtils.serializeAspect(newInfo));

    // Assert that the new source has been scheduled successfully.
    _ingestionSchedulerHook.invoke(event);
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);
    Future<?> newFuture = _ingestionSchedulerHook._nextIngestionSourceExecutionCache.get(ingestionSourceUrn);

    // Ensure that there is an overwritten future.
    assertNotSame(beforeFuture, newFuture);
  }

  @Test
  public void testInvokeNewSchedule() throws Exception {
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:2"));

    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(new DataHubIngestionSourceSchedule().setInterval("0 1 1 * *").setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );
    event.setAspect(GenericAspectUtils.serializeAspect(newInfo));

    // Assert that the new source has been scheduled successfully.
    _ingestionSchedulerHook.invoke(event);
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 2);
  }

  @Test
  public void testInvokeInvalidSchedule() throws Exception {
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:2"));

    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    // Invalid schedule set.
    newInfo.setSchedule(new DataHubIngestionSourceSchedule().setInterval("NOT A SCHEDULE").setTimezone("America/Los Angeles")); // Run every monday
    newInfo.setType("snowflake");
    newInfo.setName("My Snowflake Source");
    newInfo.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );
    event.setAspect(GenericAspectUtils.serializeAspect(newInfo));

    // Assert that no changes have been made to next execution cache.
    _ingestionSchedulerHook.invoke(event);
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);
  }

  @Test
  public void testInvokeMissingSchedule() throws Exception {
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));

    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    // No schedule set.
    newInfo.setType("mysql");
    newInfo.setName("My Test Source");
    newInfo.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );
    event.setAspect(GenericAspectUtils.serializeAspect(newInfo));

    // Assert that the schedule has been removed.
    _ingestionSchedulerHook.invoke(event);
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 0);
  }

  @Test
  public void testInvokeDelete() throws Exception {
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);

    // Attempt to delete an unscheduled urn
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.DELETE);
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSource:not-scheduled"));
    _ingestionSchedulerHook.invoke(event);
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);

    // Attempt to delete the scheduled urn
    MetadataChangeLog event2 = new MetadataChangeLog();
    event2.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event2.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event2.setChangeType(ChangeType.DELETE);
    event2.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));
    _ingestionSchedulerHook.invoke(event2);
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 0);
  }

  @Test
  public void testInvokeWrongAspect() throws Exception {
    int nextExecCacheSizeBefore = _ingestionSchedulerHook._nextIngestionSourceExecutionCache.size();
    MetadataChangeLog event = new MetadataChangeLog();
    event.setAspectName(SECRET_VALUE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    _ingestionSchedulerHook.invoke(event);
    int nextExecCacheSizeAfter = _ingestionSchedulerHook._nextIngestionSourceExecutionCache.size();

    // Assert that nothing changed.
    assertEquals(nextExecCacheSizeAfter, nextExecCacheSizeBefore);
  }

  @Test
  public void testSchedule() throws Exception {
    assertEquals(_ingestionSchedulerHook._nextIngestionSourceExecutionCache.size(), 1);

    Urn ingestionSourceUrn = Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0");
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(ingestionSourceUrn);

    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(new DataHubIngestionSourceSchedule().setInterval("* * * * *").setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );
    event.setAspect(GenericAspectUtils.serializeAspect(newInfo));

    _ingestionSchedulerHook.invoke(event);

    ScheduledFuture<?> future = _ingestionSchedulerHook._nextIngestionSourceExecutionCache.get(ingestionSourceUrn);
    assertTrue(future.getDelay(TimeUnit.SECONDS) < 60); // Next execution must always be less than a minute away.
  }
}


