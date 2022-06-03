package com.linkedin.metadata.kafka.hook.ingestion;

import com.datahub.metadata.ingestion.IngestionScheduler;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;

import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class IngestionSchedulerHookTest {
  private IngestionSchedulerHook _ingestionSchedulerHook;

  @BeforeMethod
  public void setupTest() {
    EntityRegistry registry = new ConfigEntityRegistry(
        IngestionSchedulerHookTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
    IngestionScheduler mockScheduler = Mockito.mock(IngestionScheduler.class);
    _ingestionSchedulerHook = new IngestionSchedulerHook(registry, mockScheduler);
  }

  @Test
  public void testInvoke() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(new DataHubIngestionSourceSchedule().setInterval("0 1 1 * *").setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
        .setRecipe("{ type }")
        .setVersion("0.8.18")
    );
    event.setAspect(GenericRecordUtils.serializeAspect(newInfo));
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));
    _ingestionSchedulerHook.invoke(event);
    Mockito.verify(_ingestionSchedulerHook.scheduler(), Mockito.times(1)).scheduleNextIngestionSourceExecution(Mockito.any(), Mockito.any());
  }

  @Test
  public void testInvokeDeleteKeyAspect() throws Exception {
    MetadataChangeLog event2 = new MetadataChangeLog();
    event2.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event2.setAspectName(INGESTION_SOURCE_KEY_ASPECT_NAME);
    event2.setChangeType(ChangeType.DELETE);
    event2.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));
    _ingestionSchedulerHook.invoke(event2);
    Mockito.verify(_ingestionSchedulerHook.scheduler(), Mockito.times(1)).unscheduleNextIngestionSourceExecution(Mockito.any());
  }

  @Test
  public void testInvokeDeleteInfoAspect() throws Exception {
    MetadataChangeLog event2 = new MetadataChangeLog();
    event2.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event2.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event2.setChangeType(ChangeType.DELETE);
    event2.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));
    _ingestionSchedulerHook.invoke(event2);
    Mockito.verify(_ingestionSchedulerHook.scheduler(), Mockito.times(1)).unscheduleNextIngestionSourceExecution(Mockito.any());
  }

  @Test
  public void testInvokeWrongAspect() {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setAspectName(SECRET_VALUE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    _ingestionSchedulerHook.invoke(event);
    Mockito.verifyZeroInteractions(_ingestionSchedulerHook.scheduler());
  }
}


