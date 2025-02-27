package com.linkedin.metadata.kafka.hook.test;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.MetadataTestClient;
import com.linkedin.test.TestResults;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetadataTestHookTest {
  private MetadataTestClient _mockTestClient;
  private MetadataTestHook _metadataTestHook;

  @BeforeMethod
  public void setupTest() throws Exception {
    Authentication mockAuthentication = Mockito.mock(Authentication.class);
    _mockTestClient = initTestClientMock();
    _metadataTestHook =
        new MetadataTestHook(
            ENTITY_REGISTRY, _mockTestClient, mockAuthentication, true, 1, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testInvokeUnsupportedEntityType() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(INGESTION_SOURCE_ENTITY_NAME);
    event.setAspectName(INGESTION_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("0 1 1 * *")
            .setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId(DEFAULT_EXECUTOR_ID)
            .setRecipe("{ type }")
            .setVersion("0.8.18"));
    event.setAspect(GenericRecordUtils.serializeAspect(newInfo));
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));
    _metadataTestHook.invoke(event);
    Thread.sleep(5); // Wait for urn observer hook. (5ms)
    _metadataTestHook.cleanUpCache();
    Thread.sleep(500); // Wait for async thread to execute
    // Ensure that we do not attempt to run tests
    Mockito.verify(_mockTestClient, Mockito.times(0))
        .evaluate(
            Mockito.any(Urn.class),
            Mockito.anyList(),
            Mockito.anyBoolean(),
            Mockito.any(Authentication.class));
  }

  @Test
  public void testInvokeSupportedEntityType() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(UPSTREAM_LINEAGE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final UpstreamLineage lineage = new UpstreamLineage();
    lineage.setUpstreams(new UpstreamArray());
    event.setAspect(GenericRecordUtils.serializeAspect(lineage));
    event.setEntityUrn(Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"));
    _metadataTestHook.invoke(event);
    Thread.sleep(5); // Wait for urn observer hook. (5ms)
    _metadataTestHook.cleanUpCache();
    Thread.sleep(500); // Wait for async thread to execute
    // Ensure that we do not attempt to run tests
    Mockito.verify(_mockTestClient, Mockito.times(1))
        .evaluate(
            Mockito.eq(event.getEntityUrn()),
            Mockito.eq(null),
            Mockito.eq(true),
            Mockito.any(Authentication.class));
  }

  @Test
  public void testInvokePreventReprocess() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(UPSTREAM_LINEAGE_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    systemMetadata.setProperties(properties);
    event.setSystemMetadata(systemMetadata);
    properties.put(APP_SOURCE, METADATA_TESTS_SOURCE);
    final DataHubIngestionSourceInfo newInfo = new DataHubIngestionSourceInfo();
    newInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setInterval("0 1 1 * *")
            .setTimezone("UTC")); // Run every monday
    newInfo.setType("redshift");
    newInfo.setName("My Redshift Source");
    newInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId(DEFAULT_EXECUTOR_ID)
            .setRecipe("{ type }")
            .setVersion("0.8.18"));
    event.setAspect(GenericRecordUtils.serializeAspect(newInfo));
    event.setEntityUrn(Urn.createFromString("urn:li:dataHubIngestionSourceUrn:0"));
    _metadataTestHook.invoke(event);
    Thread.sleep(5); // Wait for urn observer hook. (5ms)
    _metadataTestHook.cleanUpCache();
    Thread.sleep(500); // Wait for async thread to execute
    // Ensure that we do not attempt to run tests
    Mockito.verify(_mockTestClient, Mockito.times(0))
        .evaluate(
            Mockito.any(Urn.class),
            Mockito.anyList(),
            Mockito.anyBoolean(),
            Mockito.any(Authentication.class));
  }

  private MetadataTestClient initTestClientMock() throws Exception {
    MetadataTestClient client = Mockito.mock(MetadataTestClient.class);
    Mockito.when(
            client.evaluate(
                Mockito.eq(
                    Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)")),
                Mockito.eq(null),
                Mockito.eq(true),
                Mockito.any()))
        .thenReturn(new TestResults());
    return client;
  }
}
