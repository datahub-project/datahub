package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class NotificationUtilsTest {

  private static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)";
  private static final String ACTOR_URN = "urn:li:corpuser:creating";
  private static final long CREATED_EVENT_TIME = 123L;

  @Test
  public void testIsInitialIngestionRunRealId() {
    MetadataChangeLog log = createMCL("test-real-real-id", false);
    boolean isInitialIngestion = NotificationUtils.isFromInitialIngestionRun(log);
    assertTrue(isInitialIngestion);
  }

  @Test
  public void testIsNotInitialIngestionRunDefaultId() {
    MetadataChangeLog log = createMCL(DEFAULT_RUN_ID, false);
    boolean isInitialIngestion = NotificationUtils.isFromInitialIngestionRun(log);
    assertFalse(isInitialIngestion);
  }

  @Test
  public void testIsNotInitialIngestionRunHasPreviousAspect() {
    MetadataChangeLog log = createMCL("test-real-real-id", true);
    boolean isInitialIngestion = NotificationUtils.isFromInitialIngestionRun(log);
    assertFalse(isInitialIngestion);
  }

  @Test
  public void testIsNotInitialIngestionRunDefaultIdAndPreviousAspect() {
    MetadataChangeLog log = createMCL(DEFAULT_RUN_ID, true);
    boolean isInitialIngestion = NotificationUtils.isFromInitialIngestionRun(log);
    assertFalse(isInitialIngestion);
  }

  private MetadataChangeLog createMCL(String runId, boolean includePreviousAspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.DATASET_ENTITY_NAME);
    event.setAspectName(Constants.OWNERSHIP_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(UrnUtils.getUrn(DATASET_URN));
    if (includePreviousAspect) {
      event.setPreviousAspectValue(new GenericAspect());
    }

    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setLastObserved(CREATED_EVENT_TIME);
    if (runId != null) {
      systemMetadata.setRunId(runId);
    }
    event.setSystemMetadata(systemMetadata);
    event.setCreated(new AuditStamp().setActor(UrnUtils.getUrn(ACTOR_URN)).setTime(CREATED_EVENT_TIME));

    return event;
  }
}
