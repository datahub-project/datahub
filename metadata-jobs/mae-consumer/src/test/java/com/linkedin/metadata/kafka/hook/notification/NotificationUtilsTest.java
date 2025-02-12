package com.linkedin.metadata.kafka.hook.notification;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.notification.provider.EntityNameProvider;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NotificationUtilsTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-proj.jaffle_shop.customers,PROD)";
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

  @Test
  public void testIsEligibleForNotificationGenerationIngestionResultEvent() {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
    event.setAspectName(Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(UrnUtils.getUrn("urn:li:dataHubIngestionSource:test"));
    // no previous aspect.

    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setLastObserved(CREATED_EVENT_TIME);
    systemMetadata.setRunId("non-default-run-id");
    event.setSystemMetadata(systemMetadata);
    event.setCreated(
        new AuditStamp().setActor(UrnUtils.getUrn(ACTOR_URN)).setTime(CREATED_EVENT_TIME));

    boolean isEligible = NotificationUtils.isEligibleForNotificationGeneration(event);
    assertTrue(isEligible);
  }

  @Test
  public void testIsEligibleForNotificationGenerationAssertionRunEvent() {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(Constants.ASSERTION_ENTITY_NAME);
    event.setAspectName(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(UrnUtils.getUrn("urn:li:assertion:test"));
    // no previous aspect.

    SystemMetadata systemMetadata = new SystemMetadata();
    systemMetadata.setLastObserved(CREATED_EVENT_TIME);
    systemMetadata.setRunId("non-default-run-id");
    event.setSystemMetadata(systemMetadata);
    event.setCreated(
        new AuditStamp().setActor(UrnUtils.getUrn(ACTOR_URN)).setTime(CREATED_EVENT_TIME));

    boolean isEligible = NotificationUtils.isEligibleForNotificationGeneration(event);
    assertTrue(isEligible);
  }

  @Test
  public void testGenerateEntityPath() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,Test Name,PROD)");
    Assert.assertEquals(
        NotificationUtils.generateEntityPath(urn),
        "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2CTest+Name%2CPROD%29");
  }

  @Test
  public void testGenerateEntityPaths() {
    final Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,Test Name,PROD)");
    final Urn dashboardUrn = UrnUtils.getUrn("urn:li:dashboard:(airflow,test)");
    Assert.assertEquals(
            NotificationUtils.generateEntityPaths(List.of(datasetUrn, dashboardUrn)),
            ImmutableList.of(
              "/dataset/urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Asnowflake%2CTest+Name%2CPROD%29",
              "/dashboard/urn%3Ali%dashboard%3A%28airflow%2Ctest%29"
            ));
  }

  @Test
  public void testGetUniqueHydratedSubscriberRecipientsWithNullValuesShouldIgnoreNulls() {
    EntityNameProvider nameProvider = mock(EntityNameProvider.class);
    Urn actorUrn = UrnUtils.getUrn(ACTOR_URN);
    Mockito.when(nameProvider.getName(any(OperationContext.class), Mockito.eq(actorUrn)))
        .thenReturn("Hydrated Name");
    List<NotificationRecipient> recipients = new ArrayList<>();
    recipients.add(null);
    recipients.add(
        new NotificationRecipient()
            .setId("1")
            .setType(NotificationRecipientType.CUSTOM)
            .setActor(actorUrn));
    recipients.add(null);

    List<NotificationRecipient> uniqueRecipients =
        NotificationUtils.getUniqueHydratedSubscriberRecipients(
            mock(OperationContext.class), recipients, nameProvider);

    Assert.assertEquals(uniqueRecipients.size(), 1);
    Assert.assertEquals(uniqueRecipients.get(0).getDisplayName(), "Hydrated Name");
    Assert.assertEquals(uniqueRecipients.get(0).getId(), "1");
    Assert.assertEquals(uniqueRecipients.get(0).getActor(), actorUrn);
  }

  @Test
  public void testGetUniqueHydratedSubscriberRecipientsWithDuplicatesShouldReturnUnique() {
    EntityNameProvider nameProvider = mock(EntityNameProvider.class);
    Urn actorUrn = UrnUtils.getUrn(ACTOR_URN);
    Mockito.when(nameProvider.getName(any(OperationContext.class), Mockito.eq(actorUrn)))
        .thenReturn("Hydrated Name");
    List<NotificationRecipient> recipients = new ArrayList<>();
    NotificationRecipient recipient =
        new NotificationRecipient()
            .setId("1")
            .setType(NotificationRecipientType.CUSTOM)
            .setActor(actorUrn);

    // Add same recipient twice.
    recipients.add(recipient);
    recipients.add(recipient);

    List<NotificationRecipient> uniqueRecipients =
        NotificationUtils.getUniqueHydratedSubscriberRecipients(
            mock(OperationContext.class), recipients, nameProvider);

    Assert.assertEquals(uniqueRecipients.size(), 1);
    Assert.assertEquals(uniqueRecipients.get(0).getDisplayName(), "Hydrated Name");
    Assert.assertEquals(uniqueRecipients.get(0).getId(), "1");
    Assert.assertEquals(uniqueRecipients.get(0).getActor(), actorUrn);
  }

  @Test
  public void testGetUniqueHydratedSubscriberRecipientsShouldHydrateNames() {
    EntityNameProvider nameProvider = mock(EntityNameProvider.class);
    Urn actorUrn1 = UrnUtils.getUrn(ACTOR_URN);
    Urn actorUrn2 = UrnUtils.getUrn("urn:li:corpuser:actor-2");

    Mockito.when(nameProvider.getName(any(OperationContext.class), Mockito.eq(actorUrn1)))
        .thenReturn("Hydrated Name 1");
    Mockito.when(nameProvider.getName(any(OperationContext.class), Mockito.eq(actorUrn2)))
        .thenReturn("Hydrated Name 2");

    List<NotificationRecipient> recipients = new ArrayList<>();
    NotificationRecipient recipient1 =
        new NotificationRecipient()
            .setId("1")
            .setType(NotificationRecipientType.CUSTOM)
            .setActor(actorUrn1);
    NotificationRecipient recipient2 =
        new NotificationRecipient()
            .setId("2")
            .setType(NotificationRecipientType.CUSTOM)
            .setActor(actorUrn2);

    // Add same recipient twice.
    recipients.add(recipient1);
    recipients.add(recipient2);

    List<NotificationRecipient> uniqueRecipients =
        NotificationUtils.getUniqueHydratedSubscriberRecipients(
            mock(OperationContext.class), recipients, nameProvider);

    Assert.assertEquals(uniqueRecipients.size(), 2);

    List<String> displayNames =
        uniqueRecipients.stream()
            .map(recipient -> recipient.getDisplayName())
            .collect(Collectors.toList());

    Assert.assertEquals(displayNames, List.of("Hydrated Name 1", "Hydrated Name 2"));
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
    event.setCreated(
        new AuditStamp().setActor(UrnUtils.getUrn(ACTOR_URN)).setTime(CREATED_EVENT_TIME));

    return event;
  }
}
