package com.linkedin.metadata.kafka.hook.subscription;

import static com.linkedin.metadata.Constants.TECHNICAL_OWNER_TYPE_URN;
import static com.linkedin.metadata.kafka.hook.subscription.OwnerSubscriptionHook.DEFAULT_SUBSCRIPTION_CHANGE_TYPES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionType;
import com.linkedin.subscription.SubscriptionTypeArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerSubscriptionHookTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test");
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

  @Mock private SubscriptionService subscriptionService;

  @Mock private SettingsService settingsService;

  private OwnerSubscriptionHook ownerSubscriptionHook;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    this.ownerSubscriptionHook =
        new OwnerSubscriptionHook(subscriptionService, settingsService, true);
  }

  @Test
  public void testInvokeWithEligibleEventAndUserNotificationConfig() {
    // Given
    MetadataChangeLog event = createEligibleEvent();
    CorpUserSettings mockUserSettings = createMockUserSettings();
    when(settingsService.getCorpUserSettings(
            nullable(OperationContext.class), Mockito.eq(TEST_USER_URN)))
        .thenReturn(mockUserSettings);
    when(settingsService.getCorpGroupSettings(nullable(OperationContext.class), any()))
        .thenReturn(null);

    // When
    ownerSubscriptionHook.invoke(event);

    // Then
    verify(settingsService, times(1))
        .getCorpUserSettings(nullable(OperationContext.class), Mockito.eq(TEST_USER_URN));
    verify(subscriptionService, times(1))
        .createSubscription(
            nullable(OperationContext.class),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(new SubscriptionTypeArray(ImmutableList.of(SubscriptionType.ENTITY_CHANGE))),
            Mockito.eq(getDefaultEntityChangeDetails()),
            Mockito.eq(getDefaultNotificationSettings()));
  }

  @Test
  public void testInvokeWithEligibleEventAndGroupNotificationConfig() {
    // Given
    MetadataChangeLog event = createEligibleEvent();
    CorpGroupSettings mockGroupSettings = createMockGroupSettings();
    when(settingsService.getCorpGroupSettings(
            nullable(OperationContext.class), Mockito.eq(TEST_GROUP_URN)))
        .thenReturn(mockGroupSettings);
    when(settingsService.getCorpUserSettings(nullable(OperationContext.class), any()))
        .thenReturn(null);

    // When
    ownerSubscriptionHook.invoke(event);

    // Then
    verify(settingsService, times(1))
        .getCorpGroupSettings(nullable(OperationContext.class), Mockito.eq(TEST_GROUP_URN));
    verify(subscriptionService, times(1))
        .createSubscription(
            nullable(OperationContext.class),
            Mockito.eq(TEST_GROUP_URN),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(new SubscriptionTypeArray(ImmutableList.of(SubscriptionType.ENTITY_CHANGE))),
            Mockito.eq(getDefaultEntityChangeDetails()),
            Mockito.eq(getDefaultNotificationSettings()));
  }

  @Test
  public void testInvokeWithEligibleEventNoNotificationConfig() {
    // Given
    MetadataChangeLog event = createEligibleEvent();
    when(settingsService.getCorpUserSettings(
            any(OperationContext.class), Mockito.eq(TEST_USER_URN)))
        .thenReturn(null);
    when(settingsService.getCorpGroupSettings(
            any(OperationContext.class), Mockito.eq(TEST_GROUP_URN)))
        .thenReturn(null);

    // When
    ownerSubscriptionHook.invoke(event);

    // Then
    verify(settingsService, atLeastOnce())
        .getCorpUserSettings(nullable(OperationContext.class), Mockito.eq(TEST_USER_URN));
    verify(settingsService, atLeastOnce())
        .getCorpGroupSettings(nullable(OperationContext.class), Mockito.eq(TEST_GROUP_URN));

    // Verify user invocation.
    verify(subscriptionService, times(1))
        .createSubscription(
            nullable(OperationContext.class),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(new SubscriptionTypeArray(ImmutableList.of(SubscriptionType.ENTITY_CHANGE))),
            Mockito.eq(getDefaultEntityChangeDetails()),
            Mockito.eq(null));

    // Verify group invocation.
    verify(subscriptionService, times(1))
        .createSubscription(
            nullable(OperationContext.class),
            Mockito.eq(TEST_GROUP_URN),
            Mockito.eq(TEST_ENTITY_URN),
            Mockito.eq(new SubscriptionTypeArray(ImmutableList.of(SubscriptionType.ENTITY_CHANGE))),
            Mockito.eq(getDefaultEntityChangeDetails()),
            Mockito.eq(null));
  }

  @Test
  public void testInvokeWithoutNewOwners() {
    // Given
    MetadataChangeLog event = createEligibleEventWithNoOwnerChanges();

    // When
    ownerSubscriptionHook.invoke(event);

    // Then
    verify(settingsService, times(0)).getCorpUserSettings(any(OperationContext.class), any());
    verify(settingsService, times(0)).getCorpGroupSettings(any(OperationContext.class), any());

    // Verify no invocations
    verify(subscriptionService, times(0))
        .createSubscription(
            any(OperationContext.class), any(Urn.class), any(Urn.class), any(), any(), any());
  }

  @Test
  public void testInvokeWithIneligibleEvents() {
    // Given
    MetadataChangeLog event1 = createIneligibleEventWrongAspect();
    MetadataChangeLog event2 = createIneligibleEventWrongEntity();

    // When
    ownerSubscriptionHook.invoke(event1);
    ownerSubscriptionHook.invoke(event2);

    // Then
    verify(settingsService, times(0)).getCorpUserSettings(any(OperationContext.class), any());
    verify(settingsService, times(0)).getCorpGroupSettings(any(OperationContext.class), any());

    // Verify no invocations
    verify(subscriptionService, times(0))
        .createSubscription(
            any(OperationContext.class), any(Urn.class), any(Urn.class), any(), any(), any());
  }

  // Utility methods to create mock settings
  private CorpUserSettings createMockUserSettings() {
    return new CorpUserSettings()
        .setNotificationSettings(
            new NotificationSettings()
                .setSinkTypes(
                    new NotificationSinkTypeArray(ImmutableList.of(NotificationSinkType.SLACK))));
  }

  private CorpGroupSettings createMockGroupSettings() {
    return new CorpGroupSettings()
        .setNotificationSettings(
            new NotificationSettings()
                .setSinkTypes(
                    new NotificationSinkTypeArray(ImmutableList.of(NotificationSinkType.SLACK))));
  }

  private MetadataChangeLog createEligibleEvent() {
    // Return a MetadataChangeLog instance that matches the hook's criteria for processing
    return buildMetadataChangeLog(
        TEST_ENTITY_URN, Constants.OWNERSHIP_ASPECT_NAME, ChangeType.UPSERT, buildDefaultOwners());
  }

  private MetadataChangeLog createEligibleEventWithNoOwnerChanges() {
    // Return a MetadataChangeLog instance that matches the hook's criteria for processing
    return buildMetadataChangeLog(
        TEST_ENTITY_URN,
        Constants.OWNERSHIP_ASPECT_NAME,
        ChangeType.UPSERT,
        buildDefaultOwners(),
        buildDefaultOwners());
  }

  private MetadataChangeLog createIneligibleEventWrongAspect() {
    // Return a MetadataChangeLog instance that matches the hook's criteria for processing
    return buildMetadataChangeLog(
        TEST_ENTITY_URN,
        Constants.STATUS_ASPECT_NAME,
        ChangeType.UPSERT,
        new Status().setRemoved(true));
  }

  private MetadataChangeLog createIneligibleEventWrongEntity() {
    // Return a MetadataChangeLog instance that matches the hook's criteria for processing
    return buildMetadataChangeLog(
        TEST_GROUP_URN, Constants.OWNERSHIP_ASPECT_NAME, ChangeType.UPSERT, buildDefaultOwners());
  }

  private EntityChangeDetailsArray getDefaultEntityChangeDetails() {
    return new EntityChangeDetailsArray(
        getEntityChangeDetailsFromTypes(DEFAULT_SUBSCRIPTION_CHANGE_TYPES));
  }

  private SubscriptionNotificationConfig getDefaultNotificationSettings() {
    return new SubscriptionNotificationConfig()
        .setNotificationSettings(
            new NotificationSettings()
                .setSlackSettings(new SlackNotificationSettings()) // Empty settings required.
                .setSinkTypes(
                    new NotificationSinkTypeArray(ImmutableList.of(NotificationSinkType.SLACK))));
  }

  private List<EntityChangeDetails> getEntityChangeDetailsFromTypes(
      final Set<EntityChangeType> changeTypes) {
    final List<EntityChangeDetails> entityChangeDetails = new ArrayList<>();
    for (EntityChangeType changeType : changeTypes) {
      entityChangeDetails.add(new EntityChangeDetails().setEntityChangeType(changeType));
    }
    return entityChangeDetails;
  }

  private Ownership buildDefaultOwners() {
    Ownership ownership = new Ownership();
    ownership.setOwners(
        new OwnerArray(
            ImmutableList.of(
                new Owner()
                    .setOwner(TEST_USER_URN)
                    .setType(OwnershipType.TECHNICAL_OWNER)
                    .setTypeUrn(TECHNICAL_OWNER_TYPE_URN),
                new Owner()
                    .setOwner(TEST_GROUP_URN)
                    .setType(OwnershipType.TECHNICAL_OWNER)
                    .setTypeUrn(TECHNICAL_OWNER_TYPE_URN))));
    return ownership;
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) {
    return buildMetadataChangeLog(urn, aspectName, changeType, aspect, null);
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn,
      String aspectName,
      ChangeType changeType,
      RecordTemplate aspect,
      RecordTemplate prevAspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(urn.getEntityType());
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    if (prevAspect != null) {
      event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevAspect));
    }
    return event;
  }
}
