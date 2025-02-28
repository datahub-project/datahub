package com.linkedin.metadata.kafka.hook.subscription;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;

import com.datahub.authentication.ActorType;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.EntityChangeDetailsFilter;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.timeseries.CalendarInterval;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscriptionSubResourceDeletionHookTest {

  @Mock private SubscriptionService subscriptionService;
  @Mock private AssertionService assertionService;
  private SubscriptionSubResourceDeletionHook hook;

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpUser:test");
  private static final Urn TEST_USER_URN_2 = UrnUtils.getUrn("urn:li:corpUser:test2");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_ASSERTION_URN_2 = UrnUtils.getUrn("urn:li:assertion:test2");

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");

  private static final Urn TEST_DATASET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name-2,PROD)");

  private static final Urn TEST_SUBSCRIPTION_URN = UrnUtils.getUrn("urn:li:subscription:test1");
  private static final Urn TEST_SUBSCRIPTION_URN_2 = UrnUtils.getUrn("urn:li:subscription:test2");
  private static final Map<Urn, SubscriptionInfo> TEST_SUBSCRIPTION_INFO =
      Map.of(
          TEST_SUBSCRIPTION_URN,
          new SubscriptionInfo()
              .setEntityUrn(TEST_DATASET_URN)
              .setActorUrn(TEST_USER_URN)
              .setActorType(ActorType.USER.name())
              .setEntityChangeTypes(
                  new EntityChangeDetailsArray(
                      new EntityChangeDetails().setEntityChangeType(EntityChangeType.DEPRECATED),
                      new EntityChangeDetails()
                          .setEntityChangeType(EntityChangeType.ASSERTION_ERROR))),
          TEST_SUBSCRIPTION_URN_2,
          new SubscriptionInfo()
              .setEntityUrn(TEST_DATASET_URN)
              .setActorUrn(TEST_USER_URN_2)
              .setActorType(ActorType.USER.name())
              .setEntityChangeTypes(
                  new EntityChangeDetailsArray(
                      new EntityChangeDetails()
                          .setEntityChangeType(EntityChangeType.ASSERTION_PASSED)
                          .setFilter(
                              new EntityChangeDetailsFilter()
                                  .setIncludeAssertions(
                                      new UrnArray(TEST_ASSERTION_URN, TEST_ASSERTION_URN_2))))));

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(assertionService.getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL))
                .setDatasetAssertion(new DatasetAssertionInfo().setDataset(TEST_DATASET_URN)));
    Mockito.when(
            subscriptionService.listEntityAssertionSubscriptions(
                Mockito.any(),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.anyInt()))
        .thenReturn(TEST_SUBSCRIPTION_INFO);
    Mockito.when(
            subscriptionService.listAssertionSubscriptionsWithoutEntityUrn(
                Mockito.any(), Mockito.eq(TEST_ASSERTION_URN), Mockito.anyInt()))
        .thenReturn(TEST_SUBSCRIPTION_INFO);

    hook =
        new SubscriptionSubResourceDeletionHook(
            true, ENTITY_REGISTRY, subscriptionService, assertionService);
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {

    // Case 1: Incorrect aspect
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_INFO_ASPECT_NAME, ChangeType.UPSERT, new AssertionInfo());
    hook.invoke(event);
    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionInfo(Mockito.any(), Mockito.any());

    // Case 2: Run Event
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            new AssertionRunEvent());
    hook.invoke(event);
    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionInfo(Mockito.any(), Mockito.any());

    // Case 2: Delete but not soft delete
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            STATUS_ASPECT_NAME,
            ChangeType.UPDATE,
            new Status().setRemoved(false));
    hook.invoke(event);
    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionInfo(Mockito.any(), Mockito.any());
  }

  @Test()
  public void testInvokeAssertionSoftDeleted() throws Exception {
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, STATUS_ASPECT_NAME, ChangeType.UPSERT, mockAssertionSoftDeleted());
    hook.invoke(event);

    Mockito.verify(assertionService, Mockito.times(1))
        .getAssertionInfo(Mockito.any(), Mockito.eq(TEST_ASSERTION_URN));
    Mockito.verify(subscriptionService, Mockito.times(1))
        .listEntityAssertionSubscriptions(
            Mockito.any(),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.anyInt());

    SubscriptionInfo expectedSubscription =
        new SubscriptionInfo(TEST_SUBSCRIPTION_INFO.get(TEST_SUBSCRIPTION_URN_2).data());
    expectedSubscription.setEntityChangeTypes(
        new EntityChangeDetailsArray(
            expectedSubscription.getEntityChangeTypes().stream()
                .map(
                    t -> {
                      if (!t.hasFilter() || !t.getFilter().hasIncludeAssertions()) return t;
                      t.getFilter()
                          .setIncludeAssertions(
                              new UrnArray(
                                  t.getFilter().getIncludeAssertions().stream()
                                      .filter(u -> !u.equals(TEST_ASSERTION_URN))
                                      .collect(Collectors.toSet())));
                      return t;
                    })
                .collect(Collectors.toSet())));

    // Ensure we ingested a new aspect.
    Mockito.verify(subscriptionService, Mockito.times(1))
        .updateSubscriptionInfo(
            Mockito.any(), Mockito.eq(TEST_SUBSCRIPTION_URN_2), Mockito.eq(expectedSubscription));
  }

  @Test()
  public void testInvokeAssertionInfoHardDeleted() throws Exception {
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_INFO_ASPECT_NAME,
            ChangeType.DELETE,
            null,
            mockFreshnessAssertion(TEST_DATASET_URN));
    hook.invoke(event);

    SubscriptionInfo expectedSubscription =
        new SubscriptionInfo(TEST_SUBSCRIPTION_INFO.get(TEST_SUBSCRIPTION_URN_2).data());
    expectedSubscription.setEntityChangeTypes(
        new EntityChangeDetailsArray(
            expectedSubscription.getEntityChangeTypes().stream()
                .map(
                    t -> {
                      if (!t.hasFilter() || !t.getFilter().hasIncludeAssertions()) return t;
                      t.getFilter()
                          .setIncludeAssertions(
                              new UrnArray(
                                  t.getFilter().getIncludeAssertions().stream()
                                      .filter(u -> !u.equals(TEST_ASSERTION_URN))
                                      .collect(Collectors.toSet())));
                      return t;
                    })
                .collect(Collectors.toSet())));

    // Ensure we ingested a new aspect.
    Mockito.verify(subscriptionService, Mockito.times(1))
        .updateSubscriptionInfo(
            Mockito.any(), Mockito.eq(TEST_SUBSCRIPTION_URN_2), Mockito.eq(expectedSubscription));
  }

  @Test
  public void testInvokeAssertionKeyHardDeleted() throws Exception {
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_KEY_ASPECT_NAME, ChangeType.DELETE, null, null);
    hook.invoke(event);

    SubscriptionInfo expectedSubscription =
        new SubscriptionInfo(TEST_SUBSCRIPTION_INFO.get(TEST_SUBSCRIPTION_URN_2).data());
    expectedSubscription.setEntityChangeTypes(
        new EntityChangeDetailsArray(
            expectedSubscription.getEntityChangeTypes().stream()
                .map(
                    t -> {
                      if (!t.hasFilter() || !t.getFilter().hasIncludeAssertions()) return t;
                      t.getFilter()
                          .setIncludeAssertions(
                              new UrnArray(
                                  t.getFilter().getIncludeAssertions().stream()
                                      .filter(u -> !u.equals(TEST_ASSERTION_URN))
                                      .collect(Collectors.toSet())));
                      return t;
                    })
                .collect(Collectors.toSet())));

    // Ensure we ingested a new aspect.
    Mockito.verify(subscriptionService, Mockito.times(1))
        .updateSubscriptionInfo(
            Mockito.any(), Mockito.eq(TEST_SUBSCRIPTION_URN_2), Mockito.eq(expectedSubscription));
  }

  @Test
  public void testInvokeAssertionEntityUpdate() throws Exception {

    // Ensure that the previous subscription is removed.
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockFreshnessAssertion(TEST_DATASET_URN_2),
            mockFreshnessAssertion(TEST_DATASET_URN));
    hook.invoke(event);

    Mockito.verify(subscriptionService, Mockito.times(1))
        .listEntityAssertionSubscriptions(
            Mockito.any(),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.anyInt());
    SubscriptionInfo expectedSubscription =
        new SubscriptionInfo(TEST_SUBSCRIPTION_INFO.get(TEST_SUBSCRIPTION_URN_2).data());
    expectedSubscription.setEntityChangeTypes(
        new EntityChangeDetailsArray(
            expectedSubscription.getEntityChangeTypes().stream()
                .map(
                    t -> {
                      if (!t.hasFilter() || !t.getFilter().hasIncludeAssertions()) return t;
                      t.getFilter()
                          .setIncludeAssertions(
                              new UrnArray(
                                  t.getFilter().getIncludeAssertions().stream()
                                      .filter(u -> !u.equals(TEST_ASSERTION_URN))
                                      .collect(Collectors.toSet())));
                      return t;
                    })
                .collect(Collectors.toSet())));

    // Ensure we ingested a new aspect.
    Mockito.verify(subscriptionService, Mockito.times(1))
        .updateSubscriptionInfo(
            Mockito.any(), Mockito.eq(TEST_SUBSCRIPTION_URN_2), Mockito.eq(expectedSubscription));
  }

  private Status mockAssertionSoftDeleted() {
    Status status = new Status();
    status.setRemoved(true);
    return status;
  }

  private AssertionInfo mockFreshnessAssertion(final Urn entityUrn) {
    AssertionInfo testInfo = new AssertionInfo();
    testInfo.setType(AssertionType.FRESHNESS);
    testInfo.setFreshnessAssertion(
        new FreshnessAssertionInfo()
            .setEntity(entityUrn)
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setSchedule(
                new FreshnessAssertionSchedule()
                    .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
                    .setFixedInterval(
                        new FixedIntervalSchedule()
                            .setMultiple(2)
                            .setUnit(CalendarInterval.HOUR))));
    return testInfo;
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
