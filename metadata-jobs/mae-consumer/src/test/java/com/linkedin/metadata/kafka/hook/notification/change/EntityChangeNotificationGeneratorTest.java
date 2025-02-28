package com.linkedin.metadata.kafka.hook.notification.change;

import static org.testng.Assert.assertEquals;

import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.NotificationRecipientsGeneratorExtraContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.EntityChangeDetailsFilter;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionType;
import com.linkedin.subscription.SubscriptionTypeArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityChangeNotificationGeneratorTest {
  private static final String TEST_ACTOR_URN_STR_1 = "urn:li:corpUser:test1";
  private static final Urn TEST_ACTOR_URN_1 = UrnUtils.getUrn(TEST_ACTOR_URN_STR_1);
  private static final String TEST_ACTOR_URN_STR_2 = "urn:li:corpUser:test2";
  private static final Urn TEST_ACTOR_URN_2 = UrnUtils.getUrn(TEST_ACTOR_URN_STR_2);
  private static final String TEST_ENTITY_URN_STR_1 =
      "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)";
  private static final Urn TEST_ENTITY_URN_1 = UrnUtils.getUrn(TEST_ENTITY_URN_STR_1);
  private static final String TEST_ENTITY_URN_STR_2 =
      "urn:li:dataset:(urn:li:dataPlatform:foo2,bar2,PROD)";
  private static final Urn TEST_ENTITY_URN_2 = UrnUtils.getUrn(TEST_ENTITY_URN_STR_2);
  private static final String TEST_ASSERTION_URN_STR_1 = "urn:li:assertion:test1";
  private static final Urn TEST_ASSERTION_URN_1 = UrnUtils.getUrn(TEST_ASSERTION_URN_STR_1);
  private static final String TEST_ASSERTION_URN_STR_2 = "urn:li:assertion:test2";
  private static final Urn TEST_ASSERTION_URN_2 = UrnUtils.getUrn(TEST_ASSERTION_URN_STR_2);

  private static final SubscriptionInfo TEST_SUBSCRIPTION_INFO_NO_ASSERTIONS =
      new SubscriptionInfo()
          .setEntityUrn(TEST_ENTITY_URN_2)
          .setTypes(new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE))
          .setActorUrn(TEST_ACTOR_URN_2)
          .setActorType(ActorType.USER.name())
          .setEntityChangeTypes(
              new EntityChangeDetailsArray(
                  new EntityChangeDetails()
                      .setEntityChangeType(EntityChangeType.GLOSSARY_TERM_ADDED)));
  private static final SubscriptionInfo TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS =
      new SubscriptionInfo()
          .setEntityUrn(TEST_ENTITY_URN_1)
          .setTypes(new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE))
          .setActorUrn(TEST_ACTOR_URN_1)
          .setActorType(ActorType.USER.name())
          .setEntityChangeTypes(
              new EntityChangeDetailsArray(
                  new EntityChangeDetails().setEntityChangeType(EntityChangeType.ASSERTION_PASSED),
                  new EntityChangeDetails().setEntityChangeType(EntityChangeType.ASSERTION_ERROR)));

  private static final SubscriptionInfo TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS =
      new SubscriptionInfo()
          .setEntityUrn(TEST_ENTITY_URN_1)
          .setTypes(new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE))
          .setActorUrn(TEST_ACTOR_URN_1)
          .setActorType(ActorType.USER.name())
          .setEntityChangeTypes(
              new EntityChangeDetailsArray(
                  new EntityChangeDetails()
                      .setEntityChangeType(EntityChangeType.ASSERTION_PASSED)
                      .setFilter(
                          new EntityChangeDetailsFilter()
                              .setIncludeAssertions(new UrnArray(TEST_ASSERTION_URN_1))),
                  new EntityChangeDetails().setEntityChangeType(EntityChangeType.ASSERTION_ERROR)));
  private static final SubscriptionInfo TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS =
      new SubscriptionInfo()
          .setEntityUrn(TEST_ENTITY_URN_1)
          .setTypes(new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE))
          .setActorUrn(TEST_ACTOR_URN_1)
          .setActorType(ActorType.USER.name())
          .setEntityChangeTypes(
              new EntityChangeDetailsArray(
                  new EntityChangeDetails()
                      .setEntityChangeType(EntityChangeType.ASSERTION_PASSED)
                      .setFilter(
                          new EntityChangeDetailsFilter()
                              .setIncludeAssertions(new UrnArray(TEST_ASSERTION_URN_1))),
                  new EntityChangeDetails()
                      .setEntityChangeType(EntityChangeType.ASSERTION_ERROR)
                      .setFilter(
                          new EntityChangeDetailsFilter()
                              .setIncludeAssertions(new UrnArray(TEST_ASSERTION_URN_1)))));

  @Mock private OperationContext operationContext;
  @Mock private EntityChangeEventGeneratorRegistry eventGeneratorRegistry;
  @Mock private EventProducer eventProducer;
  @Mock private SystemEntityClient entityClient;
  @Mock private GraphClient graphClient;
  @Mock private SettingsProvider settingsProvider;
  @Mock private AssertionService assertionService;
  @Mock private NotificationRecipientBuilders recipientBuilders;
  @Mock private FeatureFlags featureFlags;

  private EntityChangeNotificationGenerator generator;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(operationContext.getAuthentication())
        .thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(operationContext.getEntityRegistry())
        .thenReturn(Mockito.mock(EntityRegistry.class));
    generator =
        new EntityChangeNotificationGenerator(
            operationContext,
            eventGeneratorRegistry,
            eventProducer,
            entityClient,
            graphClient,
            settingsProvider,
            assertionService,
            recipientBuilders,
            featureFlags);
  }

  // 1. Should not filter when the subresource is not related to an assertion
  @Test
  public void testApplySubscriptionFiltersIrrelevantChangeCase() {
    Map<Urn, SubscriptionInfo> inputMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS);
    Map<Urn, SubscriptionInfo> outputMap =
        generator.applySubscriptionFiltersToSubscriptionMap(
            inputMap,
            EntityChangeType.OPERATION_COLUMN_MODIFIED,
            new NotificationRecipientsGeneratorExtraContext());
    assertEquals(inputMap, outputMap);
  }

  // 2. Should not filter when there are no assertion subscriptions
  @Test
  public void testApplySubscriptionFiltersNoAssertionSubscription() {
    Map<Urn, SubscriptionInfo> inputMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_NO_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_NO_ASSERTIONS,
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS);
    Map<Urn, SubscriptionInfo> expectedMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_NO_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_NO_ASSERTIONS);
    Map<Urn, SubscriptionInfo> outputMap =
        generator.applySubscriptionFiltersToSubscriptionMap(
            inputMap,
            EntityChangeType.ASSERTION_PASSED,
            new NotificationRecipientsGeneratorExtraContext().setModifierUrn(TEST_ASSERTION_URN_2));
    assertEquals(expectedMap, outputMap);
  }

  // 3. Should not filter when there are only 'all' assertion subscriptions
  @Test
  public void testApplySubscriptionFiltersAllAssertionSubscription() {
    TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS.setEntityUrn(TEST_ENTITY_URN_2);
    Map<Urn, SubscriptionInfo> inputMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS,
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS);
    Map<Urn, SubscriptionInfo> expectedMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_ALL_ASSERTIONS);
    Map<Urn, SubscriptionInfo> outputMap =
        generator.applySubscriptionFiltersToSubscriptionMap(
            inputMap,
            EntityChangeType.ASSERTION_PASSED,
            new NotificationRecipientsGeneratorExtraContext().setModifierUrn(TEST_ASSERTION_URN_2));
    assertEquals(expectedMap, outputMap);
  }

  // 3. Should not filter when there is any 'all' assertion subscriptions for the change type
  @Test
  public void testApplySubscriptionFiltersPartialAssertionSubscription() {
    TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS.setEntityUrn(TEST_ENTITY_URN_2);
    Map<Urn, SubscriptionInfo> inputMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS,
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS);
    Map<Urn, SubscriptionInfo> expectedMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS);
    Map<Urn, SubscriptionInfo> outputMap =
        generator.applySubscriptionFiltersToSubscriptionMap(
            inputMap,
            EntityChangeType.ASSERTION_ERROR,
            new NotificationRecipientsGeneratorExtraContext().setModifierUrn(TEST_ASSERTION_URN_2));
    assertEquals(expectedMap, outputMap);
  }

  // 4. Should not filter when there is a specific assertion subscription for the change type
  // including trigger assertion
  @Test
  public void testApplySubscriptionFiltersSpecificAssertionSubscriptionIncludingTrigger() {
    TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS.setEntityUrn(TEST_ENTITY_URN_2);
    Map<Urn, SubscriptionInfo> inputMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS,
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS);
    Map<Urn, SubscriptionInfo> outputMap =
        generator.applySubscriptionFiltersToSubscriptionMap(
            inputMap,
            EntityChangeType.ASSERTION_PASSED,
            new NotificationRecipientsGeneratorExtraContext().setModifierUrn(TEST_ASSERTION_URN_1));
    assertEquals(inputMap, outputMap);
  }

  // 5. Should filter when there is a specific assertion subscription for the change type not
  // including trigger assertion
  @Test
  public void testApplySubscriptionFiltersSpecificAssertionSubscriptionExcludingTrigger() {
    TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS.setEntityUrn(TEST_ENTITY_URN_2);
    Map<Urn, SubscriptionInfo> inputMap =
        Map.of(
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_PARTIAL_SPECIFIC_ASSERTIONS,
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS.getEntityUrn(),
            TEST_SUBSCRIPTION_INFO_SPECIFIC_ASSERTIONS);
    Map<Urn, SubscriptionInfo> expectedMap = Collections.emptyMap();
    Map<Urn, SubscriptionInfo> outputMap =
        generator.applySubscriptionFiltersToSubscriptionMap(
            inputMap,
            EntityChangeType.ASSERTION_PASSED,
            new NotificationRecipientsGeneratorExtraContext().setModifierUrn(TEST_ASSERTION_URN_2));
    assertEquals(expectedMap, outputMap);
  }
}
