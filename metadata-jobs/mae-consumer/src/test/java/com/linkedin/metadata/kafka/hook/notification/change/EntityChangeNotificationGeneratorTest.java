package com.linkedin.metadata.kafka.hook.notification.change;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.RowCountTotal;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.DataPlatformInstance;
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
import com.linkedin.metadata.service.SettingsService;
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
  @Mock private SettingsService settingsService;
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
            settingsService,
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

  @Test
  public void testBuildAssertionStatusChangeTemplateParamsWithAllFields() {
    // Test with all optional parameters populated
    Urn assertionUrn = TEST_ASSERTION_URN_1;
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(
                new DatasetAssertionInfo()
                    .setDataset(TEST_ENTITY_URN_1)
                    .setScope(DatasetAssertionScope.UNKNOWN)
                    .setOperator(AssertionStdOperator.LESS_THAN))
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));

    AssertionResult result =
        new AssertionResult()
            .setType(AssertionResultType.SUCCESS)
            .setExternalUrl("https://example.com/result");

    DataPlatformInstance dataPlatformInstance =
        new DataPlatformInstance().setPlatform(UrnUtils.getUrn("urn:li:dataPlatform:snowflake"));

    Urn entityUrn = TEST_ENTITY_URN_1;
    String entityName = "test-entity";
    String entityType = "dataset";
    String entityPlatform = "snowflake";

    // Execute
    Map<String, String> templateParams =
        generator.buildAssertionStatusChangeTemplateParams(
            assertionUrn,
            assertionInfo,
            result,
            dataPlatformInstance,
            entityUrn,
            entityName,
            entityType,
            entityPlatform);

    // Verify required fields are populated
    assertEquals(templateParams.get("assertionType"), "DATASET");
    assertEquals(templateParams.get("assertionUrn"), assertionUrn.toString());
    assertEquals(templateParams.get("entityName"), entityName);
    assertEquals(templateParams.get("entityType"), entityType);
    assertEquals(templateParams.get("entityPlatform"), entityPlatform);
    assertEquals(templateParams.get("result"), "SUCCESS");
    assertEquals(templateParams.get("externalUrl"), "https://example.com/result");
    assertEquals(templateParams.get("sourceType"), "EXTERNAL");

    // Verify fields that depend on method calls are populated
    assertNotNull(templateParams.get("entityPath"));
    assertNotNull(templateParams.get("resultReason"));
    assertNotNull(templateParams.get("description"));
    assertNotNull(templateParams.get("externalPlatform"));
  }

  @Test
  public void testBuildAssertionStatusChangeTemplateParamsWithMinimalFields() {
    // Test with minimal required parameters only
    Urn assertionUrn = TEST_ASSERTION_URN_1;
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.VOLUME)
            .setVolumeAssertion(
                new VolumeAssertionInfo()
                    .setEntity(TEST_ENTITY_URN_1)
                    .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
                    .setRowCountTotal(
                        new RowCountTotal()
                            .setOperator(AssertionStdOperator.EQUAL_TO)
                            .setParameters(
                                new AssertionStdParameters()
                                    .setValue(
                                        new AssertionStdParameter()
                                            .setValue("0")
                                            .setType(AssertionStdParameterType.NUMBER)))));

    AssertionResult result = new AssertionResult().setType(AssertionResultType.FAILURE);

    Urn entityUrn = TEST_ENTITY_URN_1;
    String entityName = "test-entity";
    String entityType = "dataset";
    String entityPlatform = null; // null platform

    // Execute
    Map<String, String> templateParams =
        generator.buildAssertionStatusChangeTemplateParams(
            assertionUrn,
            assertionInfo,
            result,
            null, // null platform instance
            entityUrn,
            entityName,
            entityType,
            entityPlatform);

    // Verify required fields are populated
    assertEquals(templateParams.get("assertionType"), "VOLUME");
    assertEquals(templateParams.get("assertionUrn"), assertionUrn.toString());
    assertEquals(templateParams.get("entityName"), entityName);
    assertEquals(templateParams.get("entityType"), entityType);
    assertEquals(templateParams.get("result"), "FAILURE");

    // Verify optional fields are not populated when inputs are null
    assertNull(templateParams.get("entityPlatform"));
    assertNull(templateParams.get("externalUrl"));
    assertNull(templateParams.get("externalPlatform"));
    assertNull(templateParams.get("sourceType"));

    // Verify fields that depend on method calls are still populated
    assertNotNull(templateParams.get("entityPath"));
    assertNotNull(templateParams.get("resultReason"));
    assertNotNull(templateParams.get("description"));
  }

  @Test
  public void testBuildAssertionStatusChangeTemplateParamsWithDifferentResultTypes() {
    // Test with ERROR result type
    Urn assertionUrn = TEST_ASSERTION_URN_1;
    AssertionInfo assertionInfo =
        new AssertionInfo()
            .setType(AssertionType.SQL)
            .setSqlAssertion(
                new SqlAssertionInfo()
                    .setEntity(TEST_ENTITY_URN_1)
                    .setStatement("SELECT * FROM table")
                    .setType(SqlAssertionType.METRIC)
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setParameters(
                        new AssertionStdParameters()
                            .setValue(
                                new AssertionStdParameter()
                                    .setValue("0")
                                    .setType(AssertionStdParameterType.NUMBER))));

    AssertionResult result = new AssertionResult().setType(AssertionResultType.ERROR);

    Urn entityUrn = TEST_ENTITY_URN_1;
    String entityName = "test-entity";
    String entityType = "dataset";
    String entityPlatform = "mysql";

    // Execute
    Map<String, String> templateParams =
        generator.buildAssertionStatusChangeTemplateParams(
            assertionUrn,
            assertionInfo,
            result,
            null,
            entityUrn,
            entityName,
            entityType,
            entityPlatform);

    // Verify specific fields
    assertEquals(templateParams.get("assertionType"), "SQL");
    assertEquals(templateParams.get("result"), "ERROR");
    assertEquals(templateParams.get("entityPlatform"), entityPlatform);
  }
}
