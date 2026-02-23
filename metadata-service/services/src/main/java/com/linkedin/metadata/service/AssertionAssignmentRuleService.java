package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionManagedBy;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.RowCountTotal;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.assertion.rule.FreshnessAssertionAssignmentRuleConfig;
import com.linkedin.assertion.rule.SubscriptionAssignmentRuleConfig;
import com.linkedin.assertion.rule.VolumeAssertionAssignmentRuleConfig;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleTestBuilder;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionManagedBy;
import com.linkedin.subscription.SubscriptionManagedByArray;
import com.linkedin.subscription.SubscriptionType;
import com.linkedin.subscription.SubscriptionTypeArray;
import com.linkedin.test.TestInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to execute CRUD operations around assertion assignment rules and their backing
 * automations (metadata tests), including managing the lifecycle of assertions, monitors, and
 * subscriptions on target entities.
 */
@Slf4j
public class AssertionAssignmentRuleService extends BaseService {

  // Default evaluation schedule for managed monitors
  private static final String DEFAULT_CRON = "0 8 * * *";
  private static final String DEFAULT_TIMEZONE = "UTC";

  private static final List<AssertionType> MANAGED_ASSERTION_TYPES =
      ImmutableList.of(AssertionType.FRESHNESS, AssertionType.VOLUME);

  private static final String MONITOR_COUNT_CACHE_KEY = "activeMonitorCount";
  private static final long MONITOR_COUNT_TTL_MINUTES = 5;

  // Expect at most 2 managed assertions per (rule, entity) pair — one per assertion type
  private static final int MAX_MANAGED_ASSERTIONS_PER_ENTITY = 10;

  private final AssertionService assertionService;
  private final MonitorService monitorService;
  private final SubscriptionService subscriptionService;
  private final Cache<String, Integer> monitorCountCache;

  public AssertionAssignmentRuleService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      @Nonnull final AssertionService assertionService,
      @Nonnull final MonitorService monitorService,
      @Nonnull final SubscriptionService subscriptionService) {
    super(entityClient, openApiClient, objectMapper);
    this.assertionService = assertionService;
    this.monitorService = monitorService;
    this.subscriptionService = subscriptionService;
    this.monitorCountCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(MONITOR_COUNT_TTL_MINUTES, TimeUnit.MINUTES)
            .build();
  }

  /** Upsert the backing metadata test automation for an assertion assignment rule. */
  public void upsertAssertionAssignmentRuleAutomation(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final AssertionAssignmentRuleInfo ruleInfo) {
    final Urn testUrn =
        AssertionAssignmentRuleUtils.createTestUrnForAssertionAssignmentRule(ruleUrn);
    final TestInfo testInfo =
        AssertionAssignmentRuleTestBuilder.buildAssertionAssignmentRuleTest(
            opContext, ruleUrn, ruleInfo);
    try {
      final List<MetadataChangeProposal> changes =
          ImmutableList.of(
              AspectUtils.buildMetadataChangeProposal(testUrn, TEST_INFO_ASPECT_NAME, testInfo));
      ingestChangeProposals(opContext, changes, false);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          String.format(
              "Failed to upsert assertion assignment rule automation for rule with urn: %s",
              ruleUrn),
          e);
    }
  }

  /**
   * Remove the backing metadata test automation for an assertion assignment rule.
   *
   * <p>The transactional deletion of the backing test entity is handled by
   * AssertionAssignmentRuleDeleteSideEffect. This method handles additional async cleanup (e.g.,
   * removing managed assertions from target entities via SearchBasedAssertionAssignmentRuleRunner)
   * once that runner is implemented.
   *
   * <p>This cleanup is invoked from an MCL hook rather than an MCPSideEffect because it requires
   * OperationContext and SystemEntityClient to perform search queries and entity mutations (e.g.,
   * finding all target entities that have managed assertions and removing them). The
   * MCPSideEffect's RetrieverContext only supports generating additional MCPs within the same
   * transaction—it does not provide the service-layer access needed for search-based fan-out
   * operations.
   */
  public void removeAssertionAssignmentRuleAutomation(
      @Nonnull OperationContext opContext, @Nonnull final Urn ruleUrn) {
    log.info("Assertion assignment rule deleted: {}", ruleUrn);
  }

  /** Get the AssertionAssignmentRuleInfo aspect for a given rule urn. */
  @Nullable
  public AssertionAssignmentRuleInfo getRuleInfo(
      @Nonnull OperationContext opContext, @Nonnull final Urn ruleUrn) {
    try {
      final EntityResponse response =
          this.entityClient.getV2(
              opContext,
              ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME,
              ruleUrn,
              ImmutableSet.of(ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME)) {
        return new AssertionAssignmentRuleInfo(
            response
                .getAspects()
                .get(ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME)
                .getValue()
                .data());
      }
      return null;
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve assertion assignment rule info for rule with urn: %s", ruleUrn),
          e);
    }
  }

  /** List assertion assignment rules by query and filters. */
  public SearchResult listRules(
      @Nonnull OperationContext opContext,
      @Nonnull final String query,
      @Nullable final Filter filters,
      final int start,
      final int count) {
    try {
      return this.entityClient.search(
          opContext, ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME, query, filters, null, start, count);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(
          String.format(
              "Failed to list assertion assignment rules with query: '%s', start: %d, count: %d",
              query, start, count),
          e);
    }
  }

  /**
   * Count active (non-INACTIVE) monitors. Uses the same filter logic as {@code
   * MonitorLimitValidator.countExistingMonitors()}, but via entityClient.filter(). Results are
   * cached for {@link #MONITOR_COUNT_TTL_MINUTES} to avoid repeated search queries across multiple
   * rule actions in the same test engine run.
   */
  public int countActiveMonitors(@Nonnull OperationContext opContext) {
    Integer cached = monitorCountCache.getIfPresent(MONITOR_COUNT_CACHE_KEY);
    if (cached != null) {
      return cached;
    }
    try {
      final Filter activeMonitorsFilter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion()
                          .setAnd(
                              new CriterionArray(
                                  CriterionUtils.buildCriterion(
                                      "mode", Condition.EQUAL, true, "INACTIVE")))));
      final SearchResult result =
          this.entityClient.filter(
              opContext, MONITOR_ENTITY_NAME, activeMonitorsFilter, null, 0, 0);
      int count = result.getNumEntities();
      monitorCountCache.put(MONITOR_COUNT_CACHE_KEY, count);
      return count;
    } catch (RemoteInvocationException e) {
      log.error("Failed to count active monitors, defaulting to 0", e);
      return 0;
    }
  }

  // ---------------------------------------------------------------------------
  // Managed assertion + monitor lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Upsert a managed freshness assertion and its monitor for the given entity.
   *
   * @return 1 if a new monitor was created, 0 if the assertion already existed (update only)
   */
  public int upsertManagedFreshnessAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn ruleUrn,
      @Nonnull final FreshnessAssertionAssignmentRuleConfig config)
      throws Exception {
    final Urn assertionUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            ruleUrn, entityUrn, AssertionType.FRESHNESS);
    final boolean isNew = !this.entityClient.exists(opContext, assertionUrn);

    final AssertionSource source = buildAssertionSource(opContext, ruleUrn);
    final AssertionActions actions =
        buildAssertionActions(
            config.hasOnSuccess() ? config.getOnSuccess() : null,
            config.hasOnFailure() ? config.getOnFailure() : null);

    // Schedule=null is valid for INFERRED assertions
    assertionService.upsertDatasetFreshnessAssertion(
        opContext,
        assertionUrn,
        entityUrn,
        null,
        null,
        null,
        actions,
        source,
        METADATA_TESTS_SOURCE);

    if (isNew) {
      final AssertionEvaluationParameters evalParams =
          new AssertionEvaluationParameters()
              .setType(AssertionEvaluationParametersType.DATASET_FRESHNESS);
      if (config.hasPreferredEvaluationParameters()) {
        evalParams.setDatasetFreshnessParameters(config.getPreferredEvaluationParameters());
      }
      return createMonitorForAssertion(opContext, entityUrn, assertionUrn, evalParams);
    }

    return 0;
  }

  /**
   * Upsert a managed volume assertion and its monitor for the given entity.
   *
   * @return 1 if a new monitor was created, 0 if the assertion already existed (update only)
   */
  public int upsertManagedVolumeAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn ruleUrn,
      @Nonnull final VolumeAssertionAssignmentRuleConfig config)
      throws Exception {
    final Urn assertionUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            ruleUrn, entityUrn, AssertionType.VOLUME);
    final boolean isNew = !this.entityClient.exists(opContext, assertionUrn);

    final AssertionSource source = buildAssertionSource(opContext, ruleUrn);
    final AssertionActions actions =
        buildAssertionActions(
            config.hasOnSuccess() ? config.getOnSuccess() : null,
            config.hasOnFailure() ? config.getOnFailure() : null);

    // Seed values only — the INFERRED source type triggers auto-calibration, which replaces
    // these with learned thresholds. Matches the bulk create UI defaults.
    final VolumeAssertionInfo volumeInfo =
        new VolumeAssertionInfo()
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
            .setEntity(entityUrn)
            .setRowCountTotal(
                new RowCountTotal()
                    .setOperator(AssertionStdOperator.BETWEEN)
                    .setParameters(
                        new AssertionStdParameters()
                            .setMinValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("0"))
                            .setMaxValue(
                                new AssertionStdParameter()
                                    .setType(AssertionStdParameterType.NUMBER)
                                    .setValue("1000"))));

    assertionService.upsertDatasetVolumeAssertion(
        opContext,
        assertionUrn,
        entityUrn,
        null,
        volumeInfo,
        actions,
        source,
        METADATA_TESTS_SOURCE);

    if (isNew) {
      final AssertionEvaluationParameters evalParams =
          new AssertionEvaluationParameters()
              .setType(AssertionEvaluationParametersType.DATASET_VOLUME);
      if (config.hasPreferredEvaluationParameters()) {
        evalParams.setDatasetVolumeParameters(config.getPreferredEvaluationParameters());
      }
      return createMonitorForAssertion(opContext, entityUrn, assertionUrn, evalParams);
    }

    return 0;
  }

  // ---------------------------------------------------------------------------
  // Subscription sync
  // ---------------------------------------------------------------------------

  /**
   * Sync subscriptions additively for a single entity — new change types are merged in, but
   * existing types are never removed. A user may have independent subscriptions we must not
   * disturb.
   */
  public void syncSubscriptionsForEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn ruleUrn,
      @Nonnull final SubscriptionAssignmentRuleConfig config) {
    final List<EntityChangeDetails> changeDetails =
        config.getEntityChangeTypes().stream()
            .map(type -> new EntityChangeDetails().setEntityChangeType(type))
            .collect(Collectors.toList());

    for (Urn subscriberUrn : config.getSubscribers()) {
      try {
        Map.Entry<Urn, SubscriptionInfo> existing =
            subscriptionService.getSubscription(opContext, entityUrn, subscriberUrn);

        if (existing != null) {
          updateExistingSubscription(
              opContext, existing, ruleUrn, changeDetails, config.getEntityChangeTypes());
        } else {
          createNewSubscription(
              opContext,
              subscriberUrn,
              entityUrn,
              ruleUrn,
              changeDetails,
              config.getEntityChangeTypes());
        }
      } catch (Exception e) {
        log.error(
            "Failed to sync subscription for entity {} subscriber {} rule {}",
            entityUrn,
            subscriberUrn,
            ruleUrn,
            e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Managed assertion removal
  // ---------------------------------------------------------------------------

  /**
   * Remove all managed assertions for a batch of (rule, entity) pairs. Monitors are cleaned up by a
   * separate hook when the assertion is deleted.
   *
   * <p>Phase 1: deterministic URN deletion — iterate entityUrns × MANAGED_ASSERTION_TYPES,
   * construct each URN, check existence, delete. Phase 2: single batch orphan search using
   * Condition.IN across all entity URNs, delete any that weren't already handled in Phase 1.
   */
  public void removeManagedAssertionsForEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final List<Urn> entityUrns) {
    // Phase 1: Delete assertions with known deterministic URNs
    final Set<Urn> deleted = new HashSet<>();
    for (Urn entityUrn : entityUrns) {
      for (AssertionType type : MANAGED_ASSERTION_TYPES) {
        final Urn assertionUrn =
            AssertionAssignmentRuleUtils.createAssertionUrnForRule(ruleUrn, entityUrn, type);
        try {
          if (this.entityClient.exists(opContext, assertionUrn)) {
            assertionService.tryDeleteAssertionReferences(opContext, assertionUrn);
            assertionService.tryDeleteAssertion(opContext, assertionUrn);
            deleted.add(assertionUrn);
          }
        } catch (RemoteInvocationException e) {
          throw new RuntimeException(
              String.format(
                  "Failed to check existence of managed assertion %s for rule %s, entity %s",
                  assertionUrn, ruleUrn, entityUrn),
              e);
        }
      }
    }

    // Phase 2: Single batch search sweep to catch any orphaned assertions
    final List<Urn> orphaned = findAllManagedAssertionsForEntities(opContext, ruleUrn, entityUrns);
    for (Urn assertionUrn : orphaned) {
      if (!deleted.contains(assertionUrn)) {
        log.warn(
            "Found orphaned managed assertion {} for rule {}, cleaning up", assertionUrn, ruleUrn);
        assertionService.tryDeleteAssertionReferences(opContext, assertionUrn);
        assertionService.tryDeleteAssertion(opContext, assertionUrn);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Search helpers
  // ---------------------------------------------------------------------------

  /**
   * Find all managed assertions for a (rule, entities) batch, regardless of assertion type. Uses a
   * single search with Condition.IN on the entity field.
   */
  @Nonnull
  public List<Urn> findAllManagedAssertionsForEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final List<Urn> entityUrns) {
    if (entityUrns.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      final List<String> entityUrnStrings =
          entityUrns.stream().map(Urn::toString).collect(Collectors.toList());
      final Filter filter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion()
                          .setAnd(
                              new CriterionArray(
                                  ImmutableList.of(
                                      CriterionUtils.buildCriterion(
                                          "managedBySourceEntity",
                                          Condition.EQUAL,
                                          ruleUrn.toString()),
                                      CriterionUtils.buildCriterion(
                                          "entity", Condition.IN, entityUrnStrings))))));
      final int limit = entityUrns.size() * MAX_MANAGED_ASSERTIONS_PER_ENTITY;
      final SearchResult result =
          this.entityClient.filter(opContext, ASSERTION_ENTITY_NAME, filter, null, 0, limit);
      if (!result.hasEntities() || result.getEntities().isEmpty()) {
        return Collections.emptyList();
      }
      return result.getEntities().stream()
          .map(SearchEntity::getEntity)
          .collect(Collectors.toList());
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(
          String.format(
              "Failed to find all managed assertions for rule %s, entities %s",
              ruleUrn, entityUrns),
          e);
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Create a monitor for the given assertion. Returns 1 on success. On monitor limit exceeded,
   * cleans up the orphaned assertion and rethrows.
   */
  private int createMonitorForAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionEvaluationParameters evalParams)
      throws Exception {
    final CronSchedule cronSchedule =
        new CronSchedule().setCron(DEFAULT_CRON).setTimezone(DEFAULT_TIMEZONE);
    try {
      monitorService.createAssertionMonitor(
          opContext,
          entityUrn,
          assertionUrn,
          cronSchedule,
          evalParams,
          null,
          METADATA_TESTS_SOURCE);
      return 1;
    } catch (Exception e) {
      if (e.getMessage() != null
          && e.getMessage().contains(AcrylConstants.MONITOR_LIMIT_EXCEEDED_ERROR_MESSAGE_PREFIX)) {
        log.warn(
            "Monitor limit exceeded when creating monitor for assertion {}, cleaning up",
            assertionUrn);
        assertionService.tryDeleteAssertionReferences(opContext, assertionUrn);
        assertionService.tryDeleteAssertion(opContext, assertionUrn);
      }
      throw e;
    }
  }

  @Nonnull
  private AssertionSource buildAssertionSource(
      @Nonnull OperationContext opContext, @Nonnull final Urn ruleUrn) {
    Urn actorUrn = null;
    try {
      actorUrn = Urn.createFromString(opContext.getSessionAuthentication().getActor().toUrnStr());
    } catch (Exception e) {
      log.error("Could not parse actor urn", e);
    }
    final AssertionSource source =
        new AssertionSource()
            .setType(AssertionSourceType.INFERRED)
            .setManagedBy(
                new AssertionManagedBy()
                    .setSourceEntity(ruleUrn)
                    .setRunId(UUID.randomUUID().toString()));
    if (actorUrn != null) {
      source.setCreated(new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn));
    }
    return source;
  }

  @Nonnull
  static AssertionActions buildAssertionActions(
      @Nullable final AssertionActionArray onSuccess,
      @Nullable final AssertionActionArray onFailure) {
    return new AssertionActions()
        .setOnSuccess(onSuccess != null ? onSuccess : new AssertionActionArray())
        .setOnFailure(onFailure != null ? onFailure : new AssertionActionArray());
  }

  private void updateExistingSubscription(
      @Nonnull OperationContext opContext,
      @Nonnull final Map.Entry<Urn, SubscriptionInfo> existing,
      @Nonnull final Urn ruleUrn,
      @Nonnull final List<EntityChangeDetails> newChangeDetails,
      @Nonnull final EntityChangeTypeArray configChangeTypes) {
    final SubscriptionInfo info = existing.getValue();
    final EntityChangeDetailsArray mergedTypes =
        mergeEntityChangeDetails(
            info.hasEntityChangeTypes()
                ? info.getEntityChangeTypes()
                : new EntityChangeDetailsArray(),
            new EntityChangeDetailsArray(newChangeDetails));
    info.setEntityChangeTypes(mergedTypes);
    updateManagedBy(info, ruleUrn, configChangeTypes);
    subscriptionService.updateSubscriptionInfo(opContext, existing.getKey(), info);
  }

  private void createNewSubscription(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn subscriberUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn ruleUrn,
      @Nonnull final List<EntityChangeDetails> changeDetails,
      @Nonnull final EntityChangeTypeArray configChangeTypes) {
    final SubscriptionManagedByArray managedBy =
        new SubscriptionManagedByArray(
            new SubscriptionManagedBy()
                .setSourceEntity(ruleUrn)
                .setLastAppliedChangeTypes(new EntityChangeTypeArray(configChangeTypes)));
    subscriptionService.createSubscription(
        opContext,
        subscriberUrn,
        entityUrn,
        new SubscriptionTypeArray(SubscriptionType.ENTITY_CHANGE),
        new EntityChangeDetailsArray(changeDetails),
        null,
        managedBy);
  }

  /**
   * Merge entity change details additively. Incoming types overwrite existing types with the same
   * EntityChangeType; existing types not in incoming are preserved.
   */
  static EntityChangeDetailsArray mergeEntityChangeDetails(
      @Nonnull EntityChangeDetailsArray existing, @Nonnull EntityChangeDetailsArray incoming) {
    final Map<EntityChangeType, EntityChangeDetails> typeToDetails = new LinkedHashMap<>();
    for (EntityChangeDetails detail : existing) {
      typeToDetails.put(detail.getEntityChangeType(), detail);
    }
    for (EntityChangeDetails detail : incoming) {
      typeToDetails.put(detail.getEntityChangeType(), detail);
    }
    return new EntityChangeDetailsArray(typeToDetails.values());
  }

  /** Add or update our rule's entry in the subscription's managedBy array. */
  static void updateManagedBy(
      @Nonnull SubscriptionInfo info,
      @Nonnull final Urn ruleUrn,
      @Nonnull final EntityChangeTypeArray configChangeTypes) {
    SubscriptionManagedByArray managedByArray =
        info.hasManagedBy() ? info.getManagedBy() : new SubscriptionManagedByArray();

    boolean found = false;
    for (SubscriptionManagedBy entry : managedByArray) {
      if (ruleUrn.equals(entry.getSourceEntity())) {
        entry.setLastAppliedChangeTypes(new EntityChangeTypeArray(configChangeTypes));
        found = true;
        break;
      }
    }

    if (!found) {
      managedByArray.add(
          new SubscriptionManagedBy()
              .setSourceEntity(ruleUrn)
              .setLastAppliedChangeTypes(new EntityChangeTypeArray(configChangeTypes)));
    }

    info.setManagedBy(managedByArray);
  }
}
