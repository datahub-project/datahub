package com.linkedin.metadata.boot.steps;

import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.service.AssertionService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MigrateFreshnessAssertionCronToSinceTheLastCheck extends UpgradeStep {

  private static final String VERSION = "1";
  private static final String UPGRADE_ID =
      "migrate-freshness-assertion-cron-to-since-the-last-check";
  private static final Integer BATCH_SIZE = 5000;
  private final SearchService searchService;
  private final AssertionService assertionService;

  public MigrateFreshnessAssertionCronToSinceTheLastCheck(
      EntityService<?> entityService,
      SearchService searchService,
      AssertionService assertionService) {
    super(entityService, VERSION, UPGRADE_ID);
    this.searchService = searchService;
    this.assertionService = assertionService;
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.BLOCKING; // ensure there are no write conflicts.
  }

  @Override
  public void upgrade(@Nonnull OperationContext systemOpContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    int migratedCount = 0;
    String scrollId = null;
    do {
      log.info(
          String.format(
              "Migrating batch %s-%s of freshness assertions",
              migratedCount, migratedCount + BATCH_SIZE));
      scrollId = migrateFreshnessAssertions(systemOpContext, auditStamp, scrollId);
      migratedCount += BATCH_SIZE;
    } while (scrollId != null);
  }

  @Nullable
  private String migrateFreshnessAssertions(
      @Nonnull OperationContext systemOperationContext, AuditStamp auditStamp, String scrollId)
      throws Exception {
    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            systemOperationContext,
            ImmutableList.of(Constants.ASSERTION_ENTITY_NAME),
            "*",
            getFilter(),
            null,
            scrollId,
            "5m",
            BATCH_SIZE,
            null);
    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().isEmpty()) {
      return null;
    }
    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        updateFreshnessAssertion(systemOperationContext, searchEntity.getEntity(), auditStamp);
      } catch (Exception e) {
        // don't stop the whole step because of one bad urn or one bad ingestion
        log.error(
            String.format(
                "Error migrating AssertionInfo aspect for urn %s", searchEntity.getEntity()),
            e);
      }
    }

    return scrollResult.getScrollId();
  }

  private void updateFreshnessAssertion(
      @Nonnull OperationContext opContext, Urn assertionUrn, AuditStamp auditStamp)
      throws Exception {
    // 1. Get assertion info
    AssertionInfo assertionInfo = assertionService.getAssertionInfo(opContext, assertionUrn);

    // 2. validate it is a CRON freshness assertion
    if (assertionInfo == null) {
      log.warn(
          String.format(
              "Failed to find assertionInfo aspect for assertion with urn %s. Skipping migrating freshness assertion!",
              assertionUrn));
      return;
    }
    if (!assertionInfo.hasFreshnessAssertion()
        || !assertionInfo.getFreshnessAssertion().hasSchedule()) {
      log.warn(
          String.format(
              "assertionInfo aspect for assertion with urn %s is missing freshness assertion schedule. Skipping migrating freshness assertion!",
              assertionUrn));
      return;
    }
    FreshnessAssertionScheduleType scheduleType =
        assertionInfo.getFreshnessAssertion().getSchedule().getType();
    if (!scheduleType.equals(FreshnessAssertionScheduleType.CRON)) {
      log.warn(
          String.format(
              "assertionInfo aspect for assertion with urn %s does not have a cron schedule type, instead it is %s. Skipping migrating freshness assertion!",
              assertionUrn, scheduleType));
      return;
    }

    // 3. Update it to be since_the_last_check
    assertionInfo
        .getFreshnessAssertion()
        .setSchedule(
            new FreshnessAssertionSchedule()
                .setType(FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK));
    assertionInfo.setLastUpdated(auditStamp);

    this.entityService.ingestProposal(
        opContext,
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertionInfo),
        auditStamp,
        false);
  }

  @Nonnull
  private static Filter getFilter() {
    // Condition: is freshness assertion with scheduleType as cron
    Criterion isCronFreshnessAssertion =
        new Criterion()
            .setField("scheduleType")
            .setCondition(Condition.EQUAL)
            .setValue(FreshnessAssertionScheduleType.CRON.name());

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(isCronFreshnessAssertion);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }
}
