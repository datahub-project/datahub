package com.linkedin.datahub.upgrade.system.policyfields;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

/**
 * This bootstrap step is responsible for upgrading DataHub policy documents with new searchable
 * fields in ES
 */
@Slf4j
public class BackfillPolicyFieldsStep implements UpgradeStep {
  private static final String UPGRADE_ID = "BackfillPolicyFieldsStep_V2";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext opContext;
  private final boolean reprocessEnabled;
  private final Integer batchSize;
  private final EntityService<?> entityService;
  private final SearchService _searchService;

  public BackfillPolicyFieldsStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this._searchService = searchService;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      String scrollId = null;
      int migratedCount = 0;
      do {
        log.info("Upgrading batch of policies {}-{}", migratedCount, migratedCount + batchSize);
        scrollId = backfillPolicies(context, auditStamp, scrollId);
        migratedCount += batchSize;
      } while (scrollId != null);

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return true;
  }

  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variables REPROCESS_DEFAULT_POLICY_FIELDS & BACKFILL_BROWSE_PATHS_V2 to determine whether to
   * skip.
   */
  @Override
  public boolean skip(UpgradeContext context) {

    if (reprocessEnabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }

  private String backfillPolicies(UpgradeContext context, AuditStamp auditStamp, String scrollId) {

    final Filter filter = backfillPolicyFieldFilter();
    final ScrollResult scrollResult =
        _searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            ImmutableList.of(Constants.POLICY_ENTITY_NAME),
            "*",
            filter,
            null,
            scrollId,
            null,
            batchSize);

    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().isEmpty()) {
      return null;
    }

    List<Future<?>> futures = new LinkedList<>();
    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        ingestPolicyFields(context, searchEntity.getEntity(), auditStamp).ifPresent(futures::add);
      } catch (Exception e) {
        // don't stop the whole step because of one bad urn or one bad ingestion
        log.error(
            String.format(
                "Error ingesting default browsePathsV2 aspect for urn %s",
                searchEntity.getEntity()),
            e);
      }
    }

    futures.forEach(
        f -> {
          try {
            f.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    return scrollResult.getScrollId();
  }

  private Filter backfillPolicyFieldFilter() {
    // Condition: Does not have at least 1 of: `privileges`, `editable`, `state`, `type`, `users`,
    // `groups`, `allUsers`
    // `allGroups` or `roles`
    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();

    conjunctiveCriterionArray.add(getCriterionForMissingField("privilege"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("editable"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("state"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("type"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("users"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("groups"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("roles"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("allUsers"));
    conjunctiveCriterionArray.add(getCriterionForMissingField("allGroups"));

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private Optional<Future<?>> ingestPolicyFields(
      UpgradeContext context, Urn urn, AuditStamp auditStamp) {
    EntityResponse entityResponse = null;
    try {
      entityResponse =
          entityService.getEntityV2(
              context.opContext(),
              urn.getEntityType(),
              urn,
              Collections.singleton(DATAHUB_POLICY_INFO_ASPECT_NAME));
    } catch (URISyntaxException e) {
      log.error(
          String.format(
              "Error getting DataHub Policy Info for entity with urn %s while restating policy information",
              urn),
          e);
    }

    if (entityResponse != null
        && entityResponse.getAspects().containsKey(DATAHUB_POLICY_INFO_ASPECT_NAME)) {
      final DataMap dataMap =
          entityResponse.getAspects().get(DATAHUB_POLICY_INFO_ASPECT_NAME).getValue().data();
      final DataHubPolicyInfo infoAspect = new DataHubPolicyInfo(dataMap);

      log.debug("Restating policy information for urn {} with value {}", urn, infoAspect);
      return Optional.of(
          entityService
              .alwaysProduceMCLAsync(
                  context.opContext(),
                  urn,
                  urn.getEntityType(),
                  DATAHUB_POLICY_INFO_ASPECT_NAME,
                  opContext
                      .getEntityRegistry()
                      .getAspectSpecs()
                      .get(DATAHUB_POLICY_INFO_ASPECT_NAME),
                  null,
                  infoAspect,
                  null,
                  createDefaultSystemMetadata(),
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    return Optional.empty();
  }

  @NotNull
  private static ConjunctiveCriterion getCriterionForMissingField(String field) {
    final Criterion missingPrivilegesField = buildIsNullCriterion(field);

    final CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(missingPrivilegesField);
    final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);
    return conjunctiveCriterion;
  }
}
