package com.linkedin.datahub.upgrade.system.corpuserinfo;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.*;

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
import com.linkedin.identity.CorpUserInfo;
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

/**
 * This bootstrap step is responsible for upgrading DataHub CorpUser Info documents with the new
 * searchable field: system in ES
 */
@Slf4j
public class BackfillCorpUserInfoStep implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillCorpUserInfoStep";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public BackfillCorpUserInfoStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.searchService = searchService;
    this.entityService = entityService;
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
        log.info("Upgrading batch of users {}-{}", migratedCount, migratedCount + batchSize);
        scrollId = backfillCorpUserInfo(context, auditStamp, scrollId);
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
   * variables BOOTSTRAP_SYSTEM_UPDATE_CORP_USER_INFO_REPROCESS to determine whether to skip.
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

  private String backfillCorpUserInfo(
      UpgradeContext context, AuditStamp auditStamp, String scrollId) {
    final Filter filter = backfillCorpUserSystemFieldFilter();
    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            ImmutableList.of(CORP_USER_ENTITY_NAME),
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
        ingestCorpUserInfo(context, searchEntity.getEntity(), auditStamp).ifPresent(futures::add);
      } catch (Exception e) {
        // don't stop the whole step because of one bad urn or one bad ingestion
        log.error(
            "Error ingesting default corpUserInfo aspect for urn {}", searchEntity.getEntity(), e);
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

  private Filter backfillCorpUserSystemFieldFilter() {
    // Condition: Does not have the following property in elasticsearch: `system`
    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();

    final Criterion missingPrivilegesField = buildIsNullCriterion("system");
    final CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(missingPrivilegesField);

    final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private Optional<Future<?>> ingestCorpUserInfo(
      UpgradeContext context, Urn urn, AuditStamp auditStamp) {
    EntityResponse entityResponse = null;
    try {
      entityResponse =
          entityService.getEntityV2(
              context.opContext(),
              urn.getEntityType(),
              urn,
              Collections.singleton(CORP_USER_INFO_ASPECT_NAME),
              true);
    } catch (URISyntaxException e) {
      log.error(
          "Error getting Corp User Info for entity with urn {} while restating user information",
          urn,
          e);
    }

    if (entityResponse != null
        && entityResponse.getAspects().containsKey(CORP_USER_INFO_ASPECT_NAME)) {
      final DataMap dataMap =
          entityResponse.getAspects().get(CORP_USER_INFO_ASPECT_NAME).getValue().data();
      final CorpUserInfo infoAspect = new CorpUserInfo(dataMap);

      log.debug("Restating corp user information for urn {} with value {}", urn, infoAspect);
      return Optional.of(
          entityService
              .alwaysProduceMCLAsync(
                  context.opContext(),
                  urn,
                  urn.getEntityType(),
                  CORP_USER_INFO_ASPECT_NAME,
                  opContext.getEntityRegistry().getAspectSpecs().get(CORP_USER_INFO_ASPECT_NAME),
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
}
