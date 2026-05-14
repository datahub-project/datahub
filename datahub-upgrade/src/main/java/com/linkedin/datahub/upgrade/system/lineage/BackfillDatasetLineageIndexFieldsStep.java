package com.linkedin.datahub.upgrade.system.lineage;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildConjunctiveCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNotNullCriterion;
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
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
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

/** This bootstrap step is responsible for backfilling dataset lineage index fields in ES */
@Slf4j
public class BackfillDatasetLineageIndexFieldsStep implements UpgradeStep {
  private static final String UPGRADE_ID = "BackfillDatasetLineageIndexFieldsStep_V1";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext opContext;
  private final boolean reprocessEnabled;
  private final Integer batchSize;
  private final EntityService<?> entityService;
  private final SearchService _searchService;

  public BackfillDatasetLineageIndexFieldsStep(
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
        log.info(
            "Backfilling lineage index fields for batch of datasets {}-{}",
            migratedCount,
            migratedCount + batchSize);
        scrollId = backfillDatasetLineageFields(context, auditStamp, scrollId);
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
   * variable to determine whether to skip.
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

  private String backfillDatasetLineageFields(
      UpgradeContext context, AuditStamp auditStamp, String scrollId) {

    final Filter filter = backfillLineageFieldFilter();
    final ScrollResult scrollResult =
        _searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            ImmutableList.of(Constants.DATASET_ENTITY_NAME),
            "*",
            filter,
            null,
            scrollId,
            null,
            batchSize,
            null);

    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().isEmpty()) {
      return null;
    }

    List<Future<?>> futures = new LinkedList<>();
    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        restateUpstreamLineage(context, searchEntity.getEntity(), auditStamp)
            .ifPresent(futures::add);
      } catch (Exception e) {
        // don't stop the whole step because of one bad urn or one bad ingestion
        log.error("Error restating upstreamLineage aspect for urn {}", searchEntity.getEntity(), e);
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

  private Filter backfillLineageFieldFilter() {
    // Condition: Has source data (`fineGrainedUpstreams` IS NOT NULL) AND missing at least one
    // derived field (`hasUpstreams` IS NULL OR `hasFineGrainedUpstreams` IS NULL)
    //
    // Using DNF (Disjunctive Normal Form):
    // (fineGrainedUpstreams IS NOT NULL AND hasUpstreams IS NULL) OR
    // (fineGrainedUpstreams IS NOT NULL AND hasFineGrainedUpstreams IS NULL)
    //
    // This ensures we only process datasets WITH lineage data, avoiding wasteful fetches
    // of entities that have no lineage to backfill.
    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();

    // Condition 1: Has source data AND missing hasUpstreams
    conjunctiveCriterionArray.add(
        buildConjunctiveCriterion(
            buildIsNotNullCriterion("fineGrainedUpstreams"), buildIsNullCriterion("hasUpstreams")));

    // Condition 2: Has source data AND missing hasFineGrainedUpstreams
    conjunctiveCriterionArray.add(
        buildConjunctiveCriterion(
            buildIsNotNullCriterion("fineGrainedUpstreams"),
            buildIsNullCriterion("hasFineGrainedUpstreams")));

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private Optional<Future<?>> restateUpstreamLineage(
      UpgradeContext context, Urn urn, AuditStamp auditStamp) {
    EntityResponse entityResponse = null;
    try {
      entityResponse =
          entityService.getEntityV2(
              context.opContext(),
              urn.getEntityType(),
              urn,
              Collections.singleton(UPSTREAM_LINEAGE_ASPECT_NAME));
    } catch (URISyntaxException e) {
      log.error(
          "Error getting UpstreamLineage for entity with urn {} while restating lineage information",
          urn,
          e);
      ;
    }

    if (entityResponse != null
        && entityResponse.getAspects().containsKey(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      final DataMap dataMap =
          entityResponse.getAspects().get(UPSTREAM_LINEAGE_ASPECT_NAME).getValue().data();
      final UpstreamLineage upstreamLineage = new UpstreamLineage(dataMap);

      log.debug("Restating upstreamLineage for dataset urn {} with value {}", urn, upstreamLineage);
      return Optional.of(
          entityService
              .alwaysProduceMCLAsync(
                  context.opContext(),
                  urn,
                  urn.getEntityType(),
                  UPSTREAM_LINEAGE_ASPECT_NAME,
                  opContext.getEntityRegistry().getAspectSpecs().get(UPSTREAM_LINEAGE_ASPECT_NAME),
                  null,
                  upstreamLineage,
                  null,
                  createDefaultSystemMetadata(),
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    return Optional.empty();
  }
}
