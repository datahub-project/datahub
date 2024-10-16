package com.linkedin.datahub.upgrade.system.browsepaths;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.utils.DefaultAspectsUtil;
import com.linkedin.metadata.boot.BootstrapStep;
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
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackfillBrowsePathsV2Step implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillBrowsePathsV2Step";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  public static final String DEFAULT_BROWSE_PATH_V2 = "␟Default";

  private static final Set<String> ENTITY_TYPES_TO_MIGRATE =
      ImmutableSet.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.DATA_JOB_ENTITY_NAME,
          Constants.DATA_FLOW_ENTITY_NAME,
          Constants.ML_MODEL_ENTITY_NAME,
          Constants.ML_MODEL_GROUP_ENTITY_NAME,
          Constants.ML_FEATURE_TABLE_ENTITY_NAME,
          Constants.ML_FEATURE_ENTITY_NAME);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;

  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public BackfillBrowsePathsV2Step(
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
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      String scrollId = null;
      for (String entityType : ENTITY_TYPES_TO_MIGRATE) {
        int migratedCount = 0;
        do {
          log.info(
              String.format(
                  "Upgrading batch %s-%s of browse paths for entity type %s",
                  migratedCount, migratedCount + batchSize, entityType));
          scrollId = backfillBrowsePathsV2(entityType, auditStamp, scrollId);
          migratedCount += batchSize;
        } while (scrollId != null);
      }

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private String backfillBrowsePathsV2(String entityType, AuditStamp auditStamp, String scrollId) {

    final Filter filter;

    if (reprocessEnabled) {
      filter = backfillDefaultBrowsePathsV2Filter();
    } else {
      filter = backfillBrowsePathsV2Filter();
    }

    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            ImmutableList.of(entityType),
            "*",
            filter,
            null,
            scrollId,
            null,
            batchSize);

    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().size() == 0) {
      return null;
    }

    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        ingestBrowsePathsV2(opContext, searchEntity.getEntity(), auditStamp);
      } catch (Exception e) {
        // don't stop the whole step because of one bad urn or one bad ingestion
        log.error(
            String.format(
                "Error ingesting default browsePathsV2 aspect for urn %s",
                searchEntity.getEntity()),
            e);
      }
    }

    return scrollResult.getScrollId();
  }

  private Filter backfillBrowsePathsV2Filter() {
    // Condition: has `browsePaths` AND does NOT have `browsePathV2`
    Criterion missingBrowsePathV2 = buildIsNullCriterion("browsePathV2");

    // Excludes entities without browsePaths
    Criterion hasBrowsePathV1 = buildExistsCriterion("browsePaths");

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(missingBrowsePathV2);
    criterionArray.add(hasBrowsePathV1);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private Filter backfillDefaultBrowsePathsV2Filter() {
    // Condition: has default `browsePathV2`
    Criterion hasDefaultBrowsePathV2 =
        buildCriterion("browsePathV2", Condition.EQUAL, DEFAULT_BROWSE_PATH_V2);

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(hasDefaultBrowsePathV2);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private void ingestBrowsePathsV2(
      @Nonnull OperationContext opContext, Urn urn, AuditStamp auditStamp) throws Exception {
    BrowsePathsV2 browsePathsV2 =
        DefaultAspectsUtil.buildDefaultBrowsePathV2(opContext, urn, true, entityService);
    log.debug(String.format("Adding browse path v2 for urn %s with value %s", urn, browsePathsV2));
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    proposal.setAspect(GenericRecordUtils.serializeAspect(browsePathsV2));
    entityService.ingestProposal(opContext, proposal, auditStamp, true);
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variables REPROCESS_DEFAULT_BROWSE_PATHS_V2 & BACKFILL_BROWSE_PATHS_V2 to determine whether to
   * skip.
   */
  public boolean skip(UpgradeContext context) {
    boolean envEnabled = Boolean.parseBoolean(System.getenv("BACKFILL_BROWSE_PATHS_V2"));

    if (reprocessEnabled && envEnabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return (previouslyRun || !envEnabled);
  }
}
