package com.linkedin.datahub.upgrade.system.ownershiptypes;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OwnershipTypesStep implements UpgradeStep {

  private static final String UPGRADE_ID = OwnershipTypes.class.getSimpleName();
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

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
          Constants.ML_FEATURE_ENTITY_NAME,
          Constants.ML_PRIMARY_KEY_ENTITY_NAME,
          Constants.GLOSSARY_TERM_ENTITY_NAME,
          Constants.GLOSSARY_NODE_ENTITY_NAME,
          Constants.TAG_ENTITY_NAME,
          Constants.ROLE_ENTITY_NAME,
          Constants.CORP_GROUP_ENTITY_NAME,
          Constants.CORP_USER_ENTITY_NAME,
          Constants.CONTAINER_ENTITY_NAME,
          Constants.DOMAIN_ENTITY_NAME,
          Constants.DATA_PRODUCT_ENTITY_NAME,
          Constants.NOTEBOOK_ENTITY_NAME);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final boolean enabled;
  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public OwnershipTypesStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.enabled = enabled;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

      String scrollId = null;
      for (String entityType : ENTITY_TYPES_TO_MIGRATE) {
        int migratedCount = 0;
        do {
          log.info(
              String.format(
                  "Upgrading batch %s-%s of browse paths for entity type %s",
                  migratedCount, migratedCount + batchSize, entityType));
          scrollId = ownershipTypes(entityType, auditStamp, scrollId);
          migratedCount += batchSize;
        } while (scrollId != null);
      }

      BootstrapStep.setUpgradeResult(UPGRADE_ID_URN, entityService);

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private String ownershipTypes(String entityType, AuditStamp auditStamp, String scrollId) {

    final Filter filter;

    if (reprocessEnabled) {
      filter = backfillDefaultOwnershipTypesFilter();
    } else {
      filter = backfillOwnershipTypesFilter();
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

    try {
      ingestOwnershipTypes(scrollResult.getEntities(), auditStamp);
    } catch (Exception e) {
      // don't stop the whole step because of one bad urn or one bad ingestion
      log.error(
          String.format(
              "Error ingesting ownership aspect for urn %s",
              scrollResult.getEntities().stream()
                  .map(SearchEntity::getEntity)
                  .collect(Collectors.toList())),
          e);
    }

    return scrollResult.getScrollId();
  }

  private Filter backfillOwnershipTypesFilter() {
    // Condition: has `owners` AND does NOT have `ownershipTypes`
    Criterion hasOwners = new Criterion();
    hasOwners.setCondition(Condition.EXISTS);
    hasOwners.setField("owners");
    // Excludes entities with ownershipTypes
    Criterion missingOwnershipTypes = new Criterion();
    missingOwnershipTypes.setCondition(Condition.IS_NULL);
    missingOwnershipTypes.setField("ownershipTypes");

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(hasOwners);
    criterionArray.add(missingOwnershipTypes);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private Filter backfillDefaultOwnershipTypesFilter() {
    // Condition: has `owners`
    Criterion hasOwners = new Criterion();
    hasOwners.setCondition(Condition.EXISTS);
    hasOwners.setField("owners");

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(hasOwners);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);
    return filter;
  }

  private void ingestOwnershipTypes(SearchEntityArray searchBatch, AuditStamp auditStamp)
      throws Exception {
    Map<Urn, Map<String, Aspect>> existing =
        entityService.getLatestAspectObjects(
            searchBatch.stream().map(SearchEntity::getEntity).collect(Collectors.toSet()),
            Set.of(Constants.OWNERSHIP_ASPECT_NAME));

    List<MetadataChangeProposal> mcps =
        existing.entrySet().stream()
            .filter(result -> result.getValue().containsKey(Constants.OWNERSHIP_ASPECT_NAME))
            .map(
                result -> {
                  MetadataChangeProposal proposal = new MetadataChangeProposal();
                  proposal.setEntityUrn(result.getKey());
                  proposal.setEntityType(result.getKey().getEntityType());
                  proposal.setAspectName(Constants.OWNERSHIP_ASPECT_NAME);
                  proposal.setChangeType(ChangeType.UPSERT);
                  proposal.setSystemMetadata(
                      new SystemMetadata()
                          .setRunId(DEFAULT_RUN_ID)
                          .setLastObserved(System.currentTimeMillis()));
                  proposal.setAspect(
                      GenericRecordUtils.serializeAspect(
                          result.getValue().get(Constants.OWNERSHIP_ASPECT_NAME)));
                  return proposal;
                })
            .collect(Collectors.toList());

    log.debug(String.format("Reingesting ownership for %s urns", mcps.size()));
    AspectsBatch batch = AspectsBatchImpl.builder().mcps(mcps, auditStamp, entityService).build();

    entityService.ingestProposal(batch, false);
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
   * variables to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled && enabled) {
      return false;
    }

    boolean previouslyRun =
        entityService.exists(UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return (previouslyRun || !enabled);
  }
}
