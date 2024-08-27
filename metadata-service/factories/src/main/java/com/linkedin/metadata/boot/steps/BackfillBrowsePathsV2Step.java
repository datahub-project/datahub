package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.utils.DefaultAspectsUtil;
import com.linkedin.metadata.boot.UpgradeStep;
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
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackfillBrowsePathsV2Step extends UpgradeStep {

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
  private static final String VERSION = "2";
  private static final String UPGRADE_ID = "backfill-default-browse-paths-v2-step";
  private static final Integer BATCH_SIZE = 5000;

  private final SearchService searchService;

  public BackfillBrowsePathsV2Step(EntityService<?> entityService, SearchService searchService) {
    super(entityService, VERSION, UPGRADE_ID);
    this.searchService = searchService;
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

    String scrollId = null;
    for (String entityType : ENTITY_TYPES_TO_MIGRATE) {
      int migratedCount = 0;
      do {
        log.info(
            String.format(
                "Upgrading batch %s-%s of browse paths for entity type %s",
                migratedCount, migratedCount + BATCH_SIZE, entityType));
        scrollId = backfillBrowsePathsV2(systemOpContext, entityType, auditStamp, scrollId);
        migratedCount += BATCH_SIZE;
      } while (scrollId != null);
    }
  }

  private String backfillBrowsePathsV2(
      @Nonnull OperationContext systemOperationContext,
      String entityType,
      AuditStamp auditStamp,
      String scrollId)
      throws Exception {

    // Condition: has `browsePaths` AND does NOT have `browsePathV2`
    Criterion missingBrowsePathV2 = new Criterion();
    missingBrowsePathV2.setCondition(Condition.IS_NULL);
    missingBrowsePathV2.setField("browsePathV2");
    // Excludes entities without browsePaths
    Criterion hasBrowsePathV1 = new Criterion();
    hasBrowsePathV1.setCondition(Condition.EXISTS);
    hasBrowsePathV1.setField("browsePaths");

    CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(missingBrowsePathV2);
    criterionArray.add(hasBrowsePathV1);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);

    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            systemOperationContext,
            ImmutableList.of(entityType),
            "*",
            filter,
            null,
            scrollId,
            "5m",
            BATCH_SIZE);
    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().size() == 0) {
      return null;
    }

    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        ingestBrowsePathsV2(systemOperationContext, searchEntity.getEntity(), auditStamp);
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

  private void ingestBrowsePathsV2(
      @Nonnull OperationContext systemOperationContext, Urn urn, AuditStamp auditStamp)
      throws Exception {
    BrowsePathsV2 browsePathsV2 =
        DefaultAspectsUtil.buildDefaultBrowsePathV2(
            systemOperationContext, urn, true, entityService);
    log.debug(String.format("Adding browse path v2 for urn %s with value %s", urn, browsePathsV2));
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    proposal.setAspect(GenericRecordUtils.serializeAspect(browsePathsV2));
    entityService.ingestProposal(systemOperationContext, proposal, auditStamp, false);
  }
}
