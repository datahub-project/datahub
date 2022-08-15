package com.linkedin.metadata.boot.steps;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * This is an opt-in optional upgrade step to migrate your browse paths to the new truncated form.
 * It is idempotent, can be retried as many times as necessary.
 */
@Slf4j
public class UpgradeDefaultBrowsePathsStep extends UpgradeStep {

  private static final Set<String> ENTITY_TYPES_TO_MIGRATE = ImmutableSet.of(
      Constants.DATASET_ENTITY_NAME,
      Constants.DASHBOARD_ENTITY_NAME,
      Constants.CHART_ENTITY_NAME,
      Constants.DATA_JOB_ENTITY_NAME,
      Constants.DATA_FLOW_ENTITY_NAME
  );
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "upgrade-default-browse-paths-step";
  private static final Integer BATCH_SIZE = 5000;

  public UpgradeDefaultBrowsePathsStep(EntityService entityService) {
    super(entityService, VERSION, UPGRADE_ID);
  }

  @Override
  public void upgrade() throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

    int total = 0;
    for (String entityType : ENTITY_TYPES_TO_MIGRATE) {
      int migratedCount = 0;
      do {
        log.info(String.format("Upgrading batch %s-%s out of %s of browse paths for entity type %s",
            migratedCount, migratedCount + BATCH_SIZE, total, entityType));
        total = getAndMigrateBrowsePaths(entityType, migratedCount, auditStamp);
        migratedCount += BATCH_SIZE;
      } while (migratedCount < total);
    }
    log.info("Successfully upgraded all browse paths!");
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.BLOCKING; // ensure there are no write conflicts.
  }

  private int getAndMigrateBrowsePaths(String entityType, int start, AuditStamp auditStamp)
      throws Exception {

    final ListResult<RecordTemplate> latestAspects = _entityService.listLatestAspects(
        entityType,
        Constants.BROWSE_PATHS_ASPECT_NAME,
        start,
        BATCH_SIZE);

    if (latestAspects.getTotalCount() == 0 || latestAspects.getValues() == null || latestAspects.getMetadata() == null) {
      log.debug(String.format("Found 0 browse paths for entity with type %s. Skipping migration!", entityType));
      return 0;
    }

    if (latestAspects.getValues().size() != latestAspects.getMetadata().getExtraInfos().size()) {
      // Bad result -- we should log that we cannot migrate this batch of paths.
      log.warn("Failed to match browse path aspects with corresponding urns. Found mismatched length between aspects ({})"
          + "and metadata ({}) for metadata {}",
          latestAspects.getValues().size(),
          latestAspects.getMetadata().getExtraInfos().size(),
          latestAspects.getMetadata());
      return latestAspects.getTotalCount();
    }

    for (int i = 0; i < latestAspects.getValues().size(); i++) {

      ExtraInfo info = latestAspects.getMetadata().getExtraInfos().get(i);
      RecordTemplate browsePathsRec = latestAspects.getValues().get(i);

      // Assert on 2 conditions:
      // 1. The latest browse path aspect contains only 1 browse path
      // 2. The latest browse path matches exactly the legacy default path

      Urn urn = info.getUrn();
      BrowsePaths browsePaths = (BrowsePaths) browsePathsRec;

      log.debug(String.format("Inspecting browse path for urn %s, value %s", urn, browsePaths));

      if (browsePaths.hasPaths() && browsePaths.getPaths().size() == 1) {
        String legacyBrowsePath = BrowsePathUtils.getLegacyDefaultBrowsePath(urn, _entityService.getEntityRegistry());
        log.debug(String.format("Legacy browse path for urn %s, value %s", urn, legacyBrowsePath));
        if (legacyBrowsePath.equals(browsePaths.getPaths().get(0))) {
          migrateBrowsePath(urn, auditStamp);
        }
      }
    }

    return latestAspects.getTotalCount();
  }

  private void migrateBrowsePath(Urn urn, AuditStamp auditStamp) throws Exception {
    BrowsePaths newPaths = _entityService.buildDefaultBrowsePath(urn);
    log.debug(String.format("Updating browse path for urn %s to value %s", urn, newPaths));
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(Constants.BROWSE_PATHS_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(new SystemMetadata().setRunId(EntityService.DEFAULT_RUN_ID).setLastObserved(System.currentTimeMillis()));
    proposal.setAspect(GenericRecordUtils.serializeAspect(newPaths));
    _entityService.ingestProposal(
        proposal,
        auditStamp
      );
  }

}