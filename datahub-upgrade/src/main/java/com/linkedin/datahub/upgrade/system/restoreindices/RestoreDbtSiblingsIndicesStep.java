package com.linkedin.datahub.upgrade.system.restoreindices;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreDbtSiblingsIndicesStep implements UpgradeStep {

  private static final String VERSION = "0";
  private static final String UPGRADE_ID = "restore-dbt-siblings-indices";
  private static final Urn SIBLING_UPGRADE_URN =
      EntityKeyUtils.convertEntityKeyToUrn(
          new DataHubUpgradeKey().setId(UPGRADE_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final AspectDao _aspectDao;
  private final boolean _enabled;

  RestoreDbtSiblingsIndicesStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final AspectDao aspectDao,
      final boolean enabled) {
    _entityService = entityService;
    _aspectDao = aspectDao;
    _enabled = enabled;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    if (!_enabled) {
      log.info("RestoreDbtSiblingsIndices is disabled. Skipping.");
      return true;
    }
    boolean previouslyRun =
        _entityService.exists(
            context.opContext(),
            SIBLING_UPGRADE_URN,
            Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME,
            true);
    if (previouslyRun) {
      log.info("RestoreDbtSiblingsIndices has run before. Skipping.");
    }
    return previouslyRun;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      log.info("Attempting to run RestoreDbtSiblingsIndices upgrade..");
      try {
        final AuditStamp auditStamp =
            new AuditStamp()
                .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis());

        ingestUpgradeAspect(
            context.opContext(),
            Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME,
            new DataHubUpgradeRequest()
                .setTimestampMs(System.currentTimeMillis())
                .setVersion(VERSION),
            auditStamp);

        RestoreIndicesStreamUtil.reindexAspect(
            context.opContext(),
            _entityService,
            _aspectDao,
            Constants.UPSTREAM_LINEAGE_ASPECT_NAME,
            "urn:li:" + Constants.DATASET_ENTITY_NAME + ":%",
            BATCH_SIZE,
            0,
            0,
            ChangeType.RESTATE);

        ingestUpgradeAspect(
            context.opContext(),
            Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME,
            new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis()),
            auditStamp);

        log.info("Successfully restored sibling aspects");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Error when running the RestoreDbtSiblingsIndices Bootstrap Step", e);
        _entityService.deleteUrn(context.opContext(), SIBLING_UPGRADE_URN);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void ingestUpgradeAspect(
      @Nonnull OperationContext systemOperationContext,
      String aspectName,
      RecordTemplate aspect,
      AuditStamp auditStamp) {
    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(SIBLING_UPGRADE_URN);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(aspectName);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(systemOperationContext, upgradeProposal, auditStamp, false);
  }
}
