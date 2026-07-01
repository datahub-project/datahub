package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.restoreindices.RestoreIndicesStreamUtil;
import com.linkedin.entity.EntityResponse;
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
import java.util.Collections;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreGlossaryIndicesStep implements UpgradeStep {

  // STEP_ID and VERSION must match the values used when this step ran as a GMS boot step.
  // The idempotency check derives a URN from STEP_ID — changing either value causes all existing
  // deployments to re-run the migration because the prior completion record won't be found.
  private static final String STEP_ID = "restore-glossary-indices-ui";
  private static final String VERSION = "1";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final AspectDao _aspectDao;
  private final Urn _upgradeUrn;

  public RestoreGlossaryIndicesStep(
      @Nonnull final EntityService<?> entityService, @Nonnull final AspectDao aspectDao) {
    _entityService = entityService;
    _aspectDao = aspectDao;
    _upgradeUrn =
        EntityKeyUtils.convertEntityKeyToUrn(
            new DataHubUpgradeKey().setId(STEP_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    try {
      EntityResponse response =
          _entityService.getEntityV2(
              context.opContext(),
              Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
              _upgradeUrn,
              Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)) {
        DataMap dataMap =
            response
                .getAspects()
                .get(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
                .getValue()
                .data();
        DataHubUpgradeRequest request = new DataHubUpgradeRequest(dataMap);
        if (request.hasVersion() && request.getVersion().equals(VERSION)) {
          log.info("Step {} version {} already completed. Skipping.", STEP_ID, VERSION);
          return true;
        }
      }
    } catch (Exception e) {
      log.error("Error checking upgrade history for {}. Proceeding with upgrade.", STEP_ID, e);
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        ingestUpgradeRequest(context.opContext());
        execute(context.opContext());
        ingestUpgradeResult(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to restore glossary indices", e);
        _entityService.deleteUrn(context.opContext(), _upgradeUrn);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void ingestUpgradeRequest(@Nonnull final OperationContext opContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(_upgradeUrn);
    proposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new DataHubUpgradeRequest()
                .setTimestampMs(System.currentTimeMillis())
                .setVersion(VERSION)));
    proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    _entityService.ingestProposal(opContext, proposal, auditStamp, false);
  }

  private void ingestUpgradeResult(@Nonnull final OperationContext opContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(_upgradeUrn);
    proposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(
            new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis())));
    proposal.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    _entityService.ingestProposal(opContext, proposal, auditStamp, false);
  }

  /**
   * Re-emits glossaryTermInfo and glossaryNodeInfo aspects to rebuild their search documents. Uses
   * RESTATE (as the original scan did). Enumeration now streams from the primary store rather than
   * the search index, so terms/nodes missing from the index are also covered.
   */
  private void execute(@Nonnull final OperationContext systemOperationContext) {
    RestoreIndicesStreamUtil.reindexAspect(
        systemOperationContext,
        _entityService,
        _aspectDao,
        Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
        "urn:li:" + Constants.GLOSSARY_TERM_ENTITY_NAME + ":%",
        BATCH_SIZE,
        0,
        0,
        ChangeType.RESTATE);
    RestoreIndicesStreamUtil.reindexAspect(
        systemOperationContext,
        _entityService,
        _aspectDao,
        Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
        "urn:li:" + Constants.GLOSSARY_NODE_ENTITY_NAME + ":%",
        BATCH_SIZE,
        0,
        0,
        ChangeType.RESTATE);
  }
}
