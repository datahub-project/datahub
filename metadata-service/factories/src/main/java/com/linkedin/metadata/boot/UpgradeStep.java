package com.linkedin.metadata.boot;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class UpgradeStep implements BootstrapStep {

  protected final EntityService<?> entityService;
  private final String version;
  private final String upgradeId;
  private final Urn upgradeUrn;

  public UpgradeStep(EntityService<?> entityService, String version, String upgradeId) {
    this.entityService = entityService;
    this.version = version;
    this.upgradeId = upgradeId;
    this.upgradeUrn =
        EntityKeyUtils.convertEntityKeyToUrn(
            new DataHubUpgradeKey().setId(upgradeId), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext) throws Exception {

    if (hasUpgradeRan(systemOperationContext)) {
      log.info(String.format("%s has run before for version %s. Skipping..", upgradeId, version));
      return;
    }

    try {
      ingestUpgradeRequestAspect(systemOperationContext);
      upgrade(systemOperationContext);
      ingestUpgradeResultAspect(systemOperationContext);
    } catch (Exception e) {
      String errorMessage =
          String.format("Error when running %s for version %s", upgradeId, version);
      cleanUpgradeAfterError(systemOperationContext, e, errorMessage);
      throw new RuntimeException(errorMessage, e);
    }
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  public abstract void upgrade(@Nonnull OperationContext systemOperationContext) throws Exception;

  private boolean hasUpgradeRan(@Nonnull OperationContext systemOperationContext) {
    try {
      EntityResponse response =
          entityService.getEntityV2(
              systemOperationContext,
              Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
              upgradeUrn,
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
        if (request.hasVersion() && request.getVersion().equals(version)) {
          return true;
        }
      }
    } catch (Exception e) {
      log.error(
          "Error when checking to see if datahubUpgrade entity exists. Commencing with upgrade...",
          e);
      return false;
    }
    return false;
  }

  private void ingestUpgradeRequestAspect(@Nonnull OperationContext systemOperationContext)
      throws URISyntaxException {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final DataHubUpgradeRequest upgradeRequest =
        new DataHubUpgradeRequest().setTimestampMs(System.currentTimeMillis()).setVersion(version);

    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(upgradeUrn);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(upgradeRequest));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    entityService.ingestProposal(systemOperationContext, upgradeProposal, auditStamp, false);
  }

  private void ingestUpgradeResultAspect(@Nonnull OperationContext systemOperationContext)
      throws URISyntaxException {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final DataHubUpgradeResult upgradeResult =
        new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis());

    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(upgradeUrn);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(upgradeResult));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    entityService.ingestProposal(systemOperationContext, upgradeProposal, auditStamp, false);
  }

  private void cleanUpgradeAfterError(
      @Nonnull OperationContext systemOperationContext, Exception e, String errorMessage) {
    log.error(errorMessage, e);
    entityService.deleteUrn(systemOperationContext, upgradeUrn);
  }
}
