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
import java.net.URISyntaxException;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class UpgradeStep implements BootstrapStep {
  private static final Integer SLEEP_SECONDS = 120;

  protected final EntityService _entityService;
  private final String _version;
  private final String _upgradeId;
  private final Urn _upgradeUrn;

  public UpgradeStep(EntityService entityService, String version, String upgradeId) {
    this._entityService = entityService;
    this._version = version;
    this._upgradeId = upgradeId;
    this._upgradeUrn = EntityKeyUtils.convertEntityKeyToUrn(new DataHubUpgradeKey().setId(upgradeId),
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  }

  @Override
  public void execute() throws Exception {
    String upgradeStepName = name();

    log.info(String.format("Attempting to run %s Upgrade Step..", upgradeStepName));
    log.info(String.format("Waiting %s seconds..", SLEEP_SECONDS));

    if (hasUpgradeRan()) {
      log.info(String.format("%s has run before for version %s. Skipping..", _upgradeId, _version));
      return;
    }

    // Sleep to ensure deployment process finishes.
    Thread.sleep(SLEEP_SECONDS * 1000);

    try {
      ingestUpgradeRequestAspect();
      upgrade();
      ingestUpgradeResultAspect();
    } catch (Exception e) {
      String errorMessage = String.format("Error when running %s for version %s", _upgradeId, _version);
      cleanUpgradeAfterError(e, errorMessage);
      throw new RuntimeException(errorMessage, e);
    }
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  public abstract void upgrade() throws Exception;

  private boolean hasUpgradeRan() {
    try {
      EntityResponse response = _entityService.getEntityV2(Constants.DATA_HUB_UPGRADE_ENTITY_NAME, _upgradeUrn,
          Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME));

      if (response != null && response.getAspects().containsKey(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)) {
        DataMap dataMap = response.getAspects().get(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME).getValue().data();
        DataHubUpgradeRequest request = new DataHubUpgradeRequest(dataMap);
        if (request.hasVersion() && request.getVersion().equals(_version)) {
          return true;
        }
      }
    } catch (Exception e) {
      log.error("Error when checking to see if datahubUpgrade entity exists. Commencing with upgrade...", e);
      return false;
    }
    return false;
  }

  private void ingestUpgradeRequestAspect() throws URISyntaxException {
    final AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
    final DataHubUpgradeRequest upgradeRequest =
        new DataHubUpgradeRequest().setTimestampMs(System.currentTimeMillis()).setVersion(_version);

    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(_upgradeUrn);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(upgradeRequest));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(upgradeProposal, auditStamp, false);
  }

  private void ingestUpgradeResultAspect() throws URISyntaxException {
    final AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
    final DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis());

    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(_upgradeUrn);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(upgradeResult));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(upgradeProposal, auditStamp, false);
  }

  private void cleanUpgradeAfterError(Exception e, String errorMessage) {
    log.error(errorMessage, e);
    _entityService.deleteUrn(_upgradeUrn);
  }
}
