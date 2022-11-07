package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import java.util.HashMap;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class RemoveClientIdAspectStep implements BootstrapStep {

  private final EntityService _entityService;

  private static final String VERSION = "0";
  private static final String UPGRADE_ID = "remove-unknown-aspects";
  private static final String INVALID_TELEMETRY_ASPECT_NAME = "clientId";
  private static final Urn REMOVE_UNKNOWN_ASPECTS_URN =
      EntityKeyUtils.convertEntityKeyToUrn(new DataHubUpgradeKey().setId(UPGRADE_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute() throws Exception {
    try {
      if (_entityService.exists(REMOVE_UNKNOWN_ASPECTS_URN)) {
        log.info("Unknown aspects have been removed. Skipping...");
        return;
      }
      // Remove invalid telemetry aspect
      _entityService.deleteAspect(TelemetryUtils.CLIENT_ID_URN, INVALID_TELEMETRY_ASPECT_NAME, new HashMap<>(), true);

      final AuditStamp auditStamp = new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
      final DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis());
      ingestUpgradeAspect(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, upgradeResult, auditStamp);
    } catch (Exception e) {
      log.error("Error when running the RemoveUnknownAspects Bootstrap Step", e);
      _entityService.deleteUrn(REMOVE_UNKNOWN_ASPECTS_URN);
      throw new RuntimeException("Error when running the RemoveUnknownAspects Bootstrap Step", e);
    }
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private void ingestUpgradeAspect(String aspectName, RecordTemplate aspect, AuditStamp auditStamp) {
    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(REMOVE_UNKNOWN_ASPECTS_URN);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(aspectName);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(upgradeProposal, auditStamp, false);
  }

}
