package com.linkedin.metadata.boot;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

/** A single step in the Bootstrap process. */
public interface BootstrapStep {

  /** A human-readable name for the boot step. */
  String name();

  /** Execute a boot-time step, or throw an exception on failure. */
  void execute() throws Exception;

  /** Return the execution mode of this step */
  @Nonnull
  default ExecutionMode getExecutionMode() {
    return ExecutionMode.BLOCKING;
  }

  enum ExecutionMode {
    // Block service from starting up while running the step
    BLOCKING,
    // Start the step asynchronously without waiting for it to end
    ASYNC,
  }

  static Urn getUpgradeUrn(String upgradeId) {
    return EntityKeyUtils.convertEntityKeyToUrn(
        new DataHubUpgradeKey().setId(upgradeId), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  }

  static void setUpgradeResult(Urn urn, EntityService entityService) throws URISyntaxException {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final DataHubUpgradeResult upgradeResult =
        new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis());

    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(urn);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(upgradeResult));
    upgradeProposal.setChangeType(ChangeType.UPSERT);
    entityService.ingestProposal(upgradeProposal, auditStamp, false);
  }
}
