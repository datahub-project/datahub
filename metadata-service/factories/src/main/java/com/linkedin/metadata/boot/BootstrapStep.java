package com.linkedin.metadata.boot;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** A single step in the Bootstrap process. */
public interface BootstrapStep {

  /** A human-readable name for the boot step. */
  String name();

  /** Execute a boot-time step, or throw an exception on failure. */
  void execute(@Nonnull OperationContext systemOperationContext) throws Exception;

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

  static Optional<DataHubUpgradeResult> getUpgradeResult(
      @Nonnull OperationContext opContext, Urn urn, EntityService<?> entityService) {
    RecordTemplate recordTemplate =
        entityService.getLatestAspect(
            opContext, urn, Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    return Optional.ofNullable(recordTemplate).map(rt -> new DataHubUpgradeResult(rt.data()));
  }

  static void setUpgradeResult(
      @Nonnull OperationContext opContext, Urn urn, EntityService<?> entityService) {
    setUpgradeResult(opContext, urn, entityService, null);
  }

  static void setUpgradeResult(
      @Nonnull OperationContext opContext,
      Urn urn,
      EntityService<?> entityService,
      @Nullable Map<String, String> result) {
    setUpgradeResult(opContext, urn, entityService, DataHubUpgradeState.SUCCEEDED, result);
  }

  static void setUpgradeResult(
      @Nonnull OperationContext opContext,
      Urn urn,
      EntityService<?> entityService,
      @Nullable DataHubUpgradeState state,
      @Nullable Map<String, String> result) {
    final DataHubUpgradeResult upgradeResult =
        new DataHubUpgradeResult()
            .setTimestampMs(System.currentTimeMillis())
            .setState(state, SetMode.IGNORE_NULL)
            .setResult(result != null ? new StringMap(result) : null, SetMode.IGNORE_NULL);

    // Ingest the upgrade result
    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(urn);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(upgradeResult));
    upgradeProposal.setChangeType(ChangeType.UPSERT);
    entityService.ingestProposal(
        opContext, upgradeProposal, AuditStampUtils.createDefaultAuditStamp(), false);
  }
}
