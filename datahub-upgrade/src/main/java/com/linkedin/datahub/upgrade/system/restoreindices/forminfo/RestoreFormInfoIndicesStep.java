package com.linkedin.datahub.upgrade.system.restoreindices.forminfo;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreFormInfoIndicesStep implements UpgradeStep {

  // STEP_ID and VERSION must match the values used when this step ran as a GMS boot step.
  // The idempotency check derives a URN from STEP_ID — changing either value causes all existing
  // deployments to re-run the migration because the prior completion record won't be found.
  private static final String STEP_ID = "restore-form-info-indices";
  private static final String VERSION = "2";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final Urn _upgradeUrn;

  public RestoreFormInfoIndicesStep(@Nonnull final EntityService<?> entityService) {
    _entityService = entityService;
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
        log.error("Failed to restore form info indices", e);
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

  private void execute(@Nonnull final OperationContext systemOperationContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    final int totalFormCount = getAndRestoreFormInfoIndices(systemOperationContext, 0, auditStamp);
    int formCount = BATCH_SIZE;
    while (formCount < totalFormCount) {
      getAndRestoreFormInfoIndices(systemOperationContext, formCount, auditStamp);
      formCount += BATCH_SIZE;
    }
  }

  private int getAndRestoreFormInfoIndices(
      @Nonnull OperationContext systemOperationContext, int start, AuditStamp auditStamp) {
    final AspectSpec formInfoAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.FORM_ENTITY_NAME)
            .getAspectSpec(Constants.FORM_INFO_ASPECT_NAME);

    final ListResult<RecordTemplate> latestAspects =
        _entityService.listLatestAspects(
            systemOperationContext,
            Constants.FORM_ENTITY_NAME,
            Constants.FORM_INFO_ASPECT_NAME,
            start,
            BATCH_SIZE);

    if (latestAspects.getTotalCount() == 0
        || latestAspects.getValues() == null
        || latestAspects.getMetadata() == null) {
      log.debug("Found 0 formInfo aspects for forms. Skipping migration.");
      return 0;
    }

    if (latestAspects.getValues().size() != latestAspects.getMetadata().getExtraInfos().size()) {
      log.warn(
          "Failed to match formInfo aspects with corresponding urns. Found mismatched length between aspects ({})"
              + "and metadata ({}) for metadata {}",
          latestAspects.getValues().size(),
          latestAspects.getMetadata().getExtraInfos().size(),
          latestAspects.getMetadata());
      return latestAspects.getTotalCount();
    }

    List<Future<?>> futures = new LinkedList<>();
    for (int i = 0; i < latestAspects.getValues().size(); i++) {
      ExtraInfo info = latestAspects.getMetadata().getExtraInfos().get(i);
      RecordTemplate formInfoRecord = latestAspects.getValues().get(i);
      Urn urn = info.getUrn();
      FormInfo formInfo = (FormInfo) formInfoRecord;
      if (formInfo == null) {
        log.warn("Received null formInfo for urn {}", urn);
        continue;
      }

      futures.add(
          _entityService
              .alwaysProduceMCLAsync(
                  systemOperationContext,
                  urn,
                  Constants.FORM_ENTITY_NAME,
                  Constants.FORM_INFO_ASPECT_NAME,
                  formInfoAspectSpec,
                  null,
                  formInfo,
                  null,
                  null,
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    futures.stream()
        .filter(Objects::nonNull)
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    return latestAspects.getTotalCount();
  }
}
