package com.linkedin.datahub.upgrade.system.dataplatforms;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexDataPlatformsStep implements UpgradeStep {

  // STEP_ID and VERSION must match the values used when this step ran as a GMS boot step.
  // The idempotency check derives a URN from STEP_ID — changing either value causes all existing
  // deployments to re-run the migration because the prior completion record won't be found.
  private static final String STEP_ID = "index-data-platforms";
  private static final String VERSION = "1";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final EntitySearchService _entitySearchService;
  private final Urn _upgradeUrn;

  public IndexDataPlatformsStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final EntitySearchService entitySearchService) {
    _entityService = entityService;
    _entitySearchService = entitySearchService;
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
              Collections.singleton(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME));
      if (response != null
          && response.getAspects().containsKey(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME)) {
        log.info("Step {} already completed. Skipping.", STEP_ID);
        return true;
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
        log.error("Failed to index data platforms", e);
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
    final AspectSpec dataPlatformSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.DATA_PLATFORM_ENTITY_NAME)
            .getAspectSpec(Constants.DATA_PLATFORM_INFO_ASPECT_NAME);

    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    getAndReIndexDataPlatforms(systemOperationContext, auditStamp, dataPlatformSpec);

    log.info("Successfully indexed data platform aspects");
  }

  private int getAndReIndexDataPlatforms(
      @Nonnull OperationContext opContext,
      AuditStamp auditStamp,
      AspectSpec dataPlatformInfoAspectSpec)
      throws Exception {
    int start = 0;
    int processed = 0;
    ListUrnsResult listResult;

    do {
      listResult =
          _entityService.listUrns(
              opContext, Constants.DATA_PLATFORM_ENTITY_NAME, start, BATCH_SIZE);
      List<Urn> dataPlatformUrns = listResult.getEntities();

      if (dataPlatformUrns.isEmpty()) {
        break;
      }

      final Map<Urn, EntityResponse> dataPlatformInfoResponses =
          _entityService.getEntitiesV2(
              opContext,
              Constants.DATA_PLATFORM_ENTITY_NAME,
              new HashSet<>(dataPlatformUrns),
              Collections.singleton(Constants.DATA_PLATFORM_INFO_ASPECT_NAME));

      List<Future<?>> futures = new LinkedList<>();
      for (Urn dpUrn : dataPlatformUrns) {
        EntityResponse dataPlatformEntityResponse = dataPlatformInfoResponses.get(dpUrn);
        if (dataPlatformEntityResponse == null) {
          log.warn("Data Platform not in set of entity responses {}", dpUrn);
          continue;
        }

        DataPlatformInfo dpInfo = mapDpInfo(dataPlatformEntityResponse);
        if (dpInfo == null) {
          log.warn("Received null dataPlatformInfo aspect for urn {}", dpUrn);
          continue;
        }

        futures.add(
            _entityService
                .alwaysProduceMCLAsync(
                    opContext,
                    dpUrn,
                    Constants.DATA_PLATFORM_ENTITY_NAME,
                    Constants.DATA_PLATFORM_INFO_ASPECT_NAME,
                    dataPlatformInfoAspectSpec,
                    null,
                    dpInfo,
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

      processed += dataPlatformUrns.size();
      start += BATCH_SIZE;
    } while (start < listResult.getTotal());

    return processed;
  }

  private DataPlatformInfo mapDpInfo(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)) {
      return null;
    }

    return new DataPlatformInfo(
        aspectMap.get(Constants.DATA_PLATFORM_INFO_ASPECT_NAME).getValue().data());
  }
}
