package com.linkedin.datahub.upgrade.system.restoreindices;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
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
public class RestoreDbtSiblingsIndicesStep implements UpgradeStep {

  private static final String VERSION = "0";
  private static final String UPGRADE_ID = "restore-dbt-siblings-indices";
  private static final Urn SIBLING_UPGRADE_URN =
      EntityKeyUtils.convertEntityKeyToUrn(
          new DataHubUpgradeKey().setId(UPGRADE_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  private static final Integer BATCH_SIZE = 1000;
  static final Integer SLEEP_SECONDS = 120;

  private final EntityService<?> _entityService;
  private final boolean _enabled;
  private final int _sleepSeconds;

  public RestoreDbtSiblingsIndicesStep(
      @Nonnull final EntityService<?> entityService, final boolean enabled) {
    this(entityService, enabled, SLEEP_SECONDS);
  }

  // Package-private constructor used in tests to avoid the deployment-wait sleep
  RestoreDbtSiblingsIndicesStep(
      @Nonnull final EntityService<?> entityService,
      final boolean enabled,
      final int sleepSeconds) {
    _entityService = entityService;
    _enabled = enabled;
    _sleepSeconds = sleepSeconds;
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
      log.info(String.format("Waiting %s seconds..", _sleepSeconds));

      try {
        // Sleep to ensure deployment process finishes.
        Thread.sleep(_sleepSeconds * 1000L);

        log.info("Bootstrapping sibling aspects");

        final int rowCount =
            _entityService.listUrns(context.opContext(), DATASET_ENTITY_NAME, 0, 10).getTotal();

        log.info("Found {} dataset entities to attempt to bootstrap", rowCount);

        final AspectSpec datasetAspectSpec =
            context
                .opContext()
                .getEntityRegistry()
                .getEntitySpec(Constants.DATASET_ENTITY_NAME)
                .getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
        final AuditStamp auditStamp =
            new AuditStamp()
                .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis());

        final DataHubUpgradeRequest upgradeRequest =
            new DataHubUpgradeRequest()
                .setTimestampMs(System.currentTimeMillis())
                .setVersion(VERSION);
        ingestUpgradeAspect(
            context.opContext(),
            Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME,
            upgradeRequest,
            auditStamp);

        int indexedCount = 0;
        while (indexedCount < rowCount) {
          getAndRestoreUpstreamLineageIndices(
              context.opContext(), indexedCount, auditStamp, datasetAspectSpec);
          indexedCount += BATCH_SIZE;
        }

        final DataHubUpgradeResult upgradeResult =
            new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis());
        ingestUpgradeAspect(
            context.opContext(),
            Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME,
            upgradeResult,
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

  private void getAndRestoreUpstreamLineageIndices(
      @Nonnull OperationContext systemOperationContext,
      int start,
      AuditStamp auditStamp,
      AspectSpec upstreamAspectSpec) {
    ListUrnsResult datasetUrnsResult =
        _entityService.listUrns(systemOperationContext, DATASET_ENTITY_NAME, start, BATCH_SIZE);
    List<Urn> datasetUrns = datasetUrnsResult.getEntities();
    log.info("Re-indexing upstreamLineage aspect from {} with batch size {}", start, BATCH_SIZE);

    if (datasetUrns.size() == 0) {
      return;
    }

    final Map<Urn, EntityResponse> upstreamLineageResponse;
    try {
      upstreamLineageResponse =
          _entityService.getEntitiesV2(
              systemOperationContext,
              DATASET_ENTITY_NAME,
              new HashSet<>(datasetUrns),
              Collections.singleton(UPSTREAM_LINEAGE_ASPECT_NAME));
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Error fetching upstream lineage history: %s", e.toString()));
    }

    //  Loop over datasets and produce changelog
    List<Future<?>> futures = new LinkedList<>();
    for (Urn datasetUrn : datasetUrns) {
      EntityResponse response = upstreamLineageResponse.get(datasetUrn);
      if (response == null) {
        log.warn("Dataset not in set of entity responses {}", datasetUrn);
        continue;
      }
      UpstreamLineage upstreamLineage = getUpstreamLineage(response);
      if (upstreamLineage == null) {
        continue;
      }

      futures.add(
          _entityService
              .alwaysProduceMCLAsync(
                  systemOperationContext,
                  datasetUrn,
                  DATASET_ENTITY_NAME,
                  UPSTREAM_LINEAGE_ASPECT_NAME,
                  upstreamAspectSpec,
                  null,
                  upstreamLineage,
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
  }

  private UpstreamLineage getUpstreamLineage(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      return null;
    }

    return new UpstreamLineage(
        aspectMap.get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME).getValue().data());
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
