package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class RestoreDbtSiblingsIndices implements BootstrapStep {
  private static final String VERSION = "0";
  private static final String UPGRADE_ID = "restore-dbt-siblings-indices";
  private static final Urn SIBLING_UPGRADE_URN =
      EntityKeyUtils.convertEntityKeyToUrn(new DataHubUpgradeKey().setId(UPGRADE_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  private static final Integer BATCH_SIZE = 1000;
  private static final Integer SLEEP_SECONDS = 120;

  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public void execute() throws Exception {
    log.info("Attempting to run RestoreDbtSiblingsIndices upgrade..");
    log.info(String.format("Waiting %s seconds..", SLEEP_SECONDS));

    EntityResponse response = _entityService.getEntityV2(
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME, SIBLING_UPGRADE_URN,
        Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
    );
    if (response != null && response.getAspects().containsKey(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)) {
      DataMap dataMap = response.getAspects().get(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME).getValue().data();
      DataHubUpgradeRequest request = new DataHubUpgradeRequest(dataMap);
      if (request.hasVersion() && request.getVersion().equals(VERSION)) {
        log.info("RestoreDbtSiblingsIndices has run before with this version. Skipping");
        return;
      }
    }

    // Sleep to ensure deployment process finishes.
    Thread.sleep(SLEEP_SECONDS * 1000);

    log.info("Bootstrapping sibling aspects");

    try {
      final int rowCount = _entityService.listUrns(DATASET_ENTITY_NAME, 0, 10).getTotal();

      log.info("Found {} dataset entities to attempt to bootstrap", rowCount);

      final AspectSpec datasetAspectSpec =
          _entityRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME).getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
      final AuditStamp auditStamp = new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

      final DataHubUpgradeRequest upgradeRequest = new DataHubUpgradeRequest().setTimestampMs(System.currentTimeMillis()).setVersion(VERSION);
      ingestUpgradeAspect(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, upgradeRequest, auditStamp);

      int indexedCount = 0;
      while (indexedCount < rowCount) {
        getAndRestoreUpstreamLineageIndices(indexedCount, auditStamp, datasetAspectSpec);
        indexedCount += BATCH_SIZE;
      }

      final DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis());
      ingestUpgradeAspect(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, upgradeResult, auditStamp);

      log.info("Successfully restored sibling aspects");
    } catch (Exception e) {
      log.error("Error when running the RestoreDbtSiblingsIndices Bootstrap Step", e);
      _entityService.deleteUrn(SIBLING_UPGRADE_URN);
      throw new RuntimeException("Error when running the RestoreDbtSiblingsIndices Bootstrap Step", e);
    }
  }

  private void getAndRestoreUpstreamLineageIndices(int start, AuditStamp auditStamp, AspectSpec upstreamAspectSpec) {
    ListUrnsResult datasetUrnsResult = _entityService.listUrns(DATASET_ENTITY_NAME, start, BATCH_SIZE);
    List<Urn> datasetUrns = datasetUrnsResult.getEntities();
    log.info("Re-indexing upstreamLineage aspect from {} with batch size {}", start, BATCH_SIZE);

    if (datasetUrns.size() == 0) {
      return;
    }

   final Map<Urn, EntityResponse> upstreamLineageResponse;
    try {
      upstreamLineageResponse =
          _entityService.getEntitiesV2(DATASET_ENTITY_NAME, new HashSet<>(datasetUrns), Collections.singleton(UPSTREAM_LINEAGE_ASPECT_NAME));
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Error fetching upstream lineage history: %s", e.toString()));
    }

    //  Loop over datasets and produce changelog
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

      _entityService.produceMetadataChangeLog(
          datasetUrn,
          DATASET_ENTITY_NAME,
          UPSTREAM_LINEAGE_ASPECT_NAME,
          upstreamAspectSpec,
          null,
          upstreamLineage,
          null,
          null,
          auditStamp,
          ChangeType.RESTATE);
    }
  }

  private UpstreamLineage getUpstreamLineage(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      return null;
    }

    return new UpstreamLineage(aspectMap.get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME).getValue().data());
  }

  private void ingestUpgradeAspect(String aspectName, RecordTemplate aspect, AuditStamp auditStamp) {
    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(SIBLING_UPGRADE_URN);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(aspectName);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(upgradeProposal, auditStamp);
  }
}
