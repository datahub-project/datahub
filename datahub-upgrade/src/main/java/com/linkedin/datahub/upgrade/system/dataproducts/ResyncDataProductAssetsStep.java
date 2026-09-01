package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Reprocess step: scrolls Data Products, batch-fetches {@code dataProductProperties}, and emits
 * UPSERT proposals tagged as system-update so {@link
 * com.linkedin.metadata.dataproducts.sideeffects.DataProductAssetsSideEffect} re-mirrors membership
 * onto assets.
 */
@Slf4j
public class ResyncDataProductAssetsStep implements UpgradeStep {

  private static final String UPGRADE_ID = "ResyncDataProductAssetsStep";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final boolean reprocessEnabled;
  private final Integer batchSize;

  public ResyncDataProductAssetsStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean reprocessEnabled,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
    this.reprocessEnabled = reprocessEnabled;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      String scrollId = null;
      int migratedCount = 0;
      int failureCount = 0;
      do {
        log.info(
            "Resyncing dataProducts membership via dataProductProperties UPSERT, batch {}-{}",
            migratedCount,
            migratedCount + batchSize);
        ResyncBatchResult batchResult = resyncBatch(auditStamp, scrollId);
        failureCount += batchResult.failureCount();
        scrollId = batchResult.scrollId();
        migratedCount += batchSize;
      } while (scrollId != null);

      if (failureCount > 0) {
        log.error(
            "{} completed with {} failure(s); upgrade result not recorded so a later run can retry",
            id(),
            failureCount);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private ResyncBatchResult resyncBatch(AuditStamp auditStamp, String scrollId) {
    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            ImmutableList.of(DATA_PRODUCT_ENTITY_NAME),
            "*",
            null,
            null,
            scrollId,
            null,
            batchSize,
            null);

    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().isEmpty()) {
      return new ResyncBatchResult(null, 0);
    }

    Set<Urn> dataProductUrns =
        scrollResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toCollection(HashSet::new));

    int failureCount = 0;
    try {
      Map<Urn, EntityResponse> responses =
          entityService.getEntitiesV2(
              opContext,
              DATA_PRODUCT_ENTITY_NAME,
              dataProductUrns,
              Collections.singleton(DATA_PRODUCT_PROPERTIES_ASPECT_NAME));

      for (Urn dataProductUrn : dataProductUrns) {
        try {
          resyncDataProduct(dataProductUrn, responses.get(dataProductUrn), auditStamp);
        } catch (Exception e) {
          failureCount++;
          log.error("Error resyncing dataProducts for members of {}", dataProductUrn, e);
        }
      }
    } catch (Exception e) {
      failureCount += dataProductUrns.size();
      log.error("Error batch-fetching dataProductProperties for resync", e);
    }

    return new ResyncBatchResult(scrollResult.getScrollId(), failureCount);
  }

  private void resyncDataProduct(Urn dataProductUrn, EntityResponse response, AuditStamp auditStamp)
      throws Exception {
    if (response == null
        || !response.getAspects().containsKey(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)) {
      return;
    }

    EnvelopedAspect enveloped = response.getAspects().get(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    DataMap dataMap = enveloped.getValue().data();
    DataProductProperties properties = new DataProductProperties(dataMap);

    SystemMetadata systemMetadata = createDefaultSystemMetadata();
    StringMap props =
        systemMetadata.getProperties() != null
            ? new StringMap(systemMetadata.getProperties().data())
            : new StringMap();
    props.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);
    systemMetadata.setProperties(props);

    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(dataProductUrn);
    proposal.setEntityType(DATA_PRODUCT_ENTITY_NAME);
    proposal.setAspectName(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(systemMetadata);
    proposal.setAspect(GenericRecordUtils.serializeAspect(properties));

    entityService.ingestProposal(opContext, proposal, auditStamp, true);
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
  public boolean skip(UpgradeContext context) {
    // Reprocess escape hatch: always run when enabled via application.yaml.
    if (reprocessEnabled) {
      log.info("{} reprocess enabled; running resync.", id());
      return false;
    }

    boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    // Without reprocess, this step is a no-op — first-time fill is MigrateAspects / ZDU.
    return true;
  }

  private record ResyncBatchResult(String scrollId, int failureCount) {}
}
