package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.linkedin.datahub.upgrade.system.AbstractMCLStep.LAST_URN_KEY;
import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

/**
 * Scans stored {@code dataProductProperties} and invokes {@code DataProductAssetsSideEffect} via
 * RESTATE MCLItems + post-MCP side effects (not a no-op parent upsert) so each member asset gets a
 * denormalized {@code dataProducts} aspect for search filtering and faceting.
 *
 * <p>Uses a versioned upgrade id so a prior marker from the old re-UPSERT implementation cannot
 * suppress this sweep. Skip once {@code SUCCEEDED}/{@code ABORTED} unless {@code
 * REPROCESS_DATA_PRODUCT_ASSETS=true}.
 */
@Slf4j
public class ResyncDataProductAssetsStep implements UpgradeStep {

  static final String UPGRADE_ID = "data-product-assets-from-properties-v1";

  private static final List<String> REQUIRED_ASPECTS = List.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;
  private final boolean reprocessEnabled;

  public ResyncDataProductAssetsStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit,
      boolean reprocessEnabled) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
    this.reprocessEnabled = reprocessEnabled;
    log.info(
        "ResyncDataProductAssetsStep initialized (id={}, reprocessEnabled={})",
        UPGRADE_ID,
        reprocessEnabled);
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @VisibleForTesting
  @Nullable
  public String getUrnLike() {
    return "urn:li:" + DATA_PRODUCT_ENTITY_NAME + ":%";
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (reprocessEnabled) {
      log.info("{}: Reprocess enabled, not skipping.", getUpgradeIdUrn());
      return false;
    }

    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    return prevResult
        .filter(
            result ->
                DataHubUpgradeState.SUCCEEDED.equals(result.getState())
                    || DataHubUpgradeState.ABORTED.equals(result.getState()))
        .isPresent();
  }

  @VisibleForTesting
  Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  /**
   * URN to resume from when a prior run of this step is still {@code IN_PROGRESS} and recorded
   * {@code lastUrn}. Null when there is no resumable checkpoint.
   */
  @Nullable
  private String resumeFrom(UpgradeContext context) {
    String resumeUrn =
        context
            .upgrade()
            .getUpgradeResult(opContext, getUpgradeIdUrn(), entityService)
            .filter(
                result ->
                    DataHubUpgradeState.IN_PROGRESS.equals(result.getState())
                        && result.getResult() != null
                        && result.getResult().containsKey(LAST_URN_KEY))
            .map(result -> result.getResult().get(LAST_URN_KEY))
            .orElse(null);
    if (resumeUrn != null) {
      log.info("{}: Resuming from URN: {}", getUpgradeIdUrn(), resumeUrn);
    }
    return resumeUrn;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info("Starting ResyncDataProductAssetsStep ({})", id());
    return (context) -> {
      String resumeUrn = resumeFrom(context);

      RestoreIndicesArgs argsBuilder =
          new RestoreIndicesArgs()
              .aspectNames(REQUIRED_ASPECTS)
              .batchSize(batchSize)
              .lastUrn(resumeUrn)
              .urnBasedPagination(resumeUrn != null)
              .limit(limit);

      if (getUrnLike() != null) {
        argsBuilder = argsBuilder.urnLike(getUrnLike());
      }
      final RestoreIndicesArgs args = argsBuilder;

      aspectDao.streamAspectBatches(
          context.opContext(),
          args,
          stream -> {
            stream
                .partition(args.batchSize)
                .forEach(
                    batch -> {
                      log.info("Processing batch of size {}.", batchSize);

                      List<SystemAspect> systemAspects =
                          batch
                              .flatMap(
                                  ebeanAspectV2 ->
                                      EntityUtils.toSystemAspectFromEbeanAspects(
                                          opContext,
                                          opContext.getRetrieverContext(),
                                          Set.of(ebeanAspectV2))
                                          .stream())
                              .collect(Collectors.toList());

                      List<MCLItem> restateMclItems = toRestateMclItems(systemAspects);

                      // Force DataProductAssetsSideEffect without a no-op parent upsert: RESTATE
                      // MCLItems + post-MCP side effects → async ingestProposal.
                      ingestSideEffectProposals(buildSideEffectProposals(restateMclItems));

                      Urn lastUrn =
                          restateMclItems.stream()
                              .map(MCLItem::getUrn)
                              .reduce((a, b) -> b)
                              .orElse(null);
                      if (lastUrn != null) {
                        log.info("{}: Saving state. Last urn:{}", getUpgradeIdUrn(), lastUrn);
                        Map<String, String> progress = new HashMap<>();
                        progress.put(LAST_URN_KEY, lastUrn.toString());
                        context
                            .upgrade()
                            .setUpgradeResult(
                                opContext,
                                getUpgradeIdUrn(),
                                entityService,
                                DataHubUpgradeState.IN_PROGRESS,
                                progress);
                      }

                      if (batchDelayMs > 0) {
                        log.info("Sleeping for {} ms", batchDelayMs);
                        try {
                          Thread.sleep(batchDelayMs);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                      }
                    });
            return null;
          });

      BootstrapStep.setUpgradeResult(
          opContext, getUpgradeIdUrn(), entityService, DataHubUpgradeState.SUCCEEDED, Map.of());
      context.report().addLine("State updated: " + getUpgradeIdUrn());

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /**
   * Builds RESTATE {@link MCLItem}s stamped with {@code APP_SOURCE=SYSTEM_UPDATE}. {@code
   * ChangeItemImpl} cannot carry RESTATE, so we construct {@link MetadataChangeLog}s directly.
   */
  @VisibleForTesting
  List<MCLItem> toRestateMclItems(List<SystemAspect> systemAspects) {
    return systemAspects.stream()
        .map(
            systemAspect -> {
              SystemMetadata systemMetadata = withAppSource(systemAspect.getSystemMetadata());
              GenericAspect serialized =
                  GenericRecordUtils.serializeAspect(systemAspect.getRecordTemplate());
              MetadataChangeLog mcl =
                  new MetadataChangeLog()
                      .setEntityUrn(systemAspect.getUrn())
                      .setEntityType(systemAspect.getUrn().getEntityType())
                      .setChangeType(ChangeType.RESTATE)
                      .setAspectName(systemAspect.getAspectName())
                      .setAspect(serialized)
                      // Unchanged restate: previous == current. Required so sibling plugins on this
                      // aspect (DataProductUnsetSideEffect) see an empty delta instead of treating
                      // every member as a new add and unsetting it from other Data Products.
                      .setPreviousAspectValue(serialized)
                      .setSystemMetadata(systemMetadata)
                      .setCreated(systemAspect.getAuditStamp());
              return MCLItemImpl.builder().build(mcl, opContext.getAspectRetriever());
            })
        .collect(Collectors.toList());
  }

  /**
   * Runs registered post-MCP side effects (including {@code DataProductAssetsSideEffect}) on
   * RESTATE MCLItems without requiring a successful parent aspect upsert/MCL emit.
   */
  @VisibleForTesting
  List<MCPItem> buildSideEffectProposals(List<MCLItem> mclItems) {
    if (mclItems.isEmpty()) {
      return List.of();
    }
    try (Stream<MCPItem> sideEffects =
        AspectsBatch.applyPostMCPSideEffects(
            opContext, mclItems, opContext.getRetrieverContext())) {
      return sideEffects.collect(Collectors.toList());
    }
  }

  /**
   * Ingests side-effect MCPs via async {@code ingestProposal}. Matching plugins emit {@code PATCH}
   * (JSON add/remove), not MCP {@code DELETE}. Chunks by {@code batchSize}, sleeping {@code
   * batchDelayMs} between chunks (not after the last).
   */
  @VisibleForTesting
  void ingestSideEffectProposals(List<MCPItem> proposals) {
    if (proposals.isEmpty()) {
      return;
    }

    int chunkSize = Math.max(1, batchSize);
    List<List<MCPItem>> chunks = partition(proposals, chunkSize);
    log.info(
        "Ingesting {} dataProducts side-effect MCPs in {} async chunk(s) of ≤{}",
        proposals.size(),
        chunks.size(),
        chunkSize);
    for (int i = 0; i < chunks.size(); i++) {
      List<MCPItem> chunk = chunks.get(i);
      AspectsBatch proposalBatch =
          AspectsBatchImpl.builder()
              .retrieverContext(opContext.getRetrieverContext())
              .items(chunk)
              .build(opContext);
      entityService.ingestProposal(opContext, proposalBatch, true);
      if (i < chunks.size() - 1 && batchDelayMs > 0) {
        log.info("Sleeping for {} ms between side-effect MCP chunks", batchDelayMs);
        try {
          Thread.sleep(batchDelayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
  }

  @VisibleForTesting
  static <T> List<List<T>> partition(List<T> items, int size) {
    if (items.isEmpty()) {
      return List.of();
    }
    if (size < 1) {
      throw new IllegalArgumentException("partition size must be >= 1");
    }
    List<List<T>> chunks = new ArrayList<>((items.size() + size - 1) / size);
    for (int i = 0; i < items.size(); i += size) {
      chunks.add(List.copyOf(items.subList(i, Math.min(i + size, items.size()))));
    }
    return chunks;
  }

  private static SystemMetadata withAppSource(@Nullable SystemMetadata systemMetadata) {
    SystemMetadata withAppSourceSystemMetadata;
    try {
      withAppSourceSystemMetadata =
          systemMetadata != null
              ? new SystemMetadata(systemMetadata.copy().data())
              : new SystemMetadata();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    StringMap properties = withAppSourceSystemMetadata.getProperties();
    StringMap map = properties != null ? new StringMap(properties.data()) : new StringMap();
    map.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);

    withAppSourceSystemMetadata.setProperties(map);
    return withAppSourceSystemMetadata;
  }
}
