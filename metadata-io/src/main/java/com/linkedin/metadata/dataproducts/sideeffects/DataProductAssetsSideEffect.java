package com.linkedin.metadata.dataproducts.sideeffects;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATA_PRODUCTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.dataproduct.DataProducts;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.patch.template.dataproduct.DataProductsTemplate;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.elasticsearch.FilterUtils;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps each asset's denormalized {@code dataProducts} aspect in sync with the authoritative
 * membership stored on the Data Product side ({@code dataProductProperties.assets}).
 *
 * <p>Uses the post-commit MCP side-effect path ({@link #postMCPSideEffect}) so we receive MCL items
 * with previous and current aspects and can emit ADD/REMOVE patches from that diff. A pure {@code
 * MCLSideEffect} only feeds search indexing and cannot persist versioned aspect patches, so this
 * hook intentionally extends {@link MCPSideEffect} and implements {@link #postMCPSideEffect}.
 *
 * <p>Patches (rather than read-modify-write) keep concurrent membership edits to the same asset
 * from clobbering one another. The resulting {@code dataProducts} field is what makes assets
 * filterable and facetable by Data Product in normal search.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class DataProductAssetsSideEffect extends MCPSideEffect {

  /**
   * Batch size for reading existing asset-side {@code dataProducts} aspects during sync and for
   * search scroll pages when healing stale mirrors on system-update. All unsynced ADD patches for a
   * single Data Product properties commit are emitted in one side-effect invocation (not capped to
   * this value). {@code EntityServiceImpl} Kafka-batches applied patches at 500 ({@code
   * MCP_SIDE_EFFECT_KAFKA_BATCH_SIZE}).
   */
  public static final int DEFAULT_MAX_FANOUT_PER_COMMIT = 500;

  private int maxFanoutPerCommit = DEFAULT_MAX_FANOUT_PER_COMMIT;

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      java.util.Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      java.util.Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    Set<Urn> companionPropertiesDeletes =
        mclItems.stream()
            .filter(
                item ->
                    DATA_PRODUCT_PROPERTIES_ASPECT_NAME.equals(item.getAspectName())
                        && ChangeType.DELETE.equals(item.getChangeType()))
            .map(MCLItem::getUrn)
            .collect(Collectors.toSet());

    return mclItems.stream()
        .flatMap(
            item ->
                generateAssetPatches(
                    operationContext, item, retrieverContext, companionPropertiesDeletes));
  }

  private Stream<MCPItem> generateAssetPatches(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull MCLItem mclItem,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull Set<Urn> companionPropertiesDeletes) {

    if (DATA_PRODUCT_PROPERTIES_ASPECT_NAME.equals(mclItem.getAspectName())) {
      if (ChangeType.DELETE.equals(mclItem.getChangeType())) {
        return removeAllMembership(
            mclItem.getUrn(),
            associationsByAsset(mclItem.getPreviousAspect(DataProductProperties.class)),
            mclItem,
            retrieverContext);
      }
      return syncMembership(operationContext, mclItem, retrieverContext);
    }

    if (DATA_PRODUCT_KEY_ASPECT_NAME.equals(mclItem.getAspectName())
        && ChangeType.DELETE.equals(mclItem.getChangeType())) {
      if (companionPropertiesDeletes.contains(mclItem.getUrn())) {
        log.debug(
            "Skipping dataProducts scrub for {} key delete; companion {} DELETE already in batch",
            mclItem.getUrn(),
            DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
        return Stream.empty();
      }
      // Hard delete without a companion properties DELETE: scrub from snapshot or retriever cache.
      return scrubFromKeyDelete(operationContext, mclItem, retrieverContext);
    }

    return Stream.empty();
  }

  private Stream<MCPItem> scrubFromKeyDelete(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull MCLItem mclItem,
      @Nonnull RetrieverContext retrieverContext) {
    DataProductProperties previous = mclItem.getPreviousAspect(DataProductProperties.class);
    if (previous == null) {
      Aspect propertiesAspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(
                  operationContext, mclItem.getUrn(), DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
      if (propertiesAspect != null) {
        previous = new DataProductProperties(propertiesAspect.data());
      }
    }
    if (previous == null) {
      log.debug(
          "Skipping dataProducts scrub for {} key delete; no dataProductProperties available",
          mclItem.getUrn());
      return Stream.empty();
    }
    return removeAllMembership(
        mclItem.getUrn(), associationsByAsset(previous), mclItem, retrieverContext);
  }

  private Stream<MCPItem> syncMembership(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull MCLItem mclItem,
      @Nonnull RetrieverContext retrieverContext) {
    final Urn dataProductUrn = mclItem.getUrn();
    final Map<Urn, DataProductAssociation> newByAsset =
        associationsByAsset(mclItem.getAspect(DataProductProperties.class));
    final DataProductProperties previous = mclItem.getPreviousAspect(DataProductProperties.class);

    // CREATE / CREATE_ENTITY / RESTATE, first write, or ZDU/system-update rewrite: treat every
    // unsynced member as an ADD (idempotent). MigrateAspects UPSERTs identical payloads, so a
    // pure before/after diff would emit nothing without this branch. Skip assets that already
    // mirror this membership so a later reprocess can advance past already-synced members.
    if (!ChangeType.UPSERT.equals(mclItem.getChangeType())
        || previous == null
        || isSystemUpdate(mclItem.getSystemMetadata())) {
      // Materialize ADDs first: removeStaleMirrors may throw on scroll failure, and Stream.concat
      // evaluates args eagerly — without this, a REMOVE-heal failure would discard already-built
      // ADDs. MigrateAspects / Resync enqueue async MCPs and FailedMCP has no retry, so keep ADDs.
      List<MCPItem> adds =
          addUnsynced(operationContext, newByAsset, dataProductUrn, mclItem, retrieverContext)
              .collect(Collectors.toList());
      if (isSystemUpdate(mclItem.getSystemMetadata())) {
        try {
          List<MCPItem> removes =
              removeStaleMirrors(dataProductUrn, newByAsset, mclItem, retrieverContext)
                  .collect(Collectors.toList());
          return Stream.concat(adds.stream(), removes.stream());
        } catch (RuntimeException e) {
          log.warn(
              "REMOVE heal failed for {}; emitting {} ADD(s) anyway",
              dataProductUrn,
              adds.size(),
              e);
          return adds.stream();
        }
      }
      return adds.stream();
    }

    final Map<Urn, DataProductAssociation> oldByAsset = associationsByAsset(previous);

    Stream<MCPItem> adds =
        newByAsset.entrySet().stream()
            .filter(
                entry -> {
                  DataProductAssociation prior = oldByAsset.get(entry.getKey());
                  return prior == null || !sameMembership(prior, entry.getValue());
                })
            .map(
                entry ->
                    buildAssetPatch(
                        entry.getKey(),
                        dataProductUrn,
                        entry.getValue(),
                        PatchOperationType.ADD,
                        mclItem,
                        retrieverContext))
            .filter(Objects::nonNull);

    Stream<MCPItem> removes =
        oldByAsset.keySet().stream()
            .filter(asset -> !newByAsset.containsKey(asset))
            .map(
                asset ->
                    buildAssetPatch(
                        asset,
                        dataProductUrn,
                        null,
                        PatchOperationType.REMOVE,
                        mclItem,
                        retrieverContext))
            .filter(Objects::nonNull);

    return Stream.concat(removes, adds);
  }

  /**
   * Emits ADD patches for every member that does not already carry this Data Product on its
   * asset-side {@code dataProducts} aspect. Reads existing aspects in batches of {@link
   * #maxFanoutPerCommit} for efficiency but emits all unsynced patches in this invocation.
   */
  private Stream<MCPItem> addUnsynced(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Map<Urn, DataProductAssociation> byAsset,
      @Nonnull Urn dataProductUrn,
      @Nonnull MCLItem source,
      @Nonnull RetrieverContext retrieverContext) {
    if (byAsset.isEmpty()) {
      return Stream.empty();
    }
    List<Urn> assets = new ArrayList<>(byAsset.keySet());
    List<MCPItem> toEmit = new ArrayList<>();
    int scanned = 0;
    while (scanned < assets.size()) {
      int end = Math.min(scanned + maxFanoutPerCommit, assets.size());
      List<Urn> chunk = assets.subList(scanned, end);
      Map<Urn, Map<String, Aspect>> existing =
          latestDataProducts(operationContext, chunk, retrieverContext);
      for (Urn asset : chunk) {
        DataProductAssociation wanted = byAsset.get(asset);
        if (alreadyMirrored(existing.get(asset), dataProductUrn, wanted)) {
          continue;
        }
        MCPItem patch =
            buildAssetPatch(
                asset, dataProductUrn, wanted, PatchOperationType.ADD, source, retrieverContext);
        if (patch != null) {
          toEmit.add(patch);
        }
      }
      scanned = end;
    }
    return toEmit.stream();
  }

  /**
   * On system-update reprocess, search for assets that still mirror this Data Product but are no
   * longer listed in {@code dataProductProperties.assets}, and emit REMOVE patches. Live user
   * UPSERTs already diff REMOVEs from the before/after membership map. Includes soft-deleted assets
   * so restoring one cannot resurrect a stale membership.
   */
  private Stream<MCPItem> removeStaleMirrors(
      @Nonnull Urn dataProductUrn,
      @Nonnull Map<Urn, DataProductAssociation> currentMembers,
      @Nonnull MCLItem source,
      @Nonnull RetrieverContext retrieverContext) {
    SearchRetriever searchRetriever = retrieverContext.getSearchRetriever();
    if (searchRetriever == null || searchRetriever == SearchRetriever.EMPTY) {
      return Stream.empty();
    }

    List<String> entities = entitiesSupportingDataProducts(retrieverContext);
    if (entities.isEmpty()) {
      return Stream.empty();
    }

    Filter filter =
        FilterUtils.createValuesFilter("dataProduct", List.of(dataProductUrn.toString()));
    List<MCPItem> removes = new ArrayList<>();
    try {
      String scrollId = null;
      do {
        ScrollResult scrollResult =
            searchRetriever.scroll(
                entities,
                filter,
                scrollId,
                maxFanoutPerCommit,
                List.of(),
                SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS_INCLUDE_SOFT_DELETED);

        if (scrollResult.getEntities() == null || scrollResult.getEntities().isEmpty()) {
          break;
        }

        for (SearchEntity hit : scrollResult.getEntities()) {
          Urn assetUrn = hit.getEntity();
          if (assetUrn == null || currentMembers.containsKey(assetUrn)) {
            continue;
          }
          MCPItem patch =
              buildAssetPatch(
                  assetUrn,
                  dataProductUrn,
                  null,
                  PatchOperationType.REMOVE,
                  source,
                  retrieverContext);
          if (patch != null) {
            removes.add(patch);
          }
        }

        String nextScrollId = scrollResult.getScrollId();
        if (nextScrollId == null || nextScrollId.equals(scrollId)) {
          break;
        }
        scrollId = nextScrollId;
      } while (true);
    } catch (RuntimeException e) {
      log.warn(
          "Unable to scroll for stale dataProduct mirrors for {}; failing REMOVE heal",
          dataProductUrn,
          e);
      // Do not return a partial REMOVE list — incomplete heal would leave MigrateAspects /
      // ResyncDataProductAssetsStep marked successful while stale mirrors remain.
      throw e;
    }
    return removes.stream();
  }

  @Nonnull
  private static List<String> entitiesSupportingDataProducts(
      @Nonnull RetrieverContext retrieverContext) {
    return retrieverContext
        .getAspectRetriever()
        .getEntityRegistry()
        .getEntitySpecs()
        .values()
        .stream()
        .filter(spec -> spec.getAspectSpec(DATA_PRODUCTS_ASPECT_NAME) != null)
        .map(EntitySpec::getName)
        .sorted()
        .collect(Collectors.toList());
  }

  @Nonnull
  private static Map<Urn, Map<String, Aspect>> latestDataProducts(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<Urn> assets,
      @Nonnull RetrieverContext retrieverContext) {
    try {
      Map<Urn, Map<String, Aspect>> existing =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObjects(
                  operationContext, new HashSet<>(assets), Set.of(DATA_PRODUCTS_ASPECT_NAME));
      return existing != null ? existing : Map.of();
    } catch (RuntimeException e) {
      log.warn(
          "Unable to read existing dataProducts aspects for {} asset(s); treating as unsynced",
          assets.size(),
          e);
      return Map.of();
    }
  }

  private static boolean alreadyMirrored(
      @Nullable Map<String, Aspect> aspects,
      @Nonnull Urn dataProductUrn,
      @Nullable DataProductAssociation wanted) {
    if (aspects == null || wanted == null) {
      return false;
    }
    Aspect aspect = aspects.get(DATA_PRODUCTS_ASPECT_NAME);
    if (aspect == null) {
      return false;
    }
    DataProducts dataProducts = new DataProducts(aspect.data());
    if (dataProducts.getDataProducts() == null) {
      return false;
    }
    for (DataProductAssociation existing : dataProducts.getDataProducts()) {
      // Asset-side destinationUrn is the Data Product; wanted.destinationUrn is the asset.
      if (dataProductUrn.equals(existing.getDestinationUrn())
          && Boolean.TRUE.equals(existing.isOutputPort())
              == Boolean.TRUE.equals(wanted.isOutputPort())) {
        return true;
      }
    }
    return false;
  }

  private static Stream<MCPItem> removeAllMembership(
      @Nonnull Urn dataProductUrn,
      @Nonnull Map<Urn, DataProductAssociation> byAsset,
      @Nonnull MCLItem source,
      @Nonnull RetrieverContext retrieverContext) {
    return byAsset.keySet().stream()
        .map(
            asset ->
                buildAssetPatch(
                    asset,
                    dataProductUrn,
                    null,
                    PatchOperationType.REMOVE,
                    source,
                    retrieverContext))
        .filter(Objects::nonNull);
  }

  private static boolean isSystemUpdate(@Nullable SystemMetadata systemMetadata) {
    if (systemMetadata == null || systemMetadata.getProperties() == null) {
      return false;
    }
    return SYSTEM_UPDATE_SOURCE.equals(systemMetadata.getProperties().get(APP_SOURCE));
  }

  private static boolean sameMembership(
      @Nonnull DataProductAssociation left, @Nonnull DataProductAssociation right) {
    return Objects.equals(left.getDestinationUrn(), right.getDestinationUrn())
        && Boolean.TRUE.equals(left.isOutputPort()) == Boolean.TRUE.equals(right.isOutputPort());
  }

  @Nonnull
  private static Map<Urn, DataProductAssociation> associationsByAsset(
      @Nullable DataProductProperties dataProductProperties) {
    if (dataProductProperties == null || dataProductProperties.getAssets() == null) {
      return Collections.emptyMap();
    }
    Map<Urn, DataProductAssociation> byAsset = new LinkedHashMap<>();
    for (DataProductAssociation association : dataProductProperties.getAssets()) {
      if (association.getDestinationUrn() != null) {
        byAsset.put(association.getDestinationUrn(), association);
      }
    }
    return byAsset;
  }

  @Nullable
  private static MCPItem buildAssetPatch(
      @Nonnull Urn assetUrn,
      @Nonnull Urn dataProductUrn,
      @Nullable DataProductAssociation sourceAssociation,
      @Nonnull PatchOperationType operation,
      @Nonnull MCLItem source,
      @Nonnull RetrieverContext retrieverContext) {
    final EntitySpec entitySpec =
        Optional.ofNullable(retrieverContext.getAspectRetriever().getEntityRegistry())
            .map(registry -> registry.getEntitySpec(assetUrn.getEntityType()))
            .orElse(null);
    if (entitySpec == null || entitySpec.getAspectSpec(DATA_PRODUCTS_ASPECT_NAME) == null) {
      log.warn(
          "Skipping dataProducts sync for {}: entity type does not support the {} aspect",
          assetUrn,
          DATA_PRODUCTS_ASPECT_NAME);
      return null;
    }

    final GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(operation.getValue());
    patchOp.setPath(
        String.format("/%s/%s", DataProductsTemplate.DATA_PRODUCTS_FIELD_NAME, dataProductUrn));
    if (operation == PatchOperationType.ADD) {
      Map<String, Object> value = new HashMap<>();
      value.put(DataProductsTemplate.KEY_FIELD_NAME, dataProductUrn.toString());
      boolean outputPort =
          sourceAssociation != null && Boolean.TRUE.equals(sourceAssociation.isOutputPort());
      value.put("outputPort", outputPort);
      patchOp.setValue(value);
    }

    // Omit arrayPrimaryKeys so PatchItemImpl applies via the registered DataProductsTemplate.
    return PatchItemImpl.builder()
        .urn(assetUrn)
        .entitySpec(entitySpec)
        .aspectName(DATA_PRODUCTS_ASPECT_NAME)
        .aspectSpec(entitySpec.getAspectSpec(DATA_PRODUCTS_ASPECT_NAME))
        .patch(GenericJsonPatch.builder().patch(List.of(patchOp)).build().getJsonPatch())
        .auditStamp(source.getAuditStamp())
        .systemMetadata(source.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever().getEntityRegistry());
  }
}
