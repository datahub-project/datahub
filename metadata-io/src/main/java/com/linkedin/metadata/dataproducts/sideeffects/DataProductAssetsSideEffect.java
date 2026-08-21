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
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.EntitySpec;
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
 * with previous and current aspects and can emit ADD/REMOVE patches from that diff. (A pure {@code
 * MCLSideEffect} only feeds search indexing and cannot persist versioned aspect patches.)
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
   * Default cap on ADD patches emitted for a single Data Product properties commit. Large
   * CREATE/RESTATE/system-update fan-outs beyond this are truncated; already-synced members are
   * skipped so a later reprocess advances. REMOVE patches are never truncated — leftover asset-side
   * membership after delete cannot be recovered from a re-run.
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
    return mclItems.stream()
        .flatMap(item -> generateAssetPatches(operationContext, item, retrieverContext));
  }

  private Stream<MCPItem> generateAssetPatches(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull MCLItem mclItem,
      @Nonnull RetrieverContext retrieverContext) {

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
      // Hard delete emits a key-aspect DELETE after wipe; scrub from a companion
      // dataProductProperties DELETE MCL when available (see EntityServiceImpl), otherwise read
      // any leftover properties aspect before it disappears from the retriever cache path.
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
    // mirror this membership so a capped reprocess can advance past the first N.
    if (!ChangeType.UPSERT.equals(mclItem.getChangeType())
        || previous == null
        || isSystemUpdate(mclItem.getSystemMetadata())) {
      return addUnsynced(operationContext, newByAsset, dataProductUrn, mclItem, retrieverContext);
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

    return Stream.concat(removes, boundAdds(adds, dataProductUrn));
  }

  /**
   * Emits ADD patches for members that do not already carry this Data Product on their asset-side
   * {@code dataProducts} aspect, up to {@link #maxFanoutPerCommit}. Walking in stable order and
   * skipping already-synced members lets a later MigrateAspects / reprocess pass finish the rest.
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
    while (scanned < assets.size() && toEmit.size() < maxFanoutPerCommit) {
      int end = Math.min(scanned + maxFanoutPerCommit, assets.size());
      List<Urn> chunk = assets.subList(scanned, end);
      Map<Urn, Map<String, Aspect>> existing =
          latestDataProducts(operationContext, chunk, retrieverContext);
      for (Urn asset : chunk) {
        if (toEmit.size() >= maxFanoutPerCommit) {
          break;
        }
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
    if (scanned < assets.size()) {
      log.warn(
          "Truncating dataProducts ADD fan-out for {}: emitting {} unsynced patches "
              + "(maxFanoutPerCommit={}); {} member(s) not yet scanned. Re-run system-update "
              + "migrateAspects or set systemUpdate.dataProductAssets.reprocess.enabled to "
              + "finish remaining assets.",
          dataProductUrn,
          toEmit.size(),
          maxFanoutPerCommit,
          assets.size() - scanned);
    }
    return toEmit.stream();
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

  private Stream<MCPItem> boundAdds(@Nonnull Stream<MCPItem> adds, @Nonnull Urn dataProductUrn) {
    List<MCPItem> materialised = adds.collect(Collectors.toList());
    if (materialised.size() <= maxFanoutPerCommit) {
      return materialised.stream();
    }
    log.warn(
        "Truncating dataProducts ADD fan-out for {}: emitting {} of {} patches "
            + "(maxFanoutPerCommit={}). Re-run system-update migrateAspects or set "
            + "systemUpdate.dataProductAssets.reprocess.enabled to finish remaining assets.",
        dataProductUrn,
        maxFanoutPerCommit,
        materialised.size(),
        maxFanoutPerCommit);
    return materialised.stream().limit(maxFanoutPerCommit);
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
