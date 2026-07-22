package com.linkedin.metadata.dataproducts.sideeffects;

import static com.linkedin.metadata.Constants.DATA_PRODUCTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import java.util.Collections;
import java.util.HashSet;
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
 * <p>On a Data Product properties change we diff the previous vs. new asset list and emit a PATCH
 * per affected asset: ADD the data product to newly-added assets, REMOVE it from dropped assets.
 * Patches (rather than read-modify-write) keep concurrent membership edits to the same asset from
 * clobbering one another. The resulting {@code dataProducts} field is what makes assets filterable
 * and facetable by Data Product in normal search.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class DataProductAssetsSideEffect extends MCPSideEffect {

  private static final String DATA_PRODUCTS_FIELD_NAME = "dataProducts";

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
    return mclItems.stream().flatMap(item -> generateAssetPatches(item, retrieverContext));
  }

  private static Stream<MCPItem> generateAssetPatches(
      @Nonnull MCLItem mclItem, @Nonnull RetrieverContext retrieverContext) {
    if (!DATA_PRODUCT_PROPERTIES_ASPECT_NAME.equals(mclItem.getAspectName())) {
      return Stream.empty();
    }

    final Urn dataProductUrn = mclItem.getUrn();
    final Set<Urn> newAssets = assetUrns(mclItem.getAspect(DataProductProperties.class));
    final DataProductProperties previous = mclItem.getPreviousAspect(DataProductProperties.class);

    // CREATE / CREATE_ENTITY / RESTATE (or first write) carry no reliable previous value to diff
    // against, so treat every current member as an addition. ADD patches are idempotent. Mirrors
    // DataProductUnsetSideEffect.
    if (!ChangeType.UPSERT.equals(mclItem.getChangeType()) || previous == null) {
      return newAssets.stream()
          .map(
              asset ->
                  buildAssetPatch(
                      asset, dataProductUrn, PatchOperationType.ADD, mclItem, retrieverContext))
          .filter(Objects::nonNull);
    }

    final Set<Urn> oldAssets = assetUrns(previous);
    final Set<Urn> added = new HashSet<>(newAssets);
    added.removeAll(oldAssets);
    final Set<Urn> removed = new HashSet<>(oldAssets);
    removed.removeAll(newAssets);

    return Stream.concat(
            added.stream()
                .map(
                    asset ->
                        buildAssetPatch(
                            asset,
                            dataProductUrn,
                            PatchOperationType.ADD,
                            mclItem,
                            retrieverContext)),
            removed.stream()
                .map(
                    asset ->
                        buildAssetPatch(
                            asset,
                            dataProductUrn,
                            PatchOperationType.REMOVE,
                            mclItem,
                            retrieverContext)))
        .filter(Objects::nonNull);
  }

  private static Set<Urn> assetUrns(@Nullable DataProductProperties dataProductProperties) {
    if (dataProductProperties == null || dataProductProperties.getAssets() == null) {
      return Collections.emptySet();
    }
    return dataProductProperties.getAssets().stream()
        .map(DataProductAssociation::getDestinationUrn)
        .collect(Collectors.toSet());
  }

  @Nullable
  private static MCPItem buildAssetPatch(
      @Nonnull Urn assetUrn,
      @Nonnull Urn dataProductUrn,
      @Nonnull PatchOperationType operation,
      @Nonnull MCLItem source,
      @Nonnull RetrieverContext retrieverContext) {
    final EntitySpec entitySpec =
        Optional.ofNullable(retrieverContext.getAspectRetriever().getEntityRegistry())
            .map(registry -> registry.getEntitySpec(assetUrn.getEntityType()))
            .orElse(null);
    if (entitySpec == null || entitySpec.getAspectSpec(DATA_PRODUCTS_ASPECT_NAME) == null) {
      // Asset type doesn't carry the dataProducts aspect; nothing to mirror.
      log.warn(
          "Skipping dataProducts sync for {}: entity type does not support the {} aspect",
          assetUrn,
          DATA_PRODUCTS_ASPECT_NAME);
      return null;
    }

    final GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(operation.getValue());
    patchOp.setPath(String.format("/%s/%s", DATA_PRODUCTS_FIELD_NAME, dataProductUrn));
    if (operation == PatchOperationType.ADD) {
      patchOp.setValue(dataProductUrn.toString());
    }

    return PatchItemImpl.builder()
        .urn(assetUrn)
        .entitySpec(entitySpec)
        .aspectName(DATA_PRODUCTS_ASPECT_NAME)
        .aspectSpec(entitySpec.getAspectSpec(DATA_PRODUCTS_ASPECT_NAME))
        .patch(
            GenericJsonPatch.builder()
                .arrayPrimaryKeys(Map.of(DATA_PRODUCTS_FIELD_NAME, Collections.<String>emptyList()))
                .patch(List.of(patchOp))
                .build()
                .getJsonPatch())
        .auditStamp(source.getAuditStamp())
        .systemMetadata(source.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever().getEntityRegistry());
  }
}
