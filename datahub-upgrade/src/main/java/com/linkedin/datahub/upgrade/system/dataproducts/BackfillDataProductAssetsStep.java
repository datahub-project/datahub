package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * One-time backfill that populates each existing asset's {@code dataProducts} aspect from the
 * authoritative membership on the Data Product side.
 *
 * <p>The DataProductAssetsSideEffect only fires on live {@code dataProductProperties} writes, so it
 * cannot retroactively populate pre-existing memberships (RESTATE does not drive MCP side effects,
 * and re-UPSERTing an unchanged aspect produces no diff). This step therefore writes the asset
 * aspects directly: it scrolls all Data Products and, for each member asset, emits an idempotent
 * ADD PATCH to the asset's {@code dataProducts} aspect. Re-running is safe.
 */
@Slf4j
public class BackfillDataProductAssetsStep implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillDataProductAssetsStep";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  private static final String DATA_PRODUCTS_FIELD_NAME = "dataProducts";
  private static final String JSON_PATCH_CONTENT_TYPE = "application/json-patch+json";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;
  private final Integer batchSize;

  public BackfillDataProductAssetsStep(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      Integer batchSize) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.searchService = searchService;
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
      do {
        log.info(
            "Backfilling dataProducts membership onto assets, data product batch {}-{}",
            migratedCount,
            migratedCount + batchSize);
        scrollId = backfillDataProductBatch(auditStamp, scrollId);
        migratedCount += batchSize;
      } while (scrollId != null);

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private String backfillDataProductBatch(AuditStamp auditStamp, String scrollId) {
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
      return null;
    }

    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        backfillDataProduct(searchEntity.getEntity(), auditStamp);
      } catch (Exception e) {
        // Don't fail the whole backfill because of one bad data product.
        log.error("Error backfilling dataProducts for members of {}", searchEntity.getEntity(), e);
      }
    }

    return scrollResult.getScrollId();
  }

  private void backfillDataProduct(Urn dataProductUrn, AuditStamp auditStamp) throws Exception {
    final EntityResponse response =
        entityService.getEntityV2(
            opContext,
            dataProductUrn.getEntityType(),
            dataProductUrn,
            Collections.singleton(DATA_PRODUCT_PROPERTIES_ASPECT_NAME));
    if (response == null
        || !response.getAspects().containsKey(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)) {
      return;
    }

    final DataMap dataMap =
        response.getAspects().get(DATA_PRODUCT_PROPERTIES_ASPECT_NAME).getValue().data();
    final DataProductProperties properties = new DataProductProperties(dataMap);
    if (properties.getAssets() == null) {
      return;
    }

    for (DataProductAssociation association : properties.getAssets()) {
      final Urn assetUrn = association.getDestinationUrn();
      try {
        entityService.ingestProposal(
            opContext, buildAddMembershipPatch(assetUrn, dataProductUrn), auditStamp, true);
      } catch (Exception e) {
        log.error(
            "Error adding data product {} to asset {} during backfill",
            dataProductUrn,
            assetUrn,
            e);
      }
    }
  }

  /**
   * Builds an idempotent ADD PATCH on the asset's {@code dataProducts} array (keyed by URN value),
   * mirroring what DataProductAssetsSideEffect emits for live changes.
   */
  private MetadataChangeProposal buildAddMembershipPatch(Urn assetUrn, Urn dataProductUrn) {
    final ObjectNode patchOp =
        instance
            .objectNode()
            .put("op", PatchOperationType.ADD.getValue())
            .put("path", String.format("/%s/%s", DATA_PRODUCTS_FIELD_NAME, dataProductUrn))
            .put("value", dataProductUrn.toString());
    final ArrayNode patches = instance.arrayNode().add(patchOp);

    final ObjectNode arrayPrimaryKeys = instance.objectNode();
    // Empty key list => array elements are keyed by their own URN value.
    arrayPrimaryKeys.set(DATA_PRODUCTS_FIELD_NAME, instance.arrayNode());

    final ObjectNode envelope = instance.objectNode();
    envelope.set("arrayPrimaryKeys", arrayPrimaryKeys);
    envelope.set("patch", patches);

    final GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType(JSON_PATCH_CONTENT_TYPE);
    genericAspect.setValue(ByteString.copyString(envelope.toString(), StandardCharsets.UTF_8));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(assetUrn);
    proposal.setEntityType(assetUrn.getEntityType());
    proposal.setAspectName(DATA_PRODUCTS_ASPECT_NAME);
    proposal.setChangeType(ChangeType.PATCH);
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    proposal.setAspect(genericAspect);
    return proposal;
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
    final boolean envEnabled = Boolean.parseBoolean(System.getenv("BACKFILL_DATA_PRODUCT_ASSETS"));

    final boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun || !envEnabled;
  }
}
