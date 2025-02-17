package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
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
import com.linkedin.versionset.VersionSetProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Side effect that updates the isLatest property for the referenced versioned entity's Version
 * Properties aspect.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class VersionSetSideEffect extends MCPSideEffect {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream().flatMap(item -> updateLatest(item, retrieverContext));
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  private static Stream<ChangeMCP> updateLatest(
      ChangeMCP changeMCP, @Nonnull RetrieverContext retrieverContext) {

    if (VERSION_SET_PROPERTIES_ASPECT_NAME.equals(changeMCP.getAspectName())) {
      List<ChangeMCP> mcpItems = new ArrayList<>();
      VersionSetProperties versionSetProperties = changeMCP.getAspect(VersionSetProperties.class);
      if (versionSetProperties == null) {
        log.error("Unable to process version set properties for urn: {}", changeMCP.getUrn());
        return Stream.empty();
      }
      // Set old latest isLatest to false, set new latest isLatest to true
      // This side effect assumes the entity is already versioned, if it is not yet versioned it
      // will fail due
      // to not having set default values for the aspect. This creates an implicit ordering of when
      // aspects should be
      // updated. Version Properties first, then Version Set Properties.
      Urn newLatest = versionSetProperties.getLatest();

      VersionSetProperties previousVersionSetProperties =
          changeMCP.getPreviousAspect(VersionSetProperties.class);
      if (previousVersionSetProperties != null) {
        Urn previousLatest = previousVersionSetProperties.getLatest();
        Aspect previousLatestEntity =
            retrieverContext
                .getAspectRetriever()
                .getLatestAspectObject(previousLatest, VERSION_PROPERTIES_ASPECT_NAME);
        if (!newLatest.equals(previousLatest) && previousLatestEntity != null) {
          EntitySpec entitySpec =
              retrieverContext
                  .getAspectRetriever()
                  .getEntityRegistry()
                  .getEntitySpec(previousLatest.getEntityType());
          VersionProperties previousLatestProperties =
              RecordUtils.toRecordTemplate(VersionProperties.class, previousLatestEntity.data());
          GenericJsonPatch.PatchOp previousPatch = new GenericJsonPatch.PatchOp();
          previousPatch.setOp(PatchOperationType.ADD.getValue());
          previousPatch.setPath("/isLatest");
          previousPatch.setValue(false);
          mcpItems.add(
              PatchItemImpl.builder()
                  .urn(previousLatest)
                  .entitySpec(entitySpec)
                  .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
                  .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
                  .patch(
                      GenericJsonPatch.builder()
                          .patch(List.of(previousPatch))
                          .build()
                          .getJsonPatch())
                  .auditStamp(changeMCP.getAuditStamp())
                  .systemMetadata(changeMCP.getSystemMetadata())
                  .build(retrieverContext.getAspectRetriever().getEntityRegistry())
                  .applyPatch(previousLatestProperties, retrieverContext.getAspectRetriever()));
        }
      }

      // Explicitly error here to avoid downstream patch error with less context
      Aspect newLatestEntity =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(newLatest, VERSION_PROPERTIES_ASPECT_NAME);
      if (newLatestEntity == null) {
        throw new UnsupportedOperationException(
            "Cannot set latest version to unversioned entity: " + newLatest);
      }

      EntitySpec entitySpec =
          retrieverContext
              .getAspectRetriever()
              .getEntityRegistry()
              .getEntitySpec(newLatest.getEntityType());
      VersionProperties newLatestProperties =
          RecordUtils.toRecordTemplate(VersionProperties.class, newLatestEntity.data());
      GenericJsonPatch.PatchOp currentPatch = new GenericJsonPatch.PatchOp();
      currentPatch.setOp(PatchOperationType.ADD.getValue());
      currentPatch.setPath("/isLatest");
      currentPatch.setValue(true);
      mcpItems.add(
          PatchItemImpl.builder()
              .urn(newLatest)
              .entitySpec(entitySpec)
              .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
              .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
              .patch(GenericJsonPatch.builder().patch(List.of(currentPatch)).build().getJsonPatch())
              .auditStamp(changeMCP.getAuditStamp())
              .systemMetadata(changeMCP.getSystemMetadata())
              .build(retrieverContext.getAspectRetriever().getEntityRegistry())
              .applyPatch(newLatestProperties, retrieverContext.getAspectRetriever()));
      return mcpItems.stream();
    }
    return Stream.empty();
  }
}
