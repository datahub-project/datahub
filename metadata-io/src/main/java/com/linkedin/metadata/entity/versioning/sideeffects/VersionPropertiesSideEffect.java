package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.versionset.VersionSetProperties;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
public class VersionPropertiesSideEffect extends MCPSideEffect {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream().flatMap(item -> processMCP(item, retrieverContext));
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  private static Stream<ChangeMCP> processMCP(
      ChangeMCP changeMCP, @Nonnull RetrieverContext retrieverContext) {
    Urn entityUrn = changeMCP.getUrn();

    if (!VERSION_PROPERTIES_ASPECT_NAME.equals(changeMCP.getAspectName())) {
      return Stream.empty();
    }

    VersionProperties versionProperties = changeMCP.getAspect(VersionProperties.class);
    if (versionProperties == null) {
      log.error("Unable to process version properties for urn: {}", changeMCP.getUrn());
      return Stream.empty();
    }

    Urn versionSetUrn = versionProperties.getVersionSet();
    Aspect versionSetPropertiesAspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(versionSetUrn, VERSION_SET_PROPERTIES_ASPECT_NAME);
    if (versionSetPropertiesAspect == null) {
      return createVersionSet(versionProperties, changeMCP, retrieverContext);
    }

    // Version set exists -- only update if there is a new latest
    VersionSetProperties versionSetProperties =
        RecordUtils.toRecordTemplate(VersionSetProperties.class, versionSetPropertiesAspect.data());
    Urn prevLatest = versionSetProperties.getLatest();
    if (prevLatest.equals(entityUrn)) {
      return Stream.empty();
    }

    VersionProperties prevLatestVersionProperties = null;
    Aspect prevLatestVersionPropertiesAspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(prevLatest, VERSION_PROPERTIES_ASPECT_NAME);
    if (prevLatestVersionPropertiesAspect != null) {
      prevLatestVersionProperties =
          RecordUtils.toRecordTemplate(
              VersionProperties.class, prevLatestVersionPropertiesAspect.data());
      if (versionProperties.getSortId().compareTo(prevLatestVersionProperties.getSortId()) <= 0) {
        return Stream.empty();
      }
    }

    // New version properties is the new latest
    return updateVersionSetLatest(
        versionProperties, prevLatestVersionProperties, prevLatest, changeMCP, retrieverContext);
  }

  private static Stream<ChangeMCP> createVersionSet(
      @Nonnull VersionProperties versionProperties,
      ChangeMCP changeMCP,
      @Nonnull RetrieverContext retrieverContext) {
    versionProperties.setIsLatest(true);

    Urn entityUrn = changeMCP.getUrn();
    Urn versionSetUrn = versionProperties.getVersionSet();

    AspectSpec keyAspectSpec =
        retrieverContext
            .getAspectRetriever()
            .getEntityRegistry()
            .getEntitySpec(VERSION_SET_ENTITY_NAME)
            .getKeyAspectSpec();
    RecordTemplate versionSetKey =
        EntityKeyUtils.convertUrnToEntityKey(versionSetUrn, keyAspectSpec);
    ChangeMCP createVersionSetKey =
        ChangeItemImpl.builder()
            .urn(versionSetUrn)
            .aspectName(VERSION_SET_KEY_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .recordTemplate(versionSetKey)
            .auditStamp(changeMCP.getAuditStamp())
            .systemMetadata(changeMCP.getSystemMetadata())
            .build(retrieverContext.getAspectRetriever());

    VersionSetProperties versionSetPropertiesWithNewLatest =
        new VersionSetProperties()
            .setVersioningScheme(versionProperties.getVersioningScheme())
            .setLatest(entityUrn);
    ChangeMCP createVersionSetProperties =
        ChangeItemImpl.builder()
            .urn(versionSetUrn)
            .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .recordTemplate(versionSetPropertiesWithNewLatest)
            .auditStamp(changeMCP.getAuditStamp())
            .systemMetadata(changeMCP.getSystemMetadata())
            .build(retrieverContext.getAspectRetriever());

    return Stream.of(createVersionSetKey, createVersionSetProperties);
  }

  private static Stream<ChangeMCP> updateVersionSetLatest(
      @Nonnull VersionProperties versionProperties,
      @Nullable VersionProperties prevLatestVersionProperties,
      @Nonnull Urn prevLatest,
      ChangeMCP changeMCP,
      @Nonnull RetrieverContext retrieverContext) {
    versionProperties.setIsLatest(true);

    Urn entityUrn = changeMCP.getUrn();
    Urn versionSetUrn = versionProperties.getVersionSet();

    VersionSetProperties versionSetPropertiesWithNewLatest =
        new VersionSetProperties()
            .setVersioningScheme(versionProperties.getVersioningScheme())
            .setLatest(entityUrn);
    ChangeMCP updateVersionSetProperties =
        ChangeItemImpl.builder()
            .urn(versionSetUrn)
            .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
            .changeType(ChangeType.UPSERT)
            .recordTemplate(versionSetPropertiesWithNewLatest)
            .auditStamp(changeMCP.getAuditStamp())
            .systemMetadata(changeMCP.getSystemMetadata())
            .build(retrieverContext.getAspectRetriever());

    if (prevLatestVersionProperties == null) {
      return Stream.of(updateVersionSetProperties);
    }

    EntitySpec entitySpec =
        retrieverContext
            .getAspectRetriever()
            .getEntityRegistry()
            .getEntitySpec(prevLatest.getEntityType());
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(PatchOperationType.ADD.getValue());
    patchOp.setPath("/isLatest");
    patchOp.setValue(false);
    ChangeMCP updateOldLatestVersionProperties =
        PatchItemImpl.builder()
            .urn(prevLatest)
            .entitySpec(entitySpec)
            .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
            .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
            .patch(GenericJsonPatch.builder().patch(List.of(patchOp)).build().getJsonPatch())
            .auditStamp(changeMCP.getAuditStamp())
            .systemMetadata(changeMCP.getSystemMetadata())
            .build(retrieverContext.getAspectRetriever().getEntityRegistry())
            .applyPatch(prevLatestVersionProperties, retrieverContext.getAspectRetriever());

    return Stream.of(updateVersionSetProperties, updateOldLatestVersionProperties);
  }
}
