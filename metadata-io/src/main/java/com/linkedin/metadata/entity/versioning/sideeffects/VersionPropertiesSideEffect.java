package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.*;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
      @Nonnull OperationFingerprint operationContext,
      Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();

    // First pass: keep the version-properties items we can process and collect the version-set
    // URNs whose VersionSetProperties must be read.
    List<VersionPropertiesContext> contexts = new ArrayList<>();
    Set<Urn> versionSetUrns = new HashSet<>();
    for (ChangeMCP item : changeMCPS) {
      if (!VERSION_PROPERTIES_ASPECT_NAME.equals(item.getAspectName())) {
        continue;
      }
      VersionProperties versionProperties = item.getAspect(VersionProperties.class);
      if (versionProperties == null) {
        log.error("Unable to process version properties for urn: {}", item.getUrn());
        continue;
      }
      contexts.add(new VersionPropertiesContext(item, versionProperties));
      versionSetUrns.add(versionProperties.getVersionSet());
    }

    if (contexts.isEmpty()) {
      return Stream.empty();
    }

    // Batched read #1: VersionSetProperties for every referenced version set.
    Map<Urn, Map<String, Aspect>> versionSetAspects =
        aspectRetriever.getLatestAspectObjects(
            operationContext, versionSetUrns, ImmutableSet.of(VERSION_SET_PROPERTIES_ASPECT_NAME));

    // Resolve each item's version set and, for existing sets whose latest differs from the entity,
    // collect the previous-latest URNs whose VersionProperties must be read.
    Set<Urn> prevLatestUrns = new HashSet<>();
    for (VersionPropertiesContext ctx : contexts) {
      Aspect versionSetPropertiesAspect =
          versionSetAspects
              .getOrDefault(ctx.versionProperties.getVersionSet(), Collections.emptyMap())
              .get(VERSION_SET_PROPERTIES_ASPECT_NAME);
      ctx.versionSetPropertiesAspect = versionSetPropertiesAspect;
      if (versionSetPropertiesAspect != null) {
        VersionSetProperties versionSetProperties =
            RecordUtils.toRecordTemplate(
                VersionSetProperties.class, versionSetPropertiesAspect.data());
        ctx.prevLatest = versionSetProperties.getLatest();
        if (!ctx.prevLatest.equals(ctx.item.getUrn())) {
          prevLatestUrns.add(ctx.prevLatest);
        }
      }
    }

    // Batched read #2: VersionProperties for every previous-latest entity.
    Map<Urn, Map<String, Aspect>> prevLatestAspects =
        prevLatestUrns.isEmpty()
            ? Collections.emptyMap()
            : aspectRetriever.getLatestAspectObjects(
                operationContext, prevLatestUrns, ImmutableSet.of(VERSION_PROPERTIES_ASPECT_NAME));

    // Final pass: compute side-effect MCPs from the pre-fetched aspects, preserving input order.
    return contexts.stream().flatMap(ctx -> processMCP(ctx, prevLatestAspects, retrieverContext));
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  private static Stream<ChangeMCP> processMCP(
      VersionPropertiesContext ctx,
      Map<Urn, Map<String, Aspect>> prevLatestAspects,
      @Nonnull RetrieverContext retrieverContext) {
    ChangeMCP changeMCP = ctx.item;
    VersionProperties versionProperties = ctx.versionProperties;
    Urn entityUrn = changeMCP.getUrn();

    if (ctx.versionSetPropertiesAspect == null) {
      return createVersionSet(versionProperties, changeMCP, retrieverContext);
    }

    // Version set exists -- only update if there is a new latest
    Urn prevLatest = ctx.prevLatest;
    if (prevLatest.equals(entityUrn)) {
      return Stream.empty();
    }

    VersionProperties prevLatestVersionProperties = null;
    Aspect prevLatestVersionPropertiesAspect =
        prevLatestAspects
            .getOrDefault(prevLatest, Collections.emptyMap())
            .get(VERSION_PROPERTIES_ASPECT_NAME);
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

    // Set isLatest=true via a separate ChangeItemImpl rather than mutating versionProperties
    // in-place. In-place mutation contaminates the shared DataMap on ChangeItemImpl.recordTemplate;
    // on transaction retry validateProposedAspects re-runs on the same ChangeItemImpl and would
    // then see isLatest=true and incorrectly reject the proposal.
    ChangeMCP setIsLatest =
        buildSetIsLatestChange(entityUrn, versionProperties, changeMCP, retrieverContext);

    return Stream.of(createVersionSetKey, createVersionSetProperties, setIsLatest);
  }

  private static Stream<ChangeMCP> updateVersionSetLatest(
      @Nonnull VersionProperties versionProperties,
      @Nullable VersionProperties prevLatestVersionProperties,
      @Nonnull Urn prevLatest,
      ChangeMCP changeMCP,
      @Nonnull RetrieverContext retrieverContext) {
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

    // See createVersionSet for why we use a separate ChangeItemImpl rather than mutating
    // versionProperties in-place.
    ChangeMCP setIsLatest =
        buildSetIsLatestChange(entityUrn, versionProperties, changeMCP, retrieverContext);

    if (prevLatestVersionProperties == null) {
      return Stream.of(updateVersionSetProperties, setIsLatest);
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

    return Stream.of(updateVersionSetProperties, updateOldLatestVersionProperties, setIsLatest);
  }

  /**
   * Builds a new ChangeItemImpl that writes the entity's versionProperties with isLatest=true,
   * using a shallow copy of the DataMap so the original ChangeItemImpl.recordTemplate is not
   * mutated.
   */
  private static ChangeMCP buildSetIsLatestChange(
      @Nonnull Urn entityUrn,
      @Nonnull VersionProperties versionProperties,
      @Nonnull ChangeMCP changeMCP,
      @Nonnull RetrieverContext retrieverContext) {
    VersionProperties withIsLatest =
        new VersionProperties(new DataMap(versionProperties.data())).setIsLatest(true);
    return ChangeItemImpl.builder()
        .urn(entityUrn)
        .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
        .changeType(ChangeType.UPSERT)
        .recordTemplate(withIsLatest)
        .auditStamp(changeMCP.getAuditStamp())
        .systemMetadata(changeMCP.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever());
  }

  /** Per-item state carried between the batched-read passes of {@link #applyMCPSideEffect}. */
  private static final class VersionPropertiesContext {
    private final ChangeMCP item;
    private final VersionProperties versionProperties;
    @Nullable private Aspect versionSetPropertiesAspect;
    @Nullable private Urn prevLatest;

    private VersionPropertiesContext(ChangeMCP item, VersionProperties versionProperties) {
      this.item = item;
      this.versionProperties = versionProperties;
    }
  }
}
