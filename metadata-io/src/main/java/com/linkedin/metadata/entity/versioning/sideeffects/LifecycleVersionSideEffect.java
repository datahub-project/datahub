package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.*;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Status;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
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
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.versionset.VersionSetProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * MCPSideEffect that maintains the "effective latest" visible version when lifecycle stages change.
 *
 * <p>When the entity marked {@code isLatest=true} transitions into a hidden lifecycle stage, this
 * side effect:
 *
 * <ol>
 *   <li>Demotes the current entity ({@code isLatest=false}).
 *   <li>Promotes the most recent non-hidden sibling ({@code isLatest=true}).
 *   <li>Updates {@code VersionSetProperties.latest} to the promoted sibling, so that any
 *       subsequently-created version (v4) compares its sortId against the correct effective-latest
 *       rather than against the now-hidden entity.
 * </ol>
 *
 * <p>When the entity returns to a visible stage, it is re-promoted if it has the highest sortId
 * among all visible siblings. The demotion of whoever currently holds {@code isLatest=true} is also
 * emitted, and {@link VersionPropertiesSideEffect} naturally re-synchronises {@code
 * VersionSetProperties.latest} when it sees the higher-sortId entity being promoted.
 *
 * <p><strong>Cascade safety:</strong> updating {@code VersionSetProperties} triggers {@link
 * VersionSetSideEffect}, which emits a PATCH setting {@code isLatest=false} on the old latest.
 * Without a guard, {@link VersionPropertiesSideEffect} would then see that patch, notice the old
 * latest's sortId is higher than the new {@code VersionSetProperties.latest}, and re-promote the
 * (now-hidden) entity. To prevent this, {@link VersionPropertiesSideEffect} contains a hidden-stage
 * check that short-circuits before calling {@code updateVersionSetLatest} when the entity is in a
 * hidden lifecycle stage.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class LifecycleVersionSideEffect extends MCPSideEffect {

  private static final String LIFECYCLE_STAGE_TYPE_INFO_ASPECT = "lifecycleStageTypeInfo";

  /**
   * Search flags for finding all sibling versions, including those in hidden lifecycle stages. Used
   * when we need to discover which versions exist regardless of their current visibility.
   */
  private static final SearchFlags FIND_ALL_VERSIONS_FLAGS =
      new SearchFlags()
          .setFulltext(false)
          .setSkipCache(true)
          .setSkipAggregates(true)
          .setSkipHighlighting(true)
          .setIncludeSoftDeleted(false)
          .setIncludeRestricted(false)
          .setFilterNonLatestVersions(false)
          .setIncludeHiddenLifecycleStages(true);

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return changeMCPS.stream()
        .flatMap(item -> processMCP(operationContext, item, retrieverContext));
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  private Stream<ChangeMCP> processMCP(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull ChangeMCP item,
      @Nonnull RetrieverContext retrieverContext) {
    if (!STATUS_ASPECT_NAME.equals(item.getAspectName())) {
      return Stream.empty();
    }

    Status newStatus = item.getAspect(Status.class);
    Status prevStatus = item.getPreviousAspect(Status.class);

    Urn newStageUrn =
        (newStatus != null && newStatus.hasLifecycleStage()) ? newStatus.getLifecycleStage() : null;
    Urn prevStageUrn =
        (prevStatus != null && prevStatus.hasLifecycleStage())
            ? prevStatus.getLifecycleStage()
            : null;

    // No lifecycle stage change → nothing to do
    if (Objects.equals(newStageUrn, prevStageUrn)) {
      return Stream.empty();
    }

    // Only act on versioned entities
    Aspect vpAspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(operationContext, item.getUrn(), VERSION_PROPERTIES_ASPECT_NAME);
    if (vpAspect == null) {
      return Stream.empty();
    }
    VersionProperties vp = new VersionProperties(vpAspect.data());

    boolean newIsHidden = isHiddenStage(operationContext, newStageUrn, retrieverContext);
    boolean prevIsHidden = isHiddenStage(operationContext, prevStageUrn, retrieverContext);

    if (newIsHidden && !prevIsHidden && Boolean.TRUE.equals(vp.isIsLatest())) {
      // Effective latest is going into a hidden stage → demote it, promote the next visible sibling
      return handleLatestGoingHidden(operationContext, item, vp, retrieverContext);
    }

    if (!newIsHidden && prevIsHidden && !Boolean.TRUE.equals(vp.isIsLatest())) {
      // Entity is returning from a hidden stage → re-promote if it is the best visible sibling
      return handleReturningFromHidden(operationContext, item, vp, retrieverContext);
    }

    return Stream.empty();
  }

  /**
   * Demotes the current effective-latest entity, promotes the most recent non-hidden sibling, and
   * updates {@code VersionSetProperties.latest}. When no visible sibling exists (all versions
   * hidden), only the demotion is emitted and the entity becomes invisible in search.
   *
   * <p>The {@code VersionSetProperties} update is required so that if a new version (v4) is created
   * while the original latest (v3) is hidden, {@link VersionPropertiesSideEffect} compares v4's
   * sortId against the promoted sibling (v2) rather than the hidden v3.
   */
  private Stream<ChangeMCP> handleLatestGoingHidden(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull ChangeMCP item,
      @Nonnull VersionProperties vp,
      @Nonnull RetrieverContext retrieverContext) {

    Urn entityUrn = item.getUrn();
    Urn versionSetUrn = vp.getVersionSet();
    String entityType = entityUrn.getEntityType();

    List<SearchEntity> siblings =
        fetchAllVersionsSortedDesc(entityType, versionSetUrn, retrieverContext);

    // Find the best (highest-sortId) visible sibling, excluding the entity going hidden
    Urn bestCandidateUrn = null;
    VersionProperties bestCandidateVp = null;

    for (SearchEntity sibling : siblings) {
      Urn siblingUrn = sibling.getEntity();
      if (siblingUrn.equals(entityUrn)) {
        continue; // skip — this entity is going hidden
      }
      Status siblingStatus = getSiblingStatus(operationContext, siblingUrn, retrieverContext);
      Urn siblingStageUrn =
          (siblingStatus != null && siblingStatus.hasLifecycleStage())
              ? siblingStatus.getLifecycleStage()
              : null;
      if (!isHiddenStage(operationContext, siblingStageUrn, retrieverContext)) {
        Aspect candidateVpAspect =
            retrieverContext
                .getAspectRetriever()
                .getLatestAspectObject(
                    operationContext, siblingUrn, VERSION_PROPERTIES_ASPECT_NAME);
        if (candidateVpAspect != null) {
          bestCandidateUrn = siblingUrn;
          bestCandidateVp = new VersionProperties(candidateVpAspect.data());
          break;
        }
      }
    }

    List<ChangeMCP> results = new ArrayList<>();
    results.add(buildDemoteChange(operationContext, entityUrn, vp, item, retrieverContext));

    if (bestCandidateUrn != null) {
      results.add(
          buildPromoteChange(
              operationContext, bestCandidateUrn, bestCandidateVp, item, retrieverContext));
      results.add(
          buildUpdateVersionSetLatest(
              operationContext,
              versionSetUrn,
              bestCandidateUrn,
              bestCandidateVp,
              item,
              retrieverContext));
    } else {
      log.info(
          "No visible sibling found for versioned entity {}; it will be invisible in search",
          entityUrn);
    }

    return results.stream();
  }

  /**
   * Re-promotes a versioned entity that is returning from a hidden stage, if it is the
   * highest-sortId visible sibling. Also demotes whoever currently holds {@code isLatest=true}.
   *
   * <p>Also points {@code VersionSetProperties.latest} at the entity. The promotion is emitted from
   * this side effect, so {@link VersionPropertiesSideEffect} never sees it with the new stage
   * applied and cannot re-synchronise the set itself; without this, the set keeps naming the
   * previous version and the next version is created against a stale latest.
   */
  private Stream<ChangeMCP> handleReturningFromHidden(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull ChangeMCP item,
      @Nonnull VersionProperties vp,
      @Nonnull RetrieverContext retrieverContext) {

    Urn entityUrn = item.getUrn();
    Urn versionSetUrn = vp.getVersionSet();
    String entityType = entityUrn.getEntityType();

    List<SearchEntity> siblings =
        fetchAllVersionsSortedDesc(entityType, versionSetUrn, retrieverContext);

    // Walk siblings (sortId DESC) to find: (a) best visible, (b) current effective-latest
    Urn bestVisibleUrn = null;
    Urn currentEffectiveLatestUrn = null;
    VersionProperties currentEffectiveLatestVp = null;

    for (SearchEntity sibling : siblings) {
      Urn siblingUrn = sibling.getEntity();
      Aspect siblingVpAspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(operationContext, siblingUrn, VERSION_PROPERTIES_ASPECT_NAME);
      if (siblingVpAspect == null) {
        continue;
      }
      VersionProperties siblingVp = new VersionProperties(siblingVpAspect.data());

      if (currentEffectiveLatestUrn == null && Boolean.TRUE.equals(siblingVp.isIsLatest())) {
        currentEffectiveLatestUrn = siblingUrn;
        currentEffectiveLatestVp = siblingVp;
      }

      if (bestVisibleUrn == null) {
        if (siblingUrn.equals(entityUrn)) {
          // The incoming MCP transitions this entity to a non-hidden stage; treat it as visible
          bestVisibleUrn = siblingUrn;
        } else {
          Status siblingStatus = getSiblingStatus(operationContext, siblingUrn, retrieverContext);
          Urn siblingStageUrn =
              (siblingStatus != null && siblingStatus.hasLifecycleStage())
                  ? siblingStatus.getLifecycleStage()
                  : null;
          if (!isHiddenStage(operationContext, siblingStageUrn, retrieverContext)) {
            bestVisibleUrn = siblingUrn;
          }
        }
      }

      // Early exit once we have both answers
      if (bestVisibleUrn != null && currentEffectiveLatestUrn != null) {
        break;
      }
    }

    // Only act if this entity is the best visible sibling
    if (bestVisibleUrn == null || !bestVisibleUrn.equals(entityUrn)) {
      return Stream.empty();
    }

    List<ChangeMCP> results = new ArrayList<>();
    if (currentEffectiveLatestUrn != null && !currentEffectiveLatestUrn.equals(entityUrn)) {
      results.add(
          buildDemoteChange(
              operationContext,
              currentEffectiveLatestUrn,
              currentEffectiveLatestVp,
              item,
              retrieverContext));
    }
    results.add(buildPromoteChange(operationContext, entityUrn, vp, item, retrieverContext));
    results.add(
        buildUpdateVersionSetLatest(
            operationContext, versionSetUrn, entityUrn, vp, item, retrieverContext));

    return results.stream();
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private List<SearchEntity> fetchAllVersionsSortedDesc(
      @Nonnull String entityType,
      @Nonnull Urn versionSetUrn,
      @Nonnull RetrieverContext retrieverContext) {
    return retrieverContext
        .getSearchRetriever()
        .scroll(
            ImmutableList.of(entityType),
            QueryUtils.newFilter(
                CriterionUtils.buildCriterion(
                    VERSION_SET_FIELD_NAME, Condition.EQUAL, versionSetUrn.toString())),
            null,
            50,
            ImmutableList.of(
                new SortCriterion()
                    .setField(VERSION_SORT_ID_FIELD_NAME)
                    .setOrder(SortOrder.DESCENDING)),
            FIND_ALL_VERSIONS_FLAGS)
        .getEntities()
        .stream()
        .toList();
  }

  @Nullable
  private Status getSiblingStatus(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Urn siblingUrn,
      @Nonnull RetrieverContext retrieverContext) {
    try {
      Aspect statusAspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(operationContext, siblingUrn, STATUS_ASPECT_NAME);
      return statusAspect != null ? new Status(statusAspect.data()) : null;
    } catch (Exception e) {
      log.warn("Failed to load status for sibling {}, treating as not hidden", siblingUrn, e);
      return null;
    }
  }

  private boolean isHiddenStage(
      @Nonnull OperationFingerprint operationFingerprint,
      @Nullable Urn stageUrn,
      @Nonnull RetrieverContext retrieverContext) {
    if (stageUrn == null) {
      return false;
    }
    try {
      Aspect stageAspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(
                  operationFingerprint, stageUrn, LIFECYCLE_STAGE_TYPE_INFO_ASPECT);
      if (stageAspect == null) {
        return false;
      }
      LifecycleStageTypeInfo info = new LifecycleStageTypeInfo(stageAspect.data());
      return info.getSettings() != null && info.getSettings().isHideInSearch();
    } catch (Exception e) {
      log.warn("Failed to check hideInSearch for stage {}, treating as not hidden", stageUrn, e);
      return false;
    }
  }

  /** Emits a PATCH that sets {@code isLatest=false} on the target entity's VersionProperties. */
  private ChangeMCP buildDemoteChange(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Urn targetUrn,
      @Nonnull VersionProperties currentVp,
      @Nonnull ChangeMCP item,
      @Nonnull RetrieverContext retrieverContext) {
    EntitySpec entitySpec =
        retrieverContext
            .getAspectRetriever()
            .getEntityRegistry()
            .getEntitySpec(targetUrn.getEntityType());
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(PatchOperationType.ADD.getValue());
    patchOp.setPath("/isLatest");
    patchOp.setValue(false);
    return PatchItemImpl.builder()
        .urn(targetUrn)
        .entitySpec(entitySpec)
        .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
        .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
        .patch(GenericJsonPatch.builder().patch(List.of(patchOp)).build().getJsonPatch())
        .auditStamp(item.getAuditStamp())
        .systemMetadata(item.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever().getEntityRegistry())
        .applyPatch(currentVp, retrieverContext.getAspectRetriever());
  }

  /**
   * Emits an UPSERT that sets {@code isLatest=true} on the target entity's VersionProperties.
   *
   * <p>Uses a shallow copy of the DataMap (same pattern as {@link VersionPropertiesSideEffect}) so
   * the original record is not mutated in place.
   */
  private ChangeMCP buildPromoteChange(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Urn targetUrn,
      @Nonnull VersionProperties currentVp,
      @Nonnull ChangeMCP item,
      @Nonnull RetrieverContext retrieverContext) {
    VersionProperties withIsLatest =
        new VersionProperties(new DataMap(currentVp.data())).setIsLatest(true);
    return ChangeItemImpl.builder()
        .urn(targetUrn)
        .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
        .changeType(ChangeType.UPSERT)
        .recordTemplate(withIsLatest)
        .auditStamp(item.getAuditStamp())
        .systemMetadata(item.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever());
  }

  /**
   * Emits an UPSERT that updates {@code VersionSetProperties.latest} to the promoted entity. This
   * ensures that when a new version is created while the original latest is hidden, {@link
   * VersionPropertiesSideEffect} compares sortIds against the correct effective-latest.
   */
  private ChangeMCP buildUpdateVersionSetLatest(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Urn versionSetUrn,
      @Nonnull Urn newLatestUrn,
      @Nonnull VersionProperties newLatestVp,
      @Nonnull ChangeMCP item,
      @Nonnull RetrieverContext retrieverContext) {
    VersionSetProperties updatedVsp =
        new VersionSetProperties()
            .setVersioningScheme(newLatestVp.getVersioningScheme())
            .setLatest(newLatestUrn);
    return ChangeItemImpl.builder()
        .urn(versionSetUrn)
        .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
        .changeType(ChangeType.UPSERT)
        .recordTemplate(updatedVsp)
        .auditStamp(item.getAuditStamp())
        .systemMetadata(item.getSystemMetadata())
        .build(retrieverContext.getAspectRetriever());
  }
}
