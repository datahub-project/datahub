package com.linkedin.metadata.aspect.hooks;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * MutationHook that validates lifecycle stage transitions on the Status aspect.
 *
 * <p>When a Status aspect is proposed with a {@code lifecycleStage} URN, this hook looks up the
 * <em>proposed</em> stage's {@link LifecycleStageTransitionPolicy} and checks whether the entity's
 * current stage is in the {@code allowedPreviousStages} list.
 *
 * <p>Entry constraints are modeled on the destination stage so that adding a new stage is
 * self-contained — no edits to existing stages required.
 *
 * <ul>
 *   <li>{@code allowedPreviousStages == null}: any prior stage can enter (no enforcement)
 *   <li>{@code allowedPreviousStages == []}: nothing can enter (unreachable stage)
 *   <li>Use {@code urn:li:lifecycleStageType:__NONE__} as a sentinel to allow entry from the
 *       default active state (no stage set)
 * </ul>
 *
 * <p>Invalid transitions are silently reverted: the proposed {@code lifecycleStage} is reset to the
 * entity's current value and a WARN is logged. Uses {@link MutationHook#writeMutation} so the
 * revert is applied in-place before persistence — the same pattern as {@code
 * CorpUserEditableInfoMutator}.
 *
 * <p>All required data is read through {@link RetrieverContext} at call time, following DataHub's
 * idiomatic hook pattern and avoiding Spring context ordering issues.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class LifecycleStageTransitionHook extends MutationHook {

  /** Aspect name for lifecycle stage type info. */
  private static final String LIFECYCLE_STAGE_TYPE_INFO_ASPECT = "lifecycleStageTypeInfo";

  /**
   * Data-map key for the lifecycleStage field in the Status aspect. Used to remove the field when
   * reverting to the null/active state.
   */
  private static final String LIFECYCLE_STAGE_FIELD = "lifecycleStage";

  /** Sentinel URN representing "no lifecycle stage" (default active state). */
  private static final String NO_STAGE_SENTINEL = "urn:li:lifecycleStageType:__NONE__";

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    List<Pair<ChangeMCP, Boolean>> results = new ArrayList<>();
    for (ChangeMCP item : changeMCPS) {
      results.add(Pair.of(item, revertIfInvalidTransition(item, retrieverContext)));
    }
    return results.stream();
  }

  /**
   * Checks whether the proposed lifecycle stage transition is valid. If not, reverts the {@code
   * lifecycleStage} field in the proposed Status aspect to its previous value (in-place mutation),
   * logs a WARN, and returns {@code true} to indicate a mutation occurred.
   */
  private boolean revertIfInvalidTransition(ChangeMCP item, RetrieverContext retrieverContext) {
    if (!Constants.STATUS_ASPECT_NAME.equals(item.getAspectName())) {
      return false;
    }

    Status proposedStatus = item.getAspect(Status.class);
    if (proposedStatus == null || !proposedStatus.hasLifecycleStage()) {
      // Clearing the stage (→ null/active) is always allowed.
      return false;
    }

    Urn proposedStageUrn = proposedStatus.getLifecycleStage();

    // Current stage from the previous aspect (already loaded by the write pipeline).
    Status previousStatus = item.getPreviousAspect(Status.class);
    Urn currentStageUrn =
        previousStatus != null && previousStatus.hasLifecycleStage()
            ? previousStatus.getLifecycleStage()
            : null;

    // Same stage → no-op, allow.
    if (proposedStageUrn.equals(currentStageUrn)) {
      return false;
    }

    // Look up the PROPOSED (destination) stage's entry policy via RetrieverContext.
    LifecycleStageTransitionPolicy policy = getTransitionPolicy(proposedStageUrn, retrieverContext);
    if (policy == null || !policy.hasAllowedPreviousStages()) {
      // No policy on destination stage → open entry, allow.
      return false;
    }

    Set<String> allowed =
        Set.copyOf(policy.getAllowedPreviousStages().stream().map(Urn::toString).toList());

    String currentStageKey =
        currentStageUrn == null ? NO_STAGE_SENTINEL : currentStageUrn.toString();

    if (!allowed.contains(currentStageKey)) {
      log.warn(
          "Lifecycle stage transition to {} from {} on {} is not allowed. "
              + "Allowed previous stages: {}. Reverting to previous stage.",
          proposedStageUrn,
          currentStageUrn == null ? "(none/active)" : currentStageUrn,
          item.getUrn(),
          allowed);

      // Revert: restore the previous lifecycleStage in the proposed Status aspect.
      if (currentStageUrn != null) {
        proposedStatus.setLifecycleStage(currentStageUrn);
      } else {
        // Remove the field to restore the null/active state.
        proposedStatus.data().remove(LIFECYCLE_STAGE_FIELD);
      }
      return true;
    }

    return false;
  }

  /**
   * Returns the {@link LifecycleStageTransitionPolicy} for the given stage URN by reading the
   * {@code lifecycleStageTypeInfo} aspect through the {@link RetrieverContext}.
   *
   * <p>Returns {@code null} if the stage is not found or defines no policy — meaning open entry.
   */
  @Nullable
  private LifecycleStageTransitionPolicy getTransitionPolicy(
      Urn stageUrn, RetrieverContext retrieverContext) {
    try {
      var stageAspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(stageUrn, LIFECYCLE_STAGE_TYPE_INFO_ASPECT);
      if (stageAspect == null) {
        log.warn("Lifecycle stage {} not found in aspect store, allowing transition", stageUrn);
        return null;
      }
      LifecycleStageTypeInfo info = new LifecycleStageTypeInfo(stageAspect.data());
      return info.hasTransitionPolicy() ? info.getTransitionPolicy() : null;
    } catch (Exception e) {
      log.warn(
          "Failed to retrieve transition policy for stage {}, allowing transition", stageUrn, e);
      return null;
    }
  }
}
