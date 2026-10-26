package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Entry rules for lifecycle stages, shared by the user-facing set, propose and accept paths so they
 * always agree on which transitions are allowed, and by {@code LifecycleStageTransitionHook}, which
 * enforces it on every write.
 */
public final class LifecycleStages {

  public static final String STAGE_ENTITY_NAME = "lifecycleStageType";
  public static final String STAGE_INFO_ASPECT_NAME = "lifecycleStageTypeInfo";

  /** Stands for "no lifecycle stage set" in {@code allowedPreviousStages}. */
  public static final String NO_STAGE_SENTINEL = "urn:li:lifecycleStageType:__NONE__";

  private LifecycleStages() {}

  /**
   * Whether an entity in {@code currentStage} (null = no stage) may enter {@code proposedStage}. A
   * missing policy or a missing list means open entry; an empty list means unreachable. Staying in
   * the same stage is always allowed.
   */
  public static boolean isTransitionAllowed(
      @Nullable final Urn currentStage,
      @Nonnull final Urn proposedStage,
      @Nullable final LifecycleStageTransitionPolicy policy) {
    if (proposedStage.equals(currentStage)) {
      return true;
    }
    if (policy == null || !policy.hasAllowedPreviousStages()) {
      return true;
    }
    final String currentKey = currentStage == null ? NO_STAGE_SENTINEL : currentStage.toString();
    return policy.getAllowedPreviousStages().stream()
        .anyMatch(allowed -> allowed.toString().equals(currentKey));
  }
}
