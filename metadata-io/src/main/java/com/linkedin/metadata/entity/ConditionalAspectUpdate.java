package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.SystemAspect;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Input carrier for a single CAS update in a batch. Used by {@link
 * AspectDao#updateAspectsConditionalBatch}.
 */
@Getter
@RequiredArgsConstructor
public final class ConditionalAspectUpdate {
  @Nonnull private final SystemAspect newAspect;
  @Nullable private final String expectedSystemMetadataVersion;
}
