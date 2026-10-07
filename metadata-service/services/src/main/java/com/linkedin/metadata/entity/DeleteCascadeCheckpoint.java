package com.linkedin.metadata.entity;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Where {@link DeleteEntityService#removeReferencesResumable} resumes: the phase to run. Phases
 * before it are skipped; the phase itself runs again from its start, which is safe because every
 * step is idempotent. Plain data, so a caller that persists progress can store it as {@code
 * {"phase": ...}} and resume from it.
 */
public record DeleteCascadeCheckpoint(@Nonnull String phase) {

  public static final String PHASE_GRAPH = "graph";
  public static final String PHASE_SEARCH_REFERENCES = "searchReferences";
  public static final String PHASE_FILES = "files";

  public DeleteCascadeCheckpoint {
    Objects.requireNonNull(phase, "phase");
  }
}
