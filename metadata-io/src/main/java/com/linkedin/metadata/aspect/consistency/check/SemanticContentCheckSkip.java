package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;

/**
 * Decides whether an entity is excluded from {@link SemanticContentIndexMismatchCheck}. OSS
 * registers none; extensions register a bean when an entity's {@code semanticContent} is
 * intentionally omitted from Search V3.
 */
@FunctionalInterface
public interface SemanticContentCheckSkip {
  boolean skip(@Nonnull Urn urn);
}
