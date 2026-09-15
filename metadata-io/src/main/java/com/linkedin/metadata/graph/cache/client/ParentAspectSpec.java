package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** Aspect-based parent lookup for one entity type in a hierarchy graph. */
@Value
@Builder
public class ParentAspectSpec {

  @Nonnull String entityType;
  @Nonnull String aspectName;
  @Nonnull ParentExtractor parentExtractor;

  /** Extracts the parent URN from aspect payload data. */
  @FunctionalInterface
  public interface ParentExtractor {
    @Nonnull
    Optional<Urn> extractParent(@Nonnull DataMap aspectData);
  }
}
