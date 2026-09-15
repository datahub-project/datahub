package com.linkedin.metadata.entity.retention;

import java.util.Objects;

/**
 * OSS default {@link RetentionKey}: an {@code (urn, aspectName)} pair. Equality is {@code (urn,
 * aspectName)} only — there is no routing metadata to distinguish two requests for the same URN, so
 * they coalesce into one buffer entry.
 *
 * <p>An extension module that routes to different underlying databases should provide its own
 * {@link RetentionKey} implementation whose equality includes the routing metadata, plus a {@link
 * RetentionContextResolver} that produces it.
 */
public record SimpleRetentionKey(String urn, String aspectName) implements RetentionKey {

  private static final long serialVersionUID = 1L;

  public SimpleRetentionKey {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(aspectName, "aspectName must not be null");
  }
}
