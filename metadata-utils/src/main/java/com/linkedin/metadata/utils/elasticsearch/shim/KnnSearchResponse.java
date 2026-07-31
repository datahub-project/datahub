package com.linkedin.metadata.utils.elasticsearch.shim;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public record KnnSearchResponse(@Nonnull List<Hit> hits) {

  public KnnSearchResponse {
    hits = List.copyOf(hits);
  }

  public boolean isEmpty() {
    return hits.isEmpty();
  }

  /**
   * A single kNN search result.
   *
   * <p>The {@code source} parameter of the constructor accepts {@code null} as a convenience for
   * callers that receive hits without a {@code _source} field (e.g. when source fetching is
   * disabled). A {@code null} argument is silently coerced to an empty unmodifiable map, so {@link
   * #source()} never returns {@code null}.
   */
  public record Hit(@Nonnull String id, double score, @Nonnull Map<String, Object> source) {

    public Hit {
      // Defensive coercion: null input → empty map so callers never observe a null source.
      // Map.copyOf rejects null values; use an unmodifiable HashMap copy to preserve null fields.
      source = source == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(source));
    }
  }
}
