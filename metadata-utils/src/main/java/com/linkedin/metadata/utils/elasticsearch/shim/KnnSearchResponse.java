package com.linkedin.metadata.utils.elasticsearch.shim;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @param partial whether the engine reported a timeout or failed shards, so hits may be missing
 */
public record KnnSearchResponse(@Nonnull List<Hit> hits, boolean partial) {

  public KnnSearchResponse {
    hits = List.copyOf(hits);
  }

  public KnnSearchResponse(@Nonnull List<Hit> hits) {
    this(hits, false);
  }

  public boolean isEmpty() {
    return hits.isEmpty();
  }

  /** A matched passage; position is the stored chunk position, not its rank in the response. */
  public record MatchedChunk(
      @Nonnull String text, @Nullable Double score, @Nullable Integer position) {}

  /**
   * A single kNN search result.
   *
   * <p>The {@code source} parameter of the constructor accepts {@code null} as a convenience for
   * callers that receive hits without a {@code _source} field (e.g. when source fetching is
   * disabled). A {@code null} argument is silently coerced to an empty unmodifiable map, so {@link
   * #source()} never returns {@code null}.
   *
   * <p>{@code matchedChunks} carries the best-matching nested chunk(s) extracted from {@code
   * inner_hits}. When inner_hits are absent (unsupported search engine versions, keyword search),
   * this list is empty — never null.
   */
  public record Hit(
      @Nonnull String id,
      double score,
      @Nonnull Map<String, Object> source,
      @Nonnull List<MatchedChunk> matchedChunks) {

    public Hit {
      // Defensive coercion: null input → empty map so callers never observe a null source.
      // Map.copyOf rejects null values; use an unmodifiable HashMap copy to preserve null fields.
      source = source == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(source));
      matchedChunks = matchedChunks == null ? List.of() : List.copyOf(matchedChunks);
    }

    public List<String> matchedChunkTexts() {
      return matchedChunks.stream().map(MatchedChunk::text).toList();
    }

    /** Convenience constructor for callers that don't have inner_hits (backward-compatible). */
    public Hit(@Nonnull String id, double score, @Nonnull Map<String, Object> source) {
      this(id, score, source, List.of());
    }
  }
}
