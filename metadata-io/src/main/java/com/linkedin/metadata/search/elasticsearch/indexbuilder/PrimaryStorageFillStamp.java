package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Records, in {@code _meta.primaryStorageFill}, whether a V3 index filled from primary storage has
 * finished.
 *
 * <p>Phase 1 state is scoped to the deploying version, so a later deploy does not see an
 * in-progress fill row. An index that already exists, holds documents, and matches the target
 * mappings is otherwise indistinguishable from a finished corpus and would be left unfilled. The
 * stamp is on the index, which survives that version change: {@code incomplete} until the fill
 * finishes, then {@code complete}. An index that never went through this fill has no stamp and is
 * left alone.
 */
public final class PrimaryStorageFillStamp {

  public static final String KEY = "primaryStorageFill";
  public static final String INCOMPLETE = "incomplete";
  public static final String COMPLETE = "complete";

  private PrimaryStorageFillStamp() {}

  public static boolean incomplete(@Nullable Map<String, Object> mappings) {
    return INCOMPLETE.equals(read(mappings).orElse(null));
  }

  @Nonnull
  public static Optional<String> read(@Nullable Map<String, Object> mappings) {
    if (mappings == null || !(mappings.get(V3DocumentIdScheme.META) instanceof Map<?, ?> meta)) {
      return Optional.empty();
    }
    Object value = meta.get(KEY);
    return value == null ? Optional.empty() : Optional.of(value.toString());
  }

  /**
   * A copy of {@code mappings} whose {@code _meta} records {@code state}, keeping other entries.
   */
  @Nonnull
  public static Map<String, Object> stamped(
      @Nonnull Map<String, Object> mappings, @Nonnull String state) {
    Map<String, Object> meta = new HashMap<>();
    if (mappings.get(V3DocumentIdScheme.META) instanceof Map<?, ?> existing) {
      existing.forEach((key, value) -> meta.put(String.valueOf(key), value));
    }
    meta.put(KEY, state);
    Map<String, Object> stamped = new HashMap<>(mappings);
    stamped.put(V3DocumentIdScheme.META, meta);
    return stamped;
  }
}
