package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Records, in {@code _meta.migration}, whether the migration that populates a V3 index from primary
 * storage has finished: the fill, and the catch-up that replays what was written during it.
 *
 * <p>Phase 1 and catch-up state are scoped to the deploying version, so a later deploy sees neither
 * an in-progress fill row nor a half-replayed catch-up window. An index that already exists, holds
 * documents, and matches the target mappings is otherwise indistinguishable from a complete copy of
 * primary storage and would be left as it is. The mark is on the index, which survives that version
 * change: {@code incomplete} from the moment the fill's index is created until catch-up has
 * replayed the fill window, then {@code complete}. A later deploy rebuilds an index still marked
 * incomplete; it never needs the earlier version's rows. An index that never went through this
 * migration has no mark and is left alone.
 */
public final class IndexMigrationStamp {

  public static final String KEY = "migration";
  public static final String INCOMPLETE = "incomplete";
  public static final String COMPLETE = "complete";

  private IndexMigrationStamp() {}

  public static boolean incomplete(@Nullable Map<String, Object> mappings) {
    return INCOMPLETE.equals(read(mappings).orElse(null));
  }

  public static boolean complete(@Nullable Map<String, Object> mappings) {
    return COMPLETE.equals(read(mappings).orElse(null));
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
