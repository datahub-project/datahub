package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Records, in {@code _meta.v3Cutover} on a V2 entity index, that reads have moved to its paired V3
 * index.
 *
 * <p>The read hold decides once per process from the V3 migration mark. After the cutover the V2
 * indices stay on the cluster and stop receiving writes, and a later reindex of a V3 index builds
 * its destination without that mark. A start that sees only the missing mark would serve the V2
 * copy as it was at the cutover. The mark on the V2 index survives that reindex, so a later start
 * does not hold.
 */
public final class SearchV3CutoverStamp {

  public static final String KEY = "v3Cutover";
  public static final String RETIRED = "retired";

  private SearchV3CutoverStamp() {}

  public static boolean retired(@Nullable Map<String, Object> mappings) {
    return RETIRED.equals(read(mappings).orElse(null));
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
   * {@code _meta} for a put-mapping that retires the index, keeping every other entry. Put-mapping
   * replaces {@code _meta} as a whole.
   */
  @Nonnull
  public static Map<String, Object> retiredMeta(@Nullable Map<String, Object> mappings) {
    Map<String, Object> meta = new HashMap<>();
    if (mappings != null && mappings.get(V3DocumentIdScheme.META) instanceof Map<?, ?> existing) {
      existing.forEach((key, value) -> meta.put(String.valueOf(key), value));
    }
    meta.put(KEY, RETIRED);
    return meta;
  }
}
