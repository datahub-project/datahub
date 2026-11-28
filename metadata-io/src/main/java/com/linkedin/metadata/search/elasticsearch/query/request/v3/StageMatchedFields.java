package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchHitMatches;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Matched fields for a ranking stage. The scan reads the fetched source of the fields the caller
 * names, including a label whose value is stored on a path copied into that label. It does not read
 * the highlight set, and it does not decide what the response page reports.
 */
public final class StageMatchedFields {
  private static final ThreadLocal<Map<String, Stash>> PENDING = new ThreadLocal<>();

  private StageMatchedFields() {}

  public static void stash(
      @Nonnull String urn,
      @Nonnull Map<String, Object> source,
      @Nonnull MatchedFields matcher,
      @Nonnull Map<String, String> labels) {
    if (!HighlightCarry.isDeferred()) {
      return;
    }
    Map<String, Stash> rows = PENDING.get();
    if (rows == null) {
      rows = new LinkedHashMap<>();
      PENDING.set(rows);
    }
    rows.put(urn, new Stash(source, matcher, labels));
  }

  /**
   * Sets matched fields from {@code fields} on hits stashed for this search. A hit with no stashed
   * source keeps the matches it already has.
   */
  public static void fill(
      @Nonnull Collection<SearchEntity> entities, @Nonnull List<String> fields) {
    Map<String, Stash> rows = PENDING.get();
    if (rows == null || rows.isEmpty() || fields.isEmpty()) {
      return;
    }
    for (SearchEntity entity : entities) {
      if (entity.getEntity() == null) {
        continue;
      }
      Stash stash = rows.get(entity.getEntity().toString());
      if (stash == null) {
        continue;
      }
      entity.setMatchedFields(new MatchedFieldArray(scan(stash, fields).matched()));
    }
  }

  public static void clear() {
    PENDING.remove();
  }

  @Nonnull
  private static SearchHitMatches scan(@Nonnull Stash stash, @Nonnull List<String> fields) {
    Map<String, List<String>> pathsByName = new LinkedHashMap<>();
    for (String field : fields) {
      pathsByName.putIfAbsent(field, new ArrayList<>(List.of(field)));
    }
    for (Map.Entry<String, String> label : stash.labels.entrySet()) {
      List<String> paths = pathsByName.get(label.getValue());
      if (paths != null && !pathsByName.containsKey(label.getKey())) {
        paths.add(label.getKey());
      }
    }
    return new SearchHitMatches(
        stash.matcher.firstPerName(stash.source, pathsByName, false).matched(), List.of());
  }

  private record Stash(
      @Nonnull Map<String, Object> source,
      @Nonnull MatchedFields matcher,
      @Nonnull Map<String, String> labels) {}
}
