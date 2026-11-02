package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import com.linkedin.metadata.search.ExplainMatch;
import com.linkedin.metadata.search.ExplainMatchArray;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchHitMatches;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Holds opted-in source values for one search until the final page is known. Matching runs in
 * {@link #apply}, so rows discarded by reranking are not scanned.
 */
public final class HighlightCarry {
  private static final ThreadLocal<Pending> PENDING = new ThreadLocal<>();

  private HighlightCarry() {}

  public static void begin() {
    PENDING.set(new Pending());
  }

  public static boolean isDeferred() {
    return PENDING.get() != null;
  }

  public static void stash(
      @Nonnull String urn, @Nonnull Function<Boolean, SearchHitMatches> reporter) {
    Pending pending = PENDING.get();
    if (pending != null) {
      pending.rows.put(urn, reporter);
    }
  }

  public static void apply(@Nonnull Collection<SearchEntity> entities, boolean explain) {
    Pending pending = PENDING.get();
    if (pending == null || pending.rows.isEmpty()) {
      return;
    }
    for (SearchEntity entity : entities) {
      if (entity.getEntity() == null) {
        continue;
      }
      Function<Boolean, SearchHitMatches> reporter =
          pending.rows.get(entity.getEntity().toString());
      if (reporter == null) {
        continue;
      }
      SearchHitMatches matches = reporter.apply(explain);
      entity.setMatchedFields(new MatchedFieldArray(matches.matched()));
      if (explain && !matches.explain().isEmpty()) {
        ExplainMatchArray explained = new ExplainMatchArray();
        for (MatchEvidence evidence : matches.explain()) {
          explained.add(
              new ExplainMatch()
                  .setField(evidence.field())
                  .setValue(evidence.value())
                  .setSubfield(evidence.subfield())
                  .setMode(evidence.mode())
                  .setQuery(evidence.query()));
        }
        entity.setExplainMatches(explained);
      }
    }
  }

  public static void clear() {
    PENDING.remove();
  }

  private static final class Pending {
    private final Map<String, Function<Boolean, SearchHitMatches>> rows = new LinkedHashMap<>();
  }
}
