package io.datahubproject.test.search;

import java.util.List;
import javax.annotation.Nonnull;

/** Ranked hits for one query. Scorers read this object and do not query the index. */
public final class RankedSearchResult {
  private final String queryId;
  private final String surface;
  private final String query;
  private final List<RankedSearchHit> hits;

  public RankedSearchResult(
      @Nonnull String queryId,
      @Nonnull String surface,
      @Nonnull String query,
      @Nonnull List<RankedSearchHit> hits) {
    this.queryId = queryId;
    this.surface = surface;
    this.query = query;
    this.hits = List.copyOf(hits);
  }

  @Nonnull
  public String getQueryId() {
    return queryId;
  }

  @Nonnull
  public String getSurface() {
    return surface;
  }

  @Nonnull
  public String getQuery() {
    return query;
  }

  @Nonnull
  public List<RankedSearchHit> getHits() {
    return hits;
  }
}
