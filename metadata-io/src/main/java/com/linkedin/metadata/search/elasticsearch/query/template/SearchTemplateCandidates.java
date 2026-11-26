package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.rescore.SignalDocument;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;

/**
 * Main-window documents the rescore reads, and the supplementary merge that runs after it. The
 * document lookup converts a hit the first time the rescore asks for it.
 */
public final class SearchTemplateCandidates {
  public static final SearchTemplateCandidates NONE =
      new SearchTemplateCandidates(ignored -> null, SupplementaryMerge.NONE);

  private final Function<String, SignalDocument> documents;
  private final SupplementaryMerge merge;

  public SearchTemplateCandidates(
      @Nonnull Function<String, SignalDocument> documents, @Nonnull SupplementaryMerge merge) {
    this.documents = documents;
    this.merge = merge;
  }

  @Nonnull
  public Function<String, SignalDocument> documents() {
    return documents;
  }

  @Nonnull
  public SupplementaryMerge merge() {
    return merge;
  }

  /** Keys hits by the URL-encoded URN. A hit without a URN is skipped. */
  @Nonnull
  public static Function<String, SignalDocument> lookup(@Nullable SearchResponse response) {
    if (response == null || response.getHits() == null || response.getHits().getHits() == null) {
      return ignored -> null;
    }
    Map<String, SearchHit> hits = new HashMap<>();
    for (SearchHit hit : response.getHits().getHits()) {
      try {
        Urn urn = SearchRequestHandler.urnFromSearchHit(hit);
        hits.put(URLEncoder.encode(urn.toString(), StandardCharsets.UTF_8), hit);
      } catch (RuntimeException ignored) {
        // The rescore leaves a hit it cannot key in its original place.
      }
    }
    Map<String, SearchHit> frozen = Map.copyOf(hits);
    return id -> {
      SearchHit hit = frozen.get(id);
      return hit == null ? null : SignalDocument.from(hit, hit.getScore());
    };
  }

  @FunctionalInterface
  public interface SupplementaryMerge {
    SupplementaryMerge NONE = ignored -> {};

    void merge(@Nonnull SearchResult working);
  }
}
