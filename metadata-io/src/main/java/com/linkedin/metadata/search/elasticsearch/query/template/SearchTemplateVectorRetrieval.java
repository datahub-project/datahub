package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.search.VectorStoreConfiguration;
import com.linkedin.metadata.search.hybrid.v3.V3HybridKnnRequestBuilder;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The window a {@code combine: vector} rung retrieves and the outcomes it reports. The rung pages
 * one window of nearest neighbors from offset 0; the floors drop hits from that window and never
 * refill it.
 */
public final class SearchTemplateVectorRetrieval {
  /** Changes when a vector rung serves a request differently, so cached results are not shared. */
  public static final int EXECUTION_VERSION = 1;

  /** Neighbors one native kNN request returns. A larger {@code candidateCap} is clamped to it. */
  public static final int MAX_K = V3HybridKnnRequestBuilder.MAX_K;

  // Without candidateCap, the window SemanticEntitySearchService fetches for a page
  private static final int PAGE_WINDOW_MAX = 500;
  private static final double PAGE_OVERSAMPLE = 1.2d;

  /** The page has hits, or the request asked only for the total and facets. */
  public static final String SERVED = "served";

  /** No hit survived: the store found none, or the floors dropped them all. */
  public static final String EMPTY = "empty";

  /** Hits survived, and the page starts past them. */
  public static final String PAST_WINDOW = "pastWindow";

  /** No requested entity type passes the entity gates, so nothing was embedded or searched. */
  public static final String NO_ELIGIBLE_ENTITIES = "noEligibleEntities";

  /** {@code candidateCap} was above {@link #MAX_K} and was clamped. */
  public static final String CAP_CLAMPED = "capClamped";

  /** A floor dropped at least one hit. */
  public static final String FLOOR_DROPPED = "floorDropped";

  /** The embedding, kNN, or facet request failed. */
  public static final String FAILED = "failed";

  /** This process cannot run a vector rung: semantic read is off or no embedding provider. */
  public static final String UNAVAILABLE = "unavailable";

  /** The request has a caller filter, and the store serving the profile cannot apply it. */
  public static final String NO_PRE_FILTER = "noPreFilter";

  /** The floor inherited from configuration was skipped: the profile's metric is not cosine. */
  public static final String PROFILE_NOT_COSINE = "profileNotCosine";

  public static final String MIN_SCORE_DEPRECATION =
      "SearchFlags.minScore is deprecated as a vector leg floor; use retrieval.minSimilarity";

  private SearchTemplateVectorRetrieval() {}

  /**
   * Neighbors to request: {@code candidateCap}, else the window semantic search fetches for the
   * page, {@code max(size, min(500, ceil((from + size) * 1.2)))}. Either is clamped to {@link
   * #MAX_K}, and is at least one. A count-only request (size 0) counts every candidate a page could
   * reach: the cap, else 500.
   */
  public static int window(@Nullable Integer candidateCap, int from, int size) {
    if (candidateCap != null) {
      return Math.max(1, Math.min(candidateCap, MAX_K));
    }
    if (size == 0) {
      return PAGE_WINDOW_MAX;
    }
    int oversampled = (int) Math.ceil(((long) from + size) * PAGE_OVERSAMPLE);
    return Math.max(1, Math.min(MAX_K, Math.max(size, Math.min(PAGE_WINDOW_MAX, oversampled))));
  }

  /**
   * The system that serves a vector rung: the engine serving the search when the rung's profiles
   * join there, else the first external store, by name, they join on. When they join nowhere, it is
   * the engine, and the entity gates report why.
   */
  @Nullable
  public static String system(
      @Nonnull SearchTemplateGates gates,
      @Nonnull MergedSearchTemplate template,
      @Nonnull Collection<String> entityTypes,
      @Nullable String engine,
      @Nonnull Map<String, VectorStoreConfiguration> stores) {
    if (stores.isEmpty() || !gates.joinedProfileIds(template, entityTypes, engine).isEmpty()) {
      return engine;
    }
    for (String store : new TreeSet<>(stores.keySet())) {
      if (!gates.joinedProfileIds(template, entityTypes, store).isEmpty()) {
        return store;
      }
    }
    return engine;
  }

  /** The external vector stores semantic search configures, by name. */
  @Nonnull
  public static Map<String, VectorStoreConfiguration> stores(
      @Nullable SemanticSearchConfiguration semanticSearch) {
    return semanticSearch == null || semanticSearch.getStores() == null
        ? Map.of()
        : semanticSearch.getStores();
  }

  /**
   * Cache identity of the window a page from an external store falls in, so the search cache stops
   * serving that page once the store's result TTL passes. Empty on the DataHub index.
   */
  @Nonnull
  public static String resultWindow(
      @Nullable String system, @Nonnull Map<String, VectorStoreConfiguration> stores) {
    final VectorStoreConfiguration store = system == null ? null : stores.get(system);
    return store == null
        ? ""
        : ":" + System.currentTimeMillis() / Math.max(1L, store.getResultTtlMillis());
  }

  /**
   * The raw cosine behind a cosine kNN score. Elasticsearch 8 and 9 and OpenSearch 3, the engines
   * semantic read allows, score cosine as {@code (1 + cosine) / 2}.
   */
  public static double cosine(double engineScore) {
    return 2d * engineScore - 1d;
  }
}
