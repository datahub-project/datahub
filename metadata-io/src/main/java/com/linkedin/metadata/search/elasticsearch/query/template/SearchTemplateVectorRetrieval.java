package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.search.VectorStoreConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.metadata.search.hybrid.v3.V3HybridKnnRequestBuilder;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
  public static final int EXECUTION_VERSION = 2;

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

  /**
   * This process cannot run a vector rung: semantic read is off, or a rung that embeds its query
   * has no embedding provider. Similarity embeds nothing.
   */
  public static final String UNAVAILABLE = "unavailable";

  /** The request has a caller filter, and the store serving the profile cannot apply it. */
  public static final String NO_PRE_FILTER = "noPreFilter";

  /** The floor inherited from configuration was skipped: the profile's metric is not cosine. */
  public static final String PROFILE_NOT_COSINE = "profileNotCosine";

  /**
   * Similarity could not run: the query is not an entity URN, the viewer may not see that entity,
   * the default search filters or the search's document visibility rule leave it out, its type has
   * no embedding profile, or it has no stored vectors or more than {@link #MAX_ANCHOR_CHUNKS}
   * stored chunks for that profile. The search fails. A profile that cannot be read or selected is
   * {@link #UNAVAILABLE} instead.
   */
  public static final String NO_ANCHOR = "noAnchor";

  /**
   * The most stored chunks a similarity anchor may have: the most the default embedding pipeline
   * stores for one document ({@code max_chunks_per_document}, {@code
   * DEFAULT_MAX_CHUNKS_PER_DOCUMENT} in the ingestion framework's {@code chunking_config.py}), so
   * every document it embeds can be an anchor. Each chunk sends its own kNN request for the window,
   * so this bounds one search's requests. An anchor with more fails as {@link #NO_ANCHOR}: no
   * subset of its chunks is a principled stand-in for the whole entity.
   */
  public static final int MAX_ANCHOR_CHUNKS = 100;

  /** Why similarity left out a requested entity type: it is not under the anchor's profile. */
  public static final String NOT_ANCHOR_PROFILE = "notAnchorProfile";

  /** Similarity reads the anchor's stored chunk vectors from this aspect. */
  public static final String SEMANTIC_CONTENT_ASPECT_NAME = "semanticContent";

  private static final Set<String> ENGINES =
      Set.of(EmbeddingProfileRegistry.ELASTICSEARCH, EmbeddingProfileRegistry.OPENSEARCH);

  public static final String MIN_SCORE_DEPRECATION =
      "SearchFlags.minScore is deprecated as a vector leg floor; use SearchFlags.minSimilarity,"
          + " or retrieval.minSimilarity in a template";

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

  /**
   * The external vector stores semantic search configures, by name. A store named elasticsearch or
   * opensearch is ignored: those names always mean the DataHub index.
   */
  @Nonnull
  public static Map<String, VectorStoreConfiguration> stores(
      @Nullable SemanticSearchConfiguration semanticSearch) {
    if (semanticSearch == null || semanticSearch.getStores() == null) {
      return Map.of();
    }
    final Map<String, VectorStoreConfiguration> stores = new HashMap<>(semanticSearch.getStores());
    stores.keySet().removeAll(ENGINES);
    return stores;
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

  /**
   * The entity a similarity query names, or null when the query is not an entity URN. Bind and the
   * rung both read the anchor here, as routing reads the query: a route that misses is tried again
   * without the structured prefix, so the prefix is dropped before the rest is trimmed. The GraphQL
   * keyword search resolvers escape each {@code /} as {@code \/}, so that escape is undone, for the
   * anchor only. The semantic search resolver passes the query as typed, so there the unescape
   * changes nothing unless the query holds a literal {@code \/}. Rest.li, OpenAPI and SDK callers
   * do not escape either, so a literal {@code \/} in their query is read as {@code /} too.
   */
  @Nullable
  public static Urn anchorUrn(@Nonnull String query) {
    final String text =
        query.startsWith(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX)
            ? query.substring(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX.length())
            : query;
    try {
      return Urn.createFromString(text.trim().replace("\\/", "/"));
    } catch (URISyntaxException e) {
      return null;
    }
  }

  // Reasons an anchor selects no profile that are about its entity type, so the request is at fault
  private static final Set<String> ENTITY_TYPE_REASONS =
      Set.of(
          SearchTemplateGates.ENTITY_NOT_ENABLED,
          SearchTemplateGates.ENTITY_NOT_ADVERTISED,
          EmbeddingProfileRegistry.NO_DEFAULT);

  /**
   * The failure for an anchor whose entity type selects no single embedding profile, given the
   * reason from {@link SearchTemplateGates#anchorProfileFailure}. A reason about the entity type is
   * {@link #noAnchor}. An unknown or unavailable profile, an empty selection, or several profiles
   * is a fault of the template or the registry: it fails closed as {@link #UNAVAILABLE}, so it is
   * not reported as a bad request.
   */
  @Nonnull
  public static SearchTemplateFailClosedException anchorWithoutProfile(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull Urn anchor,
      @Nullable String reason) {
    if (reason != null && ENTITY_TYPE_REASONS.contains(reason)) {
      return noAnchor(opContext, template, anchor + " has no embedding profile to search with");
    }
    SearchTemplateMetrics.vectorRetrieval(opContext, UNAVAILABLE);
    return new SearchTemplateFailClosedException(
        "Search template "
            + template.getUrn()
            + " cannot resolve one embedding profile for "
            + anchor.getEntityType()
            + ": "
            + reason);
  }

  /**
   * Counts {@link #NO_ANCHOR} and returns the failure for a similarity search that cannot use its
   * anchor. Bind and the rung both fail through it, so an anchor the viewer may not use and one
   * with no stored vectors read the same way.
   */
  @Nonnull
  public static SearchTemplateFailClosedException noAnchor(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull String reason) {
    SearchTemplateMetrics.vectorRetrieval(opContext, NO_ANCHOR);
    return new SearchTemplateNoAnchorException(
        "Search template " + template.getUrn() + " finds similar entities, and " + reason);
  }
}
