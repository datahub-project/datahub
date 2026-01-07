package com.linkedin.metadata.search.semantic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchResultUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;

// removed BoolQueryBuilder in favor of Map-based filters

/**
 * Minimal semantic search service using OpenSearch kNN against the POC semantic indices.
 *
 * <p>REQUIREMENTS: - OpenSearch 2.17.0 or higher (for pre-filtering support with nested kNN
 * vectors) - k-NN plugin installed and enabled - Semantic indices with nested vector fields at
 * embeddings.cohere_embed_v3.chunks.vector
 *
 * <p>Implementation details: - No caching - No hybrid search (semantic only) - Uses Low Level REST
 * Client to support pre-filtering inside kNN block - Oversamples candidates and slices in-memory
 * for stable pagination (skip/take)
 *
 * <p>Low Level REST Client rationale: We use the Low Level REST Client instead of the High Level
 * REST Client because: 1. HLRC doesn't natively support kNN queries 2. HLRC makes it difficult to
 * properly serialize filters inside the kNN block 3. Low Level client allows us to send the exact
 * JSON query we want, similar to Python/curl 4. This enables us to use pre-filtering (filters
 * inside kNN) for better performance
 *
 * <p>Pre-filtering (OpenSearch 2.17+): Filters are placed inside the kNN block's "filter"
 * parameter, which applies them BEFORE the approximate nearest neighbor traversal. This
 * dramatically improves efficiency when filtering reduces the candidate set significantly. This
 * feature requires OpenSearch 2.17+ for proper support with nested vector fields.
 *
 * <p>Stable pagination note: We request k >= ceil((from + pageSize) * oversampleFactor) to ensure
 * that, after pre-filtering and any deduplication, there are at least from + pageSize candidates
 * available to slice [from, from + pageSize) without re-issuing the query. With pre-filtering, a
 * small oversampleFactor (e.g., 1.2) is typically sufficient.
 *
 * <p>Result shape parity: We intentionally populate {@link
 * com.linkedin.metadata.search.SearchEntity} fields similarly to the keyword path implemented by
 * {@code com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler#getResult}:
 * - entity (URN) - score (backend score) - features (SEARCH_BACKEND_SCORE and QUERY_COUNT when
 * available), analogous to {@code extractFeatures} - extraFields (stringified copy of _source),
 * analogous to {@code getStringMap}
 *
 * <p>Matched fields/highlighting are not set in semantic mode v1. The keyword path derives {@code
 * matchedFields} from highlight fragments, but we do not request highlighting for semantic queries
 * and clients are expected to suppress highlights in this mode. Facets are attached by the caller
 * (see {@code SearchService}) using a parallel keyword aggregation to maintain parity with existing
 * consumers.
 */
@Slf4j
public class SemanticEntitySearchService implements SemanticEntitySearch {

  private static final String NESTED_PATH = "embeddings.cohere_embed_v3.chunks";
  private static final String VECTOR_FIELD = NESTED_PATH + ".vector";
  private static final double DEFAULT_OVERSAMPLE_FACTOR = 1.2d; // Lower for pre-filtering
  private static final int MAX_K = 500;

  private final SearchClientShim<?> searchClient;
  private final EmbeddingProvider embeddingProvider;
  private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final MappingsBuilder mappingsBuilder;

  /**
   * Constructs a semantic entity search service backed by OpenSearch's low-level REST client.
   *
   * @param searchClient high-level client used only to obtain the low-level client
   * @param embeddingProvider provider capable of generating query embeddings
   */
  public SemanticEntitySearchService(
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull EmbeddingProvider embeddingProvider,
      MappingsBuilder mappingsBuilder) {
    this.searchClient = Objects.requireNonNull(searchClient, "searchClientShim");
    this.embeddingProvider = Objects.requireNonNull(embeddingProvider, "embeddingProvider");
    // Initialize with empty chain for POC - in production this would be injected
    this.queryFilterRewriteChain = QueryFilterRewriteChain.EMPTY;
    this.mappingsBuilder = mappingsBuilder;
  }

  /**
   * Execute semantic search against the semantic indices for the provided entities.
   *
   * @param opContext operation context (auth, flags, registries)
   * @param entityNames list of entity type names whose semantic indices should be searched
   * @param input raw query text to embed for kNN
   * @param postFilters optional document-level filters applied inside or after kNN
   * @param sortCriterion optional sort criterion (semantic v1 typically ignores custom sort)
   * @param from zero-based starting offset for pagination on the ranked candidate list
   * @param pageSize requested number of results per page; when null, defaults are applied
   *     internally
   * @return a {@link SearchResult} containing paginated, semantically ranked entities and metadata
   */
  @Nonnull
  @Override
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion,
      int from,
      @Nullable Integer pageSize) {
    // 1) Map entity names to semantic indices
    List<String> indices =
        entityNames.stream()
            .map(
                entity -> {
                  String baseIndex =
                      opContext.getSearchContext().getIndexConvention().getEntityIndexName(entity);
                  return appendSemanticSuffix(baseIndex);
                })
            .collect(Collectors.toList());

    if (indices.isEmpty()) {
      int normalizedPageSize = pageSize != null ? pageSize : 10;
      return emptyResult(from, normalizedPageSize);
    }

    // 2) Generate query embedding
    // TODO: Make model configurable
    float[] queryEmbedding = embeddingProvider.embed(input, null);

    // 3) Get entity specs to extract field types
    List<EntitySpec> entitySpecs =
        entityNames.stream()
            .map(
                name -> {
                  try {
                    return opContext.getEntityRegistry().getEntitySpec(name);
                  } catch (Exception e) {
                    log.warn("Failed to get entity spec for {}, using empty field types", name, e);
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // 4) Build searchable field types from entity specs
    Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes =
        !entitySpecs.isEmpty()
            ? ESUtils.buildSearchableFieldTypes(opContext.getEntityRegistry(), mappingsBuilder)
            : new HashMap<>();

    if (searchableFieldTypes.isEmpty()) {
      log.warn(
          "No searchable field types found for entities {}. Numeric filters may not work correctly.",
          entityNames);
    }

    // 5) Transform virtual filters (like _entityType) to actual semantic index fields
    // This transformation converts _entityType filters to _index filters with _semantic suffix
    SemanticIndexConvention semanticIndexConvention =
        new SemanticIndexConvention(opContext.getSearchContext().getIndexConvention());
    Filter transformedFilters =
        postFilters != null
            ? SearchUtil.transformFilterForEntities(postFilters, semanticIndexConvention)
            : null;

    // 6) Build filters using ESUtils with proper field types
    Map<String, Object> finalFilterMap =
        transformedFilters != null
            ? ESUtils.buildFilterMap(
                // Use the new method that delegates to buildFilterQuery
                transformedFilters, // Use transformed filters instead of raw postFilters
                false, // not timeseries
                searchableFieldTypes,
                opContext,
                queryFilterRewriteChain)
            : null;

    // Calculate k for oversampling
    int normalizedPageSize = pageSize != null ? pageSize : 10;
    int needed = from + normalizedPageSize;
    int k =
        Math.max(
            normalizedPageSize,
            Math.min(MAX_K, (int) Math.ceil(needed * DEFAULT_OVERSAMPLE_FACTOR)));

    // 7) Build field set using same logic as keyword search
    Set<String> fieldsToFetch =
        new HashSet<>(SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH);

    // 8) Execute OpenSearch nested kNN query with pre-filtering inside kNN
    List<SearchEntity> hits =
        executeKnn(
            opContext.getObjectMapper(), indices, queryEmbedding, k, finalFilterMap, fieldsToFetch);

    // 9) Slice [from, from+pageSize)
    if (from >= hits.size()) {
      return emptyResult(from, normalizedPageSize);
    }
    int to = Math.min(hits.size(), from + normalizedPageSize);
    List<SearchEntity> page = hits.subList(from, to);

    // 10) Build SearchResult following keyword search pattern
    // Note: For k-NN, numEntities represents the total candidates found (after filtering),
    // not total documents in index. With track_total_hits=false, hits.size() is our best estimate.
    SearchResultMetadata metadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray());
    return new SearchResult()
        .setEntities(new SearchEntityArray(page))
        .setMetadata(metadata)
        .setFrom(from)
        .setPageSize(normalizedPageSize)
        .setNumEntities(hits.size());
  }

  /**
   * Appends the semantic index suffix to the provided base index name.
   *
   * @param baseIndex base index name (e.g., datasetindex_v2)
   * @return semantic index name (e.g., datasetindex_v2_semantic)
   */
  private static String appendSemanticSuffix(String baseIndex) {
    return baseIndex + "_semantic";
  }

  @Nonnull
  /**
   * Builds an empty {@link SearchResult} with paging metadata.
   *
   * @param from starting offset
   * @param size requested page size
   * @return empty search result with metadata initialized
   */
  private SearchResult emptyResult(int from, int size) {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(from)
        .setPageSize(size)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  @Nonnull
  /**
   * Executes a nested kNN query against the provided semantic indices with optional pre-filtering.
   *
   * @param objectMapper Jackson object mapper for JSON (request/response) handling
   * @param indices list of semantic index names to search
   * @param vector query embedding vector
   * @param k number of nearest neighbors to retrieve from OpenSearch
   * @param docLevelFilterMap optional document-level filter map to apply within the kNN filter
   * @param fieldsToFetch set of fields to fetch in the _source (supports fetchExtraFields)
   * @return list of {@link SearchEntity} constructed from OpenSearch hits
   */
  private List<SearchEntity> executeKnn(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull List<String> indices,
      @Nonnull float[] vector,
      int k,
      @Nullable Map<String, Object> docLevelFilterMap,
      @Nonnull Set<String> fieldsToFetch) {
    try {
      // Build the complete query JSON with pre-filtering inside kNN
      Map<String, Object> queryJson =
          buildSemanticQueryWithPreFiltering(vector, k, docLevelFilterMap, fieldsToFetch);

      // Prepare the request body
      String requestBody = objectMapper.writeValueAsString(queryJson);
      log.info("Semantic search query with pre-filtering (k={}): {}", k, requestBody);

      // Build the endpoint URL
      String endpoint = "/" + String.join(",", indices) + "/_search";

      // Create the Low Level REST request
      Request request = new Request("POST", endpoint);
      request.setJsonEntity(requestBody);
      request.addParameter("ignore_unavailable", "true");
      request.addParameter("allow_no_indices", "true");

      // Execute the request
      RawResponse response = searchClient.performLowLevelRequest(request);

      // Parse the response
      String responseBody = EntityUtils.toString(response.getEntity());
      JsonNode responseJson = objectMapper.readTree(responseBody);

      // Extract hits
      List<SearchEntity> results = new ArrayList<>();
      JsonNode hits = responseJson.path("hits").path("hits");

      for (JsonNode hit : hits) {
        String urn = hit.path("_source").path("urn").asText();
        if (urn == null || urn.isEmpty()) {
          continue;
        }
        SearchEntity entity = new SearchEntity();
        try {
          entity.setEntity(com.linkedin.common.urn.Urn.createFromString(urn));
        } catch (java.net.URISyntaxException e) {
          log.warn("Invalid URN in search result: {}", urn);
          continue;
        }
        double score = hit.path("_score").asDouble();
        entity.setScore((float) score);

        // Populate features and extraFields using shared utilities (parity with keyword path)
        Map<String, Object> sourceAsMap =
            objectMapper.convertValue(
                hit.path("_source"), new TypeReference<Map<String, Object>>() {});
        entity.setFeatures(SearchResultUtils.buildBaseFeatures(score, sourceAsMap));
        entity.setExtraFields(SearchResultUtils.toExtraFields(objectMapper, sourceAsMap));
        results.add(entity);
      }

      // Note: With track_total_hits=false, we don't get exact total counts for performance
      // This follows keyword search pattern. In k-NN, the returned hits are our best estimate.
      log.info("kNN search with pre-filtering returned {} hits", results.size());
      return results;
    } catch (IOException e) {
      throw new RuntimeException("Failed to execute semantic kNN search with pre-filtering", e);
    }
  }

  /**
   * Build the complete OpenSearch query JSON with pre-filtering inside the kNN block. This produces
   * a query structure like: { "size": k, "track_total_hits": false, "_source": [fieldsToFetch],
   * "query": { "nested": { "path": "embeddings.cohere_embed_v3.chunks", "score_mode": "max",
   * "query": { "knn": { "embeddings.cohere_embed_v3.chunks.vector": { "vector": [...], "k": k,
   * "filter": { "bool": { "filter": [...] } } } } } } } }
   *
   * <p>Note: track_total_hits=false for performance, following keyword search pattern. In k-NN
   * context, total_hits would represent either k (no filters) or filtered k-NN results count.
   *
   * @param vector query embedding vector
   * @param k number of nearest neighbors to retrieve
   * @param docLevelFilterMap optional document-level filter map
   * @param fieldsToFetch set of fields to fetch in _source (follows keyword search pattern)
   * @return complete query map for OpenSearch
   */
  private Map<String, Object> buildSemanticQueryWithPreFiltering(
      float[] vector,
      int k,
      @Nullable Map<String, Object> docLevelFilterMap,
      @Nonnull Set<String> fieldsToFetch)
      throws IOException {
    // Build the kNN parameters map
    Map<String, Object> knnParams = new HashMap<>();
    knnParams.put("vector", convertToFloatList(vector));
    knnParams.put("k", k);

    // Add filters inside kNN for pre-filtering (REQUIRES OpenSearch 2.17.0+)
    if (docLevelFilterMap != null && !docLevelFilterMap.isEmpty()) {
      knnParams.put("filter", docLevelFilterMap);
      log.debug("Applied pre-filtering to kNN query: {}", docLevelFilterMap);
    }

    // Build the complete query structure using Map.of() for clarity
    // Use fieldsToFetch (supports DEFAULT_FIELDS_TO_FETCH_ON_SEARCH + fetchExtraFields like keyword
    // search)
    // Note: track_total_hits=false for better performance, following keyword search pattern
    // In k-NN context, total_hits represents either k (no filters) or filtered k-NN results count
    Map<String, Object> query =
        Map.of(
            "size",
            k,
            "track_total_hits",
            false,
            "_source",
            fieldsToFetch.toArray(new String[0]),
            "query",
            Map.of(
                "nested",
                Map.of(
                    "path",
                    NESTED_PATH,
                    "score_mode",
                    "max",
                    "query",
                    Map.of("knn", Map.of(VECTOR_FIELD, knnParams)))));

    return query;
  }

  /**
   * Convert float array to List<Float> for serialization.
   *
   * @param vector the float array to convert
   * @return List of Float values
   */
  private List<Float> convertToFloatList(float[] vector) {
    List<Float> vectorList = new ArrayList<>(vector.length);
    for (float v : vector) {
      vectorList.add(v);
    }
    return vectorList;
  }
}
