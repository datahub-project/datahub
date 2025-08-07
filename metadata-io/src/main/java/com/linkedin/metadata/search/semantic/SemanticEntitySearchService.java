/**
 * SAAS-SPECIFIC: This class is part of the semantic search feature exclusive to DataHub SaaS. It
 * should NOT be merged back to the open-source DataHub repository. Dependencies: Requires embedding
 * service and OpenSearch 2.17+ with k-NN plugin.
 */
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
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchResultUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

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
  private static final int EXPECTED_VECTOR_DIMS = 1024;
  private static final double DEFAULT_OVERSAMPLE_FACTOR = 1.2d; // Lower for pre-filtering
  private static final int MAX_K = 500;

  private final RestClient lowLevelClient;
  private final IndexConvention indexConvention;
  private final EmbeddingProvider embeddingProvider;
  private final ObjectMapper objectMapper;
  private final QueryFilterRewriteChain queryFilterRewriteChain;

  /**
   * Constructs a semantic entity search service backed by OpenSearch's low-level REST client.
   *
   * @param searchClient high-level client used only to obtain the low-level client
   * @param indexConvention index naming convention for resolving base and semantic indices
   * @param embeddingProvider provider capable of generating query embeddings
   * @param objectMapper Jackson object mapper for JSON (request/response) handling
   */
  public SemanticEntitySearchService(
      @Nonnull RestHighLevelClient searchClient,
      @Nonnull IndexConvention indexConvention,
      @Nonnull EmbeddingProvider embeddingProvider,
      @Nonnull ObjectMapper objectMapper) {
    this.lowLevelClient = Objects.requireNonNull(searchClient, "searchClient").getLowLevelClient();
    this.indexConvention = Objects.requireNonNull(indexConvention, "indexConvention");
    this.embeddingProvider = Objects.requireNonNull(embeddingProvider, "embeddingProvider");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    // Initialize with empty chain for POC - in production this would be injected
    this.queryFilterRewriteChain = QueryFilterRewriteChain.EMPTY;
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
                  String baseIndex = indexConvention.getEntityIndexName(entity);
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
            ? ESUtils.buildSearchableFieldTypes(opContext.getEntityRegistry(), entitySpecs)
            : new HashMap<>();

    if (searchableFieldTypes.isEmpty()) {
      log.warn(
          "No searchable field types found for entities {}. Numeric filters may not work correctly.",
          entityNames);
    }

    // 5) Build filters using ESUtils with proper field types
    Map<String, Object> finalFilterMap =
        postFilters != null
            ? ESUtils.buildFilterMap( // Use the new method that delegates to buildFilterQuery
                postFilters,
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

    // 6) Execute OpenSearch nested kNN query with pre-filtering inside kNN
    List<SearchEntity> hits = executeKnn(indices, queryEmbedding, k, finalFilterMap);

    // 7) Slice [from, from+pageSize)
    if (from >= hits.size()) {
      return emptyResult(from, normalizedPageSize);
    }
    int to = Math.min(hits.size(), from + normalizedPageSize);
    List<SearchEntity> page = hits.subList(from, to);

    // 8) Build SearchResult (numEntities unknown precisely with kNN; best-effort is hits.size())
    SearchResultMetadata metadata =
        new SearchResultMetadata()
            .setAggregations(new AggregationMetadataArray())
            .setScoringMethod("cosine_similarity");
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
        .setMetadata(
            new SearchResultMetadata()
                .setAggregations(new AggregationMetadataArray())
                .setScoringMethod("cosine_similarity"));
  }

  @Nonnull
  /**
   * Executes a nested kNN query against the provided semantic indices with optional pre-filtering.
   *
   * @param indices list of semantic index names to search
   * @param vector query embedding vector
   * @param k number of nearest neighbors to retrieve from OpenSearch
   * @param docLevelFilterMap optional document-level filter map to apply within the kNN filter
   * @return list of {@link SearchEntity} constructed from OpenSearch hits
   */
  private List<SearchEntity> executeKnn(
      @Nonnull List<String> indices,
      @Nonnull float[] vector,
      int k,
      @Nullable Map<String, Object> docLevelFilterMap) {
    try {
      // Build the complete query JSON with pre-filtering inside kNN
      Map<String, Object> queryJson =
          buildSemanticQueryWithPreFiltering(vector, k, docLevelFilterMap);

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
      Response response = lowLevelClient.performRequest(request);

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

      long totalHits = responseJson.path("hits").path("total").path("value").asLong();
      log.info(
          "kNN search with pre-filtering returned {} hits out of {} total",
          results.size(),
          totalHits);
      return results;

    } catch (IOException e) {
      throw new RuntimeException("Failed to execute semantic kNN search with pre-filtering", e);
    }
  }

  /**
   * Build the complete OpenSearch query JSON with pre-filtering inside the kNN block. This produces
   * a query structure like: { "size": k, "track_total_hits": true, "_source": ["urn", "typeNames",
   * "name", "platform", "qualifiedName"], "query": { "nested": { "path":
   * "embeddings.cohere_embed_v3.chunks", "score_mode": "max", "query": { "knn": {
   * "embeddings.cohere_embed_v3.chunks.vector": { "vector": [...], "k": k, "filter": { "bool": {
   * "filter": [...] } } } } } } } }
   */
  private Map<String, Object> buildSemanticQueryWithPreFiltering(
      float[] vector, int k, @Nullable Map<String, Object> docLevelFilterMap) throws IOException {

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
    Map<String, Object> query =
        Map.of(
            "size",
            k,
            "track_total_hits",
            true,
            "_source",
            List.of("urn", "typeNames", "name", "platform", "qualifiedName"),
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
