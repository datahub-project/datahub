package com.linkedin.metadata.search.semantic;

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
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
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

/**
 * Semantic search service that issues approximate nearest-neighbour (kNN) queries against semantic
 * indices containing nested vector fields.
 *
 * <p>Engine support is provided through {@link SearchClientShim}: the concrete shim implementation
 * determines which search engine backs the deployment:
 *
 * <ul>
 *   <li><b>Elasticsearch 8</b> — uses the native {@code knn} clause inside a {@code nested} query.
 *       Filters are placed inside the {@code knn} block (pre-filtering). Index mappings use {@code
 *       dense_vector} with a similarity metric derived from the configured space type.
 *   <li><b>OpenSearch 2</b> — issues kNN requests via the low-level REST client, which is required
 *       because the high-level client does not natively support kNN with nested vector fields.
 *       Index mappings use {@code knn_vector} with the k-NN plugin method block.
 * </ul>
 *
 * <p>REQUIREMENTS:
 *
 * <ul>
 *   <li>Elasticsearch 8.x, or OpenSearch 2.17.0+ (for pre-filtering support with nested kNN
 *       vectors)
 *   <li>Semantic indices with nested vector fields at {@code
 *       embeddings.{modelEmbeddingKey}.chunks.vector}
 * </ul>
 *
 * <p>Implementation details:
 *
 * <ul>
 *   <li>No caching
 *   <li>No hybrid search (semantic only)
 *   <li>Filter placement is engine-specific (see below)
 *   <li>Oversamples candidates and slices in-memory for stable pagination (skip/take)
 * </ul>
 *
 * <p>Filter placement varies by engine:
 *
 * <ul>
 *   <li><b>OpenSearch 2</b>: filters are placed inside the kNN clause ({@code filter} parameter),
 *       applying them <em>before</em> the approximate nearest-neighbour traversal (pre-filtering).
 *   <li><b>Elasticsearch 8</b>: filters are placed in the outer {@code bool.filter} clause
 *       (post-filtering), because ES 8's nested kNN context cannot see root-level document fields.
 *       The 1.2x oversample factor compensates for candidates discarded by post-filtering.
 * </ul>
 *
 * <p>Stable pagination note: We request {@code k >= ceil((from + pageSize) * oversampleFactor)} to
 * ensure that, after pre-filtering and any deduplication, there are at least {@code from +
 * pageSize} candidates available to slice {@code [from, from + pageSize)} without re-issuing the
 * query. With pre-filtering, a small oversampleFactor (e.g., 1.2) is typically sufficient.
 *
 * <p>Result shape parity: We intentionally populate {@link
 * com.linkedin.metadata.search.SearchEntity} fields similarly to the keyword path implemented by
 * {@code com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler#getResult}:
 * entity (URN), score (backend score), features (SEARCH_BACKEND_SCORE and QUERY_COUNT when
 * available), and extraFields (stringified copy of {@code _source}).
 *
 * <p>Matched fields/highlighting are not set in semantic mode v1. The keyword path derives {@code
 * matchedFields} from highlight fragments, but we do not request highlighting for semantic queries
 * and clients are expected to suppress highlights in this mode. Facets are attached by the caller
 * (see {@code SearchService}) using a parallel keyword aggregation to maintain parity with existing
 * consumers.
 */
@Slf4j
public class SemanticEntitySearchService implements SemanticEntitySearch {

  private static final String EMBEDDINGS_PREFIX = "embeddings.";
  private static final String CHUNKS_SUFFIX = ".chunks";
  private static final String VECTOR_SUFFIX = ".vector";
  private static final double DEFAULT_OVERSAMPLE_FACTOR = 1.2d; // Lower for pre-filtering
  private static final int MAX_K = 500;
  private static final String DEFAULT_MODEL_EMBEDDING_KEY = "text_embedding_3_large";

  private final SearchClientShim<?> searchClient;
  private final EmbeddingProvider embeddingProvider;
  private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final MappingsBuilder mappingsBuilder;
  private final String modelEmbeddingKey;
  private final String nestedPath;
  private final String vectorField;

  /**
   * Constructs a semantic entity search service with the default model embedding key.
   *
   * <p>The concrete {@link SearchClientShim} implementation determines which search engine is used
   * (Elasticsearch 8 or OpenSearch 2) and dispatches queries accordingly.
   *
   * @param searchClient shim abstraction over the underlying search cluster
   * @param embeddingProvider provider capable of generating query embeddings
   * @param mappingsBuilder mappings builder for the semantic indices
   */
  public SemanticEntitySearchService(
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull EmbeddingProvider embeddingProvider,
      @Nonnull MappingsBuilder mappingsBuilder) {
    this(searchClient, embeddingProvider, mappingsBuilder, DEFAULT_MODEL_EMBEDDING_KEY);
  }

  /**
   * Constructs a semantic entity search service with a custom model embedding key.
   *
   * <p>The concrete {@link SearchClientShim} implementation determines which search engine is used
   * (Elasticsearch 8 or OpenSearch 2) and dispatches queries accordingly.
   *
   * @param searchClient shim abstraction over the underlying search cluster
   * @param embeddingProvider provider capable of generating query embeddings
   * @param mappingsBuilder mappings builder for the semantic indices
   * @param modelEmbeddingKey the model embedding key (e.g., "cohere_embed_v3",
   *     "text_embedding_3_small")
   */
  public SemanticEntitySearchService(
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull EmbeddingProvider embeddingProvider,
      @Nonnull MappingsBuilder mappingsBuilder,
      @Nonnull String modelEmbeddingKey) {
    this.searchClient = Objects.requireNonNull(searchClient, "searchClientShim");
    this.embeddingProvider = Objects.requireNonNull(embeddingProvider, "embeddingProvider");
    // Initialize with empty chain for POC - in production this would be injected
    this.queryFilterRewriteChain = QueryFilterRewriteChain.EMPTY;
    this.mappingsBuilder = Objects.requireNonNull(mappingsBuilder, "mappingsBuilder");
    this.modelEmbeddingKey = Objects.requireNonNull(modelEmbeddingKey, "modelEmbeddingKey");
    this.nestedPath = EMBEDDINGS_PREFIX + modelEmbeddingKey + CHUNKS_SUFFIX;
    this.vectorField = nestedPath + VECTOR_SUFFIX;
    log.info(
        "SemanticEntitySearchService initialized with modelEmbeddingKey={}, nestedPath={}, vectorField={}",
        modelEmbeddingKey,
        nestedPath,
        vectorField);
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

    // 8) Execute kNN query via the engine-specific SearchClientShim path
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
   * Executes a kNN query via {@link SearchClientShim#searchKnn} so the correct engine-specific
   * query format (ES 8 or OpenSearch 2) is used.
   *
   * @param objectMapper the operation context's configured mapper, used to serialize extra fields
   *     consistently with the rest of the search subsystem (e.g. {@code Include.NON_NULL})
   * @param indices list of semantic index names to search (comma-joined for multi-index)
   * @param vector query embedding vector
   * @param k number of nearest neighbors to retrieve
   * @param docLevelFilterMap optional document-level filter map to apply within the kNN filter
   * @param fieldsToFetch set of fields to fetch in the _source (supports fetchExtraFields)
   * @return list of {@link SearchEntity} constructed from kNN hits
   */
  private List<SearchEntity> executeKnn(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull List<String> indices,
      @Nonnull float[] vector,
      int k,
      @Nullable Map<String, Object> docLevelFilterMap,
      @Nonnull Set<String> fieldsToFetch) {
    if (docLevelFilterMap != null && !docLevelFilterMap.isEmpty()) {
      log.debug("Applied pre-filtering to kNN query: {}", docLevelFilterMap);
    }

    String commaJoinedIndices = String.join(",", indices);
    KnnSearchRequest request =
        KnnSearchRequest.builder()
            .indexName(commaJoinedIndices)
            .vectorField(vectorField)
            .queryVector(vector)
            .k(k)
            .fieldsToFetch(new ArrayList<>(fieldsToFetch))
            .filter(
                docLevelFilterMap != null && !docLevelFilterMap.isEmpty()
                    ? docLevelFilterMap
                    : null)
            .build();

    try {
      KnnSearchResponse response = searchClient.searchKnn(request);
      log.info("kNN search returned {} hits", response.hits().size());

      List<SearchEntity> results = new ArrayList<>(response.hits().size());
      for (KnnSearchResponse.Hit hit : response.hits()) {
        String urn = (String) hit.source().get("urn");
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
        entity.setScore((float) hit.score());
        entity.setFeatures(SearchResultUtils.buildBaseFeatures(hit.score(), hit.source()));
        entity.setExtraFields(SearchResultUtils.toExtraFields(objectMapper, hit.source()));
        results.add(entity);
      }
      return results;
    } catch (IOException e) {
      throw new RuntimeException("Failed to execute semantic kNN search", e);
    }
  }
}
