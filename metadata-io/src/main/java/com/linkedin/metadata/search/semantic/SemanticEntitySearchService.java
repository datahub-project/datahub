package com.linkedin.metadata.search.semantic;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.EmbeddingTaskType;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.SearchResultUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import com.linkedin.metadata.utils.elasticsearch.V3IndexKeys;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 *   <li>Search V3 semantic reads ({@code semanticReadEnabled}): OpenSearch 3.5+ or Elasticsearch
 *       8.18+ on the Search V3 cluster
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
 * <p>Index selection: by default kNN runs against the V2 semantic indices ({@code
 * <entity>index_v2_semantic}). When Search V3 is on and {@code semanticReadEnabled} is set, it runs
 * against the V3 entity indices whose mappings carry the same {@code embeddings} field (document
 * entities by default) on the Search V3 cluster.
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
  @Nullable private final EntityIndexConfiguration entityIndexConfiguration;
  private final Set<String> warnedSharedIndexEntities = ConcurrentHashMap.newKeySet();

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
    this(searchClient, embeddingProvider, mappingsBuilder, modelEmbeddingKey, null);
  }

  /**
   * Constructs a semantic entity search service that can read Search V3 entity indices.
   *
   * @param searchClient shim for the V2 semantic indices (the {@code semantic} component)
   * @param embeddingProvider provider capable of generating query embeddings
   * @param mappingsBuilder mappings builder for the semantic indices
   * @param modelEmbeddingKey the model embedding key (e.g., "cohere_embed_v3")
   * @param entityIndexConfiguration V2/V3 index flags and semantic settings; null keeps every read
   *     on the V2 semantic indices
   */
  public SemanticEntitySearchService(
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull EmbeddingProvider embeddingProvider,
      @Nonnull MappingsBuilder mappingsBuilder,
      @Nonnull String modelEmbeddingKey,
      @Nullable EntityIndexConfiguration entityIndexConfiguration) {
    this.searchClient = Objects.requireNonNull(searchClient, "searchClientShim");
    this.embeddingProvider = Objects.requireNonNull(embeddingProvider, "embeddingProvider");
    // Initialize with empty chain for POC - in production this would be injected
    this.queryFilterRewriteChain = QueryFilterRewriteChain.EMPTY;
    this.mappingsBuilder = Objects.requireNonNull(mappingsBuilder, "mappingsBuilder");
    this.modelEmbeddingKey = Objects.requireNonNull(modelEmbeddingKey, "modelEmbeddingKey");
    this.nestedPath = EMBEDDINGS_PREFIX + modelEmbeddingKey + CHUNKS_SUFFIX;
    this.vectorField = nestedPath + VECTOR_SUFFIX;
    this.entityIndexConfiguration = entityIndexConfiguration;
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
    // 1) Map entity names to the indices holding their vectors: V3 entity indices once semantic
    // reads are cut over, otherwise the V2 semantic indices
    final boolean readV3 = shouldReadSemanticV3(entityIndexConfiguration);
    List<String> indices =
        readV3
            ? v3SemanticIndices(opContext, entityNames)
            : entityNames.stream()
                .map(
                    entity -> {
                      String baseIndex =
                          opContext
                              .getSearchContext()
                              .getIndexConvention()
                              .getEntityIndexName(opContext, entity);
                      return appendSemanticSuffix(baseIndex);
                    })
                .collect(Collectors.toList());

    if (indices.isEmpty()) {
      int normalizedPageSize = pageSize != null ? pageSize : 10;
      return emptyResult(from, normalizedPageSize);
    }

    // 2) Generate query embedding
    // TODO: Make model configurable
    float[] queryEmbedding = embeddingProvider.embed(input, null, EmbeddingTaskType.QUERY);

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

    // 5) Transform virtual filters (like _entityType) to actual index fields: _index filters with
    // the _semantic suffix on V2, V3 entity index names on V3
    final IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    Filter transformedFilters = null;
    if (postFilters != null) {
      transformedFilters =
          readV3
              ? toV3Filter(opContext, postFilters)
              : SearchUtil.transformFilterForEntities(
                  opContext, postFilters, new SemanticIndexConvention(indexConvention));
    }

    // 6) Build filters using ESUtils with proper field types
    Map<String, Object> finalFilterMap =
        transformedFilters != null
            ? ESUtils.buildFilterMap(
                // Use the new method that delegates to buildFilterQuery
                transformedFilters, // Use transformed filters instead of raw postFilters
                // The timeseries flag drops the .keyword suffix, which V3 needs: its keyword and
                // URN fields have no such subfield, and its text fields are keyword-typed too. The
                // flag's other effect, the rewrite context, is unused with an empty rewrite chain.
                readV3,
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
        SearchDocFieldFetchConfig.resolve(
            SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH,
            opContext.getSearchContext().getSearchFlags());

    // 8) Execute kNN query via the engine-specific SearchClientShim path. V3 indices live on the
    // Search V3 cluster, which may differ from the semantic component's cluster.
    SearchClientShim<?> client =
        readV3 ? SearchClients.forComponent(opContext, SearchComponent.SEARCH_V3) : searchClient;
    List<SearchEntity> hits =
        executeKnn(
            opContext,
            client,
            opContext.getObjectMapper(),
            indices,
            queryEmbedding,
            k,
            finalFilterMap,
            fieldsToFetch);

    // Apply relevance floor: drop hits scoring below minScore so a caller (e.g. an agent) can
    // abstain rather than surface weak matches. Post-kNN filtering keeps this engine-agnostic
    // across the ES8 and OpenSearch shims.
    final com.linkedin.metadata.query.SearchFlags searchFlags =
        opContext.getSearchContext().getSearchFlags();
    final Float minScore = searchFlags != null ? searchFlags.getMinScore() : null;
    if (minScore != null) {
      hits =
          hits.stream()
              .filter(h -> h.getScore() != null && h.getScore() >= minScore)
              .collect(Collectors.toList());
    }

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

  /**
   * True when kNN reads V3 entity indices instead of the V2 semantic indices: V3 is on and {@code
   * semanticReadEnabled} is set. Semantic search itself requires V2 to stay enabled, so there is no
   * V2-off case to handle here.
   */
  static boolean shouldReadSemanticV3(@Nullable EntityIndexConfiguration entityIndex) {
    return entityIndex != null
        && entityIndex.getV3() != null
        && entityIndex.getV3().isEnabled()
        && entityIndex.getV3().isSemanticReadEnabled();
  }

  /**
   * Refuses semantic reads on a Search V3 cluster that would silently drop facet and View filters.
   * See {@link #supportsV3SemanticFilters}. Only queries the cluster when the flag is on.
   */
  public static void requireSupportedV3Engine(
      @Nullable EntityIndexConfiguration entityIndex, @Nonnull SearchClientShim<?> v3Client) {
    if (!shouldReadSemanticV3(entityIndex)
        || entityIndex.getSemanticSearch() == null
        || !entityIndex.getSemanticSearch().isEnabled()
        || !isOpenSearch(v3Client)) {
      return;
    }
    String version = engineVersion(v3Client);
    int[] majorMinor = majorMinor(version);
    if (majorMinor == null) {
      throw new IllegalStateException(
          "elasticsearch.entityIndex.v3.semanticReadEnabled needs OpenSearch 3.5+ on the Search V3"
              + " cluster, but its version could not be read (got '"
              + version
              + "'); grant the cluster:monitor/main permission or turn the flag off");
    }
    if (!atLeastOpenSearch35(majorMinor)) {
      throw new IllegalStateException(
          "elasticsearch.entityIndex.v3.semanticReadEnabled needs OpenSearch 3.5+ or Elasticsearch"
              + " 8.18+ on the Search V3 cluster, which runs OpenSearch "
              + version
              + ": earlier OpenSearch k-NN pre-filters ignore the V3 _aspects fields that facet and"
              + " View filters use");
    }
  }

  /**
   * False for OpenSearch before 3.5, whose k-NN plugin runs a nested query's pre-filter in the
   * nested scope for fields under an underscore-prefixed object. V3 keeps every aspect field under
   * {@code _aspects}, so such filters match nothing there. Elasticsearch applies them.
   */
  public static boolean supportsV3SemanticFilters(@Nonnull SearchClientShim<?> client) {
    if (!isOpenSearch(client)) {
      return true;
    }
    int[] majorMinor = majorMinor(engineVersion(client));
    return majorMinor != null && atLeastOpenSearch35(majorMinor);
  }

  private static boolean isOpenSearch(@Nonnull SearchClientShim<?> client) {
    SearchEngineType engineType = client.getEngineType();
    return engineType != null && engineType.isOpenSearch();
  }

  @Nullable
  private static String engineVersion(@Nonnull SearchClientShim<?> client) {
    try {
      return client.getEngineVersion();
    } catch (IOException e) {
      return null;
    }
  }

  @Nullable
  private static int[] majorMinor(@Nullable String version) {
    if (version == null) {
      return null;
    }
    String[] parts = version.split("\\.");
    try {
      return new int[] {
        Integer.parseInt(parts[0]), parts.length > 1 ? Integer.parseInt(parts[1]) : 0
      };
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean atLeastOpenSearch35(@Nonnull int[] majorMinor) {
    return majorMinor[0] > 3 || (majorMinor[0] == 3 && majorMinor[1] >= 5);
  }

  /**
   * V3 entity indices that can serve kNN for {@code entityNames}: entity-named indices of
   * semantic-enabled entity types, the only V3 indices that get the {@code embeddings} mapping.
   * With the default {@code enabledEntities} that leaves {@code documentindex_v3}. Entity types in
   * a shared search-group index are skipped.
   */
  @Nonnull
  private List<String> v3SemanticIndices(
      @Nonnull OperationContext opContext, @Nonnull List<String> entityNames) {
    SemanticSearchConfiguration semanticConfig =
        Objects.requireNonNull(entityIndexConfiguration).getSemanticSearch();
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    Set<String> indices = new LinkedHashSet<>();
    for (String entityName : entityNames) {
      EntitySpec entitySpec;
      try {
        entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
      } catch (Exception e) {
        log.warn("Skipping unknown entity type {} for semantic search", entityName);
        continue;
      }
      if (!SemanticEmbeddingMappings.isEnabledForEntity(semanticConfig, entitySpec.getName())) {
        continue;
      }
      String indexKey = V3IndexKeys.resolve(entitySpec);
      if (!indexKey.equals(entitySpec.getName())) {
        if (warnedSharedIndexEntities.add(entitySpec.getName())) {
          log.warn(
              "Semantic search on Search V3 skips {}: it is stored in the shared {} index, which"
                  + " has no embeddings mapping",
              entitySpec.getName(),
              indexKey);
        }
        continue;
      }
      indices.add(indexConvention.getEntityIndexNameV3(opContext, indexKey));
    }
    return new ArrayList<>(indices);
  }

  /**
   * Adapts a filter to the V3 mapping. {@code _entityType} becomes an {@code _index} filter on the
   * V3 index names, resolved like the V2 rewrite (underscores dropped, case ignored), because
   * GraphQL sends {@code DOCUMENT} while V3 stores {@code document}. Only entity-named V3 indices
   * are searched, so an index name identifies the entity type. An explicit {@code .keyword} suffix
   * is dropped: V3 keyword and URN fields have no such subfield, and its text fields are
   * keyword-typed at the root.
   */
  @Nonnull
  private static Filter toV3Filter(@Nonnull OperationContext opContext, @Nonnull Filter filter) {
    if (filter.getOr() == null) {
      if (filter.getCriteria() == null) {
        return filter;
      }
      // Deprecated single-conjunction form
      filter =
          new Filter()
              .setOr(
                  new ConjunctiveCriterionArray(
                      new ConjunctiveCriterion().setAnd(filter.getCriteria())));
    }
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    ConjunctiveCriterionArray or = new ConjunctiveCriterionArray();
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      CriterionArray and = new CriterionArray();
      for (Criterion criterion : conjunction.getAnd()) {
        String field = criterion.getField();
        if (field.endsWith(ESUtils.KEYWORD_SUFFIX)) {
          field = field.substring(0, field.length() - ESUtils.KEYWORD_SUFFIX.length());
        }
        if (!field.equalsIgnoreCase(SearchUtil.INDEX_VIRTUAL_FIELD)) {
          and.add(
              field.equals(criterion.getField())
                  ? criterion
                  : buildCriterion(
                      field,
                      criterion.getCondition(),
                      criterion.isNegated(),
                      criterion.getValues()));
          continue;
        }
        List<String> indexNames = new ArrayList<>();
        for (String value : criterion.getValues()) {
          try {
            EntitySpec entitySpec =
                opContext.getEntityRegistry().getEntitySpec(String.join("", value.split("_")));
            indexNames.add(
                indexConvention.getEntityIndexNameV3(opContext, V3IndexKeys.resolve(entitySpec)));
          } catch (RuntimeException e) {
            // Unknown entity type: keep the value so the filter matches nothing, as on V2
            indexNames.add(value);
          }
        }
        and.add(
            buildCriterion(
                SearchUtil.ES_INDEX_FIELD, Condition.EQUAL, criterion.isNegated(), indexNames));
      }
      or.add(new ConjunctiveCriterion().setAnd(and));
    }
    return new Filter().setOr(or);
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
   * @param opContext operation context threaded to the shim
   * @param client shim for the cluster that hosts {@code indices}
   * @param objectMapper the operation context's configured mapper, used to serialize extra fields
   * @param indices list of semantic index names to search (comma-joined for multi-index)
   * @param vector query embedding vector
   * @param k number of nearest neighbors to retrieve
   * @param docLevelFilterMap optional document-level filter map to apply within the kNN filter
   * @param fieldsToFetch set of fields to fetch in the _source (supports fetchExtraFields)
   * @return list of {@link SearchEntity} constructed from kNN hits
   */
  private List<SearchEntity> executeKnn(
      @Nonnull OperationContext opContext,
      @Nonnull SearchClientShim<?> client,
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
      KnnSearchResponse response = client.searchKnn(opContext, request);
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
