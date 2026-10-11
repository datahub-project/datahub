package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.utils.ESUtils.NAME_SUGGESTION;
import static com.linkedin.metadata.search.utils.ESUtils.applyDefaultSearchFilters;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.HighlightFields;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchSuggestion;
import com.linkedin.metadata.search.SearchSuggestionArray;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.InvalidSearchHitException;
import com.linkedin.metadata.search.utils.SearchResultUtils;
import com.linkedin.metadata.search.utils.UrnExtractionUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.lucene.search.Explanation;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.text.Text;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.suggest.term.TermSuggestion;

@Slf4j
public class SearchRequestHandler extends BaseRequestHandler {

  // Subfields that V2 highlight fields name and V3 indices do not have
  private static final Set<String> V2_HIGHLIGHT_SUBFIELDS =
      Set.of(
          "*",
          V2MappingsBuilder.DELIMITED,
          ESUtils.KEYWORD,
          V2LegacySettingsBuilder.NGRAM,
          "_2gram",
          "_3gram",
          "_4gram",
          V2MappingsBuilder.WORD_GRAMS_LENGTH_2,
          V2MappingsBuilder.WORD_GRAMS_LENGTH_3,
          V2MappingsBuilder.WORD_GRAMS_LENGTH_4);

  private static final Map<SearchHandlerKey, SearchRequestHandler> REQUEST_HANDLER_BY_ENTITY_NAME =
      new ConcurrentHashMap<>();
  private final List<EntitySpec> entitySpecs;
  private final List<String> entityNames;
  @Getter private final Set<String> defaultQueryFieldNames;
  @Nonnull private final HighlightBuilder highlights;

  private final SearchServiceConfiguration searchServiceConfig;
  private final SearchQueryBuilder searchQueryBuilder;
  // Set when keyword reads go to Search V3, whose full-text query reads the shared _search fields
  @Nullable private final V3SearchQueryBuilder v3SearchQueryBuilder;
  // Words shorter than this are dropped by the analyzers, so they never match on V3
  private final int v3MinWordLength;
  private final AggregationQueryBuilder aggregationQueryBuilder;
  private final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes;
  private final CustomizedQueryHandler customizedQueryHandler;
  private final Map<PathSpec, String> searchableFieldPaths;

  private final QueryFilterRewriteChain queryFilterRewriteChain;
  @Nullable private final EntityIndexConfiguration entityIndexConfiguration;

  private SearchRequestHandler(
      @Nonnull OperationContext opContext,
      @Nonnull EntitySpec entitySpec,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchServiceConfiguration searchServiceConfig) {
    this(
        opContext,
        ImmutableList.of(entitySpec),
        configs,
        customSearchConfiguration,
        queryFilterRewriteChain,
        searchServiceConfig);
  }

  private SearchRequestHandler(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchServiceConfiguration searchServiceConfig) {
    this.entitySpecs = entitySpecs;
    this.entityNames = entitySpecs.stream().map(EntitySpec::getName).collect(Collectors.toList());
    Map<EntitySpec, List<SearchableAnnotation>> entitySearchAnnotations =
        getSearchableAnnotations();
    List<SearchableAnnotation> annotations =
        entitySearchAnnotations.values().stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    defaultQueryFieldNames = getDefaultQueryFieldNames(annotations);
    highlights = getDefaultHighlights(opContext);
    v3SearchQueryBuilder =
        EntitySearchIndexResolver.shouldReadV3(configs.getEntityIndex())
            ? new V3SearchQueryBuilder(configs.getSearch(), customSearchConfiguration)
            : null;
    searchQueryBuilder =
        v3SearchQueryBuilder != null
            ? v3SearchQueryBuilder
            : new SearchQueryBuilder(configs.getSearch(), customSearchConfiguration, false);
    v3MinWordLength =
        configs.getIndex() != null ? configs.getIndex().getMinSearchFilterLength() : 0;
    aggregationQueryBuilder =
        new AggregationQueryBuilder(
            configs.getSearch(),
            entitySearchAnnotations,
            EntitySearchIndexResolver.shouldReadV3(configs.getEntityIndex()));
    this.searchServiceConfig = searchServiceConfig;
    searchableFieldTypes = opContext.getSearchContext().getSearchableFieldTypes();
    searchableFieldPaths = opContext.getSearchContext().getSearchableFieldPaths();
    this.queryFilterRewriteChain = queryFilterRewriteChain;
    this.entityIndexConfiguration = configs.getEntityIndex();
    this.customizedQueryHandler =
        CustomizedQueryHandler.builder(configs.getSearch().getCustom(), customSearchConfiguration)
            .build();
  }

  public static SearchRequestHandler getBuilder(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntitySpec entitySpec,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchServiceConfiguration searchServiceConfiguration) {
    return getBuilder(
        systemOperationContext,
        ImmutableList.of(entitySpec),
        configs,
        customSearchConfiguration,
        queryFilterRewriteChain,
        searchServiceConfiguration);
  }

  public static SearchRequestHandler getBuilder(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchServiceConfiguration searchServiceConfiguration) {
    return REQUEST_HANDLER_BY_ENTITY_NAME.computeIfAbsent(
        new SearchHandlerKey(
            ImmutableList.copyOf(entitySpecs),
            configs,
            customSearchConfiguration,
            queryFilterRewriteChain,
            searchServiceConfiguration),
        k ->
            new SearchRequestHandler(
                systemOperationContext,
                entitySpecs,
                configs,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchServiceConfiguration));
  }

  private Map<EntitySpec, List<SearchableAnnotation>> getSearchableAnnotations() {
    return entitySpecs.stream()
        .map(
            spec ->
                Pair.of(
                    spec,
                    spec.getSearchableFieldSpecs().stream()
                        .map(SearchableFieldSpec::getSearchableAnnotation)
                        .collect(Collectors.toList())))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  @VisibleForTesting
  private Set<String> getDefaultQueryFieldNames(List<SearchableAnnotation> annotations) {
    return Stream.concat(
            annotations.stream()
                .filter(SearchableAnnotation::isQueryByDefault)
                .map(SearchableAnnotation::getFieldName),
            Stream.of("urn"))
        .collect(Collectors.toSet());
  }

  @Override
  protected Collection<String> getValidQueryFieldNames() {
    return searchableFieldTypes.keySet();
  }

  public BoolQueryBuilder getFilterQuery(
      @Nonnull OperationContext opContext, @Nullable Filter filter) {
    return getFilterQuery(
        opContext,
        this.entityNames,
        filter,
        searchableFieldTypes,
        queryFilterRewriteChain,
        entityIndexConfiguration);
  }

  public static BoolQueryBuilder getFilterQuery(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityNames,
      @Nullable Filter filter,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    return getFilterQuery(
        opContext, entityNames, filter, searchableFieldTypes, queryFilterRewriteChain, null);
  }

  public static BoolQueryBuilder getFilterQuery(
      @Nonnull OperationContext opContext,
      @Nonnull final List<String> entityNames,
      @Nullable Filter filter,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nullable EntityIndexConfiguration entityIndexConfiguration) {
    final boolean readV3 = EntitySearchIndexResolver.shouldReadV3(entityIndexConfiguration);
    BoolQueryBuilder filterQuery =
        ESUtils.buildFilterQuery(
            readV3 ? ESUtils.toV3EntityFilter(opContext, filter) : filter,
            false,
            searchableFieldTypes,
            opContext,
            queryFilterRewriteChain);
    return applyDefaultSearchFilters(
        opContext, entityNames, filter, filterQuery, entityIndexConfiguration);
  }

  /**
   * Constructs the search query based on the query request.
   *
   * <p>TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or
   * later
   *
   * @param input the search input text
   * @param filter the search filter
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param facets list of facets we want aggregations for
   * @return a valid search request
   */
  @Nonnull
  @WithSpan
  public SearchRequest getSearchRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String input,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {

    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    SearchRequest searchRequest = new SearchRequest();
    applySearchType(searchRequest, opContext);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(from);
    searchSourceBuilder.size(ConfigUtils.applyLimit(searchServiceConfig, size));
    applyFetchSource(
        searchSourceBuilder, searchFlags, getV3MatchedFieldSources(opContext, searchFlags, input));

    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filter);
    searchSourceBuilder.query(
        QueryBuilders.boolQuery()
            .must(getQuery(opContext, input, Boolean.TRUE.equals(searchFlags.isFulltext())))
            .filter(filterQuery));
    if (Boolean.FALSE.equals(searchFlags.isSkipAggregates())) {
      aggregationQueryBuilder
          .getAggregations(opContext, facets)
          .forEach(searchSourceBuilder::aggregation);
    }
    // Search V3 finds matched fields after the query instead (see V3MatchedFields)
    if (v3SearchQueryBuilder == null && Boolean.FALSE.equals(searchFlags.isSkipHighlighting())) {
      // Apply custom highlight configuration
      HighlightBuilder highlightBuilder = getHighlightBuilder(opContext, searchFlags);
      searchSourceBuilder.highlighter(highlightBuilder);
    }

    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);

    if (Boolean.TRUE.equals(searchFlags.isGetSuggestions())) {
      ESUtils.buildNameSuggestions(searchSourceBuilder, input);
    }

    // Enable Elasticsearch explain if requested (use searchFlags parameter directly)
    if (Boolean.TRUE.equals(searchFlags.isIncludeExplain())) {
      searchSourceBuilder.explain(true);
    }

    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: {}", searchRequest);
    return searchRequest;
  }

  /**
   * Constructs the search query based on the query request.
   *
   * <p>TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or
   * later
   *
   * @param input the search input text
   * @param filter the search filter
   * @param sort sort values of the last result of the previous page
   * @param size the number of search hits to return
   * @return a valid search request
   */
  @Nonnull
  @WithSpan
  public SearchRequest getSearchRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String input,
      @Nullable Filter filter,
      List<SortCriterion> sortCriteria,
      @Nullable Object[] sort,
      @Nullable String pitId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    SearchRequest searchRequest = new PITAwareSearchRequest();
    applySearchType(searchRequest, opContext);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);
    ESUtils.setSliceOptions(searchSourceBuilder, searchFlags.getSliceOptions());

    searchSourceBuilder.size(ConfigUtils.applyLimit(searchServiceConfig, size));
    applyFetchSource(
        searchSourceBuilder, searchFlags, getV3MatchedFieldSources(opContext, searchFlags, input));

    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filter);
    searchSourceBuilder.query(
        QueryBuilders.boolQuery()
            .must(getQuery(opContext, input, Boolean.TRUE.equals(searchFlags.isFulltext())))
            .filter(filterQuery));
    if (Boolean.FALSE.equals(searchFlags.isSkipAggregates())) {
      aggregationQueryBuilder
          .getAggregations(opContext, facets)
          .forEach(searchSourceBuilder::aggregation);
    }
    // Search V3 finds matched fields after the query instead (see V3MatchedFields)
    if (v3SearchQueryBuilder == null && Boolean.FALSE.equals(searchFlags.isSkipHighlighting())) {
      // Apply custom highlight configuration
      HighlightBuilder highlightBuilder = getHighlightBuilder(opContext, searchFlags);
      searchSourceBuilder.highlighter(highlightBuilder);
    }
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);

    // Enable Elasticsearch explain if requested (use searchFlags parameter directly)
    if (Boolean.TRUE.equals(searchFlags.isIncludeExplain())) {
      searchSourceBuilder.explain(true);
    }

    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: {}", searchRequest);
    searchRequest.indicesOptions(null);

    return searchRequest;
  }

  /**
   * Applies search type from SearchFlags to the SearchRequest.
   *
   * @param searchRequest the search request to configure
   * @param opContext operation context holding the search flags
   */
  private void applySearchType(SearchRequest searchRequest, OperationContext opContext) {
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    String searchType = searchFlags.getSearchType();
    if (SearchType.DFS_QUERY_THEN_FETCH.name().equalsIgnoreCase(searchType)) {
      searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
    } else {
      searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
    }
  }

  /**
   * Returns a {@link SearchRequest} given filters to be applied to search query and sort criterion
   * to be applied to search results.
   *
   * @param filters {@link Filter} list of conditions with fields and values
   * @param sortCriteria list of {@link SortCriterion} to be applied to the search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return {@link SearchRequest} that contains the filtered query
   */
  @Nonnull
  public SearchRequest getFilterRequest(
      @Nonnull OperationContext opContext,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    SearchRequest searchRequest = new SearchRequest();

    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filters);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQuery);
    searchSourceBuilder.from(from).size(ConfigUtils.applyLimit(searchServiceConfig, size));
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  /**
   * Get search request to aggregate and get document counts per field value
   *
   * @param field Field to aggregate by
   * @param filter {@link Filter} list of conditions with fields and values
   * @param limit number of aggregations to return
   * @return {@link SearchRequest} that contains the aggregation query
   */
  @Nonnull
  public SearchRequest getAggregationRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String field,
      @Nullable Filter filter,
      @Nullable Integer limit) {

    SearchRequest searchRequest = new SearchRequest();
    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filter);

    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQuery);
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(
        AggregationBuilders.terms(field)
            .field(ESUtils.toKeywordField(opContext, field, false, opContext.getAspectRetriever()))
            .size(ConfigUtils.applyLimit(searchServiceConfig, limit)));
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  public QueryBuilder getQuery(
      @Nonnull OperationContext opContext, @Nonnull String query, boolean fulltext) {
    return searchQueryBuilder.buildQuery(opContext, entitySpecs, query, fulltext);
  }

  /**
   * Build the query, optionally the light Stage 1 query without its expensive clauses (fuzzy,
   * wildcard). Null for a light query that a custom configuration leaves without any clause.
   *
   * @see SearchQueryBuilder#buildQuery(OperationContext, List, String, boolean, boolean)
   */
  @Nullable
  public QueryBuilder getQuery(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean fulltext,
      boolean skipExpensiveClauses) {
    return searchQueryBuilder.buildQuery(
        opContext, entitySpecs, query, fulltext, skipExpensiveClauses);
  }

  private static void applyFetchSource(
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable SearchFlags searchFlags,
      @Nonnull Collection<String> matchedFieldSources) {
    Set<String> includes =
        new LinkedHashSet<>(
            SearchDocFieldFetchConfig.resolve(
                SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SCROLL, searchFlags));
    includes.addAll(matchedFieldSources);
    searchSourceBuilder.fetchSource(includes.toArray(String[]::new), null);
  }

  /**
   * On Search V3, the root fields whose values {@link V3MatchedFields} checks: those feeding the
   * shared fields the query reads that {@link V3SearchFields#matchedFieldSources} keeps, and the
   * urn, as V2 highlights the fields it queries. Empty on V2, when highlighting is off, for a
   * structured query (the {@code /q} prefix, or a search that is not full text), which names its
   * own fields and operators that word matching would misread, and for a query with no word that
   * could match, such as only stop words.
   */
  @Nonnull
  private List<String> getV3MatchedFieldSources(
      @Nonnull OperationContext opContext,
      @Nullable SearchFlags searchFlags,
      @Nullable String input) {
    if (v3SearchQueryBuilder == null
        || searchFlags == null
        || Boolean.TRUE.equals(searchFlags.isSkipHighlighting())
        || !Boolean.TRUE.equals(searchFlags.isFulltext())
        || input == null
        || input.startsWith(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX)
        || !new V3MatchedFields(input, v3MinWordLength).hasQueryWords()) {
      return List.of();
    }
    String fieldConfigLabel =
        customizedQueryHandler.resolveFieldConfiguration(
            searchFlags, CustomConfiguration::getSearchFieldConfigDefault);
    if (!customizedQueryHandler.isHighlightingEnabled(fieldConfigLabel)) {
      return List.of();
    }
    Set<String> sources =
        new LinkedHashSet<>(
            V3SearchFields.matchedFieldSources(
                v3SearchQueryBuilder.searchedFields(opContext, entitySpecs)));
    sources.add("urn");
    // Only a field the query reads can match, as V2 highlights only those
    if (CollectionUtils.isNotEmpty(searchFlags.getCustomHighlightingFields())) {
      Set<String> custom = v3SourceFields(searchFlags.getCustomHighlightingFields());
      return sources.stream().filter(custom::contains).collect(Collectors.toList());
    }
    // A highlight configuration names V2 fields and subfields; read them as root fields. Only a
    // field the query reads can match, as V2 highlights only those, so added fields change nothing
    HighlightFields highlightFields = getHighlightFields(fieldConfigLabel);
    if (highlightFields != null && !highlightFields.getReplace().isEmpty()) {
      Set<String> replace = v3SourceFields(highlightFields.getReplace());
      return sources.stream().filter(replace::contains).collect(Collectors.toList());
    }
    Set<String> remove =
        highlightFields == null ? Set.of() : v3SourceFields(highlightFields.getRemove());
    return sources.stream().filter(field -> !remove.contains(field)).collect(Collectors.toList());
  }

  @Nullable
  private HighlightFields getHighlightFields(@Nullable String fieldConfigLabel) {
    CustomSearchConfiguration customSearchConfiguration =
        customizedQueryHandler.getCustomSearchConfiguration();
    if (fieldConfigLabel == null
        || customSearchConfiguration == null
        || customSearchConfiguration.getFieldConfigurations() == null) {
      return null;
    }
    FieldConfiguration fieldConfiguration =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);
    return fieldConfiguration == null ? null : fieldConfiguration.getHighlightFields();
  }

  @Nonnull
  private static Set<String> v3SourceFields(@Nonnull Collection<String> highlightFields) {
    return highlightFields.stream()
        .map(SearchRequestHandler::v3SourceField)
        .collect(Collectors.toSet());
  }

  /**
   * The root field a V2 highlight field names: V2 highlights subfields such as {@code
   * name.delimited} or {@code name.*}, while V3 matches the root field's value.
   */
  @Nonnull
  private static String v3SourceField(@Nonnull final String highlightField) {
    String field = highlightField;
    int subfield;
    while ((subfield = field.lastIndexOf('.')) > 0
        && V2_HIGHLIGHT_SUBFIELDS.contains(field.substring(subfield + 1))) {
      field = field.substring(0, subfield);
    }
    return field;
  }

  @Override
  protected Stream<String> highlightFieldExpansion(
      @Nonnull OperationContext opContext, @Nonnull String fieldName) {
    // If the field already ends with .*, don't expand it further
    if (fieldName.endsWith(".*")) {
      return Stream.of(fieldName);
    }

    // For normal fields, expand as before
    return Stream.of(fieldName, fieldName + ".*");
  }

  @WithSpan
  public SearchResult extractResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Filter filter,
      int from,
      @Nullable Integer size) {
    return extractResult(opContext, searchResponse, filter, from, size, null);
  }

  /**
   * @param input the search input, from which Search V3 finds the fields each hit matched on; null
   *     reports none on V3
   */
  @WithSpan
  public SearchResult extractResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Filter filter,
      int from,
      @Nullable Integer size,
      @Nullable String input) {
    handleShardFailures(opContext, searchResponse);
    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    Collection<SearchEntity> resultList = getRestrictedResults(opContext, searchResponse, input);
    SearchResultMetadata searchResultMetadata =
        extractSearchResultMetadata(opContext, searchResponse, filter);

    return new SearchResult()
        .setEntities(new SearchEntityArray(resultList))
        .setMetadata(searchResultMetadata)
        .setFrom(from)
        .setPageSize(ConfigUtils.applyLimit(searchServiceConfig, size))
        .setNumEntities(totalCount);
  }

  @WithSpan
  public ScrollResult extractScrollResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Filter filter,
      @Nullable String keepAlive,
      @Nullable Integer size,
      boolean supportsPointInTime) {
    return extractScrollResult(
        opContext, searchResponse, filter, keepAlive, size, supportsPointInTime, null);
  }

  /**
   * @param input the search input, from which Search V3 finds the fields each hit matched on; null
   *     reports none on V3
   */
  @WithSpan
  public ScrollResult extractScrollResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Filter filter,
      @Nullable String keepAlive,
      @Nullable Integer size,
      boolean supportsPointInTime,
      @Nullable String input) {
    handleShardFailures(opContext, searchResponse);
    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    size = ConfigUtils.applyLimit(searchServiceConfig, size);

    // Build per-hit results and attach a per-element scrollId
    final SearchHit[] searchHits = searchResponse.getHits().getHits();
    long expirationTimeMs = 0L;
    if (keepAlive != null && supportsPointInTime) {
      expirationTimeMs =
          TimeValue.parseTimeValue(keepAlive, "expirationTime").getMillis()
              + System.currentTimeMillis();
    }

    List<SearchEntity> results = new ArrayList<>(searchHits.length);
    final Function<SearchHit, List<MatchedField>> matchedFields = matchedFieldsOf(opContext, input);
    for (SearchHit hit : searchHits) {
      // Build base SearchEntity — skip hits with missing/invalid URN rather than crashing
      Optional<SearchEntity> maybeEntity = getResultSafely(opContext, hit, matchedFields);
      if (maybeEntity.isEmpty()) {
        continue;
      }
      SearchEntity entity = maybeEntity.get();
      // Compute per-hit scrollId using this hit's sort values
      Object[] sort = hit.getSortValues();
      String perHitScrollId =
          new SearchAfterWrapper(sort, searchResponse.pointInTimeId(), expirationTimeMs)
              .toScrollId();
      // Merge into existing extraFields if present
      StringMap extra = entity.getExtraFields();
      if (extra == null) {
        entity.setExtraFields(new StringMap(Map.of("scrollId", perHitScrollId)));
      } else {
        extra.put("scrollId", perHitScrollId);
        entity.setExtraFields(extra);
      }
      results.add(entity);
    }

    // Apply access control restrictions while preserving order
    Collection<SearchEntity> resultList =
        ESAccessControlUtil.restrictSearchResult(opContext, results);

    SearchResultMetadata searchResultMetadata =
        extractSearchResultMetadata(opContext, searchResponse, filter);

    // Only return next scroll ID if there are more results, indicated by full size results
    String nextScrollId = null;
    if (searchHits.length == size && searchHits.length > 0) {
      Object[] lastSort = searchHits[searchHits.length - 1].getSortValues();
      nextScrollId =
          new SearchAfterWrapper(lastSort, searchResponse.pointInTimeId(), expirationTimeMs)
              .toScrollId();
    }

    ScrollResult scrollResult =
        new ScrollResult()
            .setEntities(new SearchEntityArray(resultList))
            .setMetadata(searchResultMetadata)
            .setPageSize(Math.min(size, totalCount))
            .setNumEntities(totalCount);

    if (nextScrollId != null) {
      scrollResult.setScrollId(nextScrollId);
    }
    return scrollResult;
  }

  /**
   * Surfaces per-shard failures that Elasticsearch reports inside an HTTP 200 response. Without
   * this check, hits from failing shards are silently dropped and entities simply vanish from
   * results. Deterministic failures (query/mapping bugs that fail on every retry, e.g. a terms
   * aggregation on a dynamically-mapped text field) throw so callers see a loud error; transient
   * failures (circuit breakers, timeouts, rejected executions on a busy cluster) keep the partial
   * result and are surfaced via log + metric only.
   */
  @VisibleForTesting
  void handleShardFailures(
      @Nonnull OperationContext opContext, @Nonnull SearchResponse searchResponse) {
    ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
    if (shardFailures == null || shardFailures.length == 0) {
      return;
    }
    for (ShardSearchFailure failure : shardFailures) {
      if (isDeterministicShardFailure(failure)) {
        throw new ESQueryException(
            String.format(
                "Search response had %d/%d failed shards with a deterministic query failure: %s",
                shardFailures.length, searchResponse.getTotalShards(), shardFailureReason(failure)),
            failure.getCause());
      }
    }
    log.warn(
        "Search response had {}/{} failed shards (transient). First failure: {}",
        shardFailures.length,
        searchResponse.getTotalShards(),
        shardFailureReason(shardFailures[0]));
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                metricUtils.increment(
                    SearchRequestHandler.class, "transientShardFailures", shardFailures.length));
  }

  private static boolean isDeterministicShardFailure(@Nonnull ShardSearchFailure failure) {
    String reason = shardFailureReason(failure).toLowerCase(Locale.ROOT);
    // Covers both the REST-parsed form ("type=illegal_argument_exception") and the local/transport
    // form (the cause's class name), plus the specific fielddata error Lucene emits when a terms
    // aggregation hits a dynamically-mapped text field.
    // NOTE: the ES8 client shim rebuilds shard failures from the reason message alone and drops the
    // exception type, so on ES8 a non-fielddata illegal_argument error lacks the type token and
    // falls through to the transient path (no regression vs pre-change — the text-fielddata symptom
    // that causes the SP poisoning still matches on every backend). Carrying the type through the
    // shim is a follow-up.
    // A clause overflow (too_many_nested_clauses) also repeats for the same query, but it stays
    // here: failing the search would drop the results of the indices that answered too.
    return reason.contains("illegal_argument_exception")
        || reason.contains("illegalargumentexception")
        || reason.contains("text fields are not optimised");
  }

  private static String shardFailureReason(@Nonnull ShardSearchFailure failure) {
    StringBuilder reason = new StringBuilder();
    if (failure.reason() != null) {
      reason.append(failure.reason());
    }
    Throwable cause = failure.getCause();
    if (cause != null) {
      reason.append(' ').append(cause);
    }
    return reason.toString();
  }

  /**
   * How the hits of one response find their matched fields: from the highlights on V2, and on V3
   * from the fetched values of the fields the query searched, which are worked out once for the
   * response.
   */
  @Nonnull
  private Function<SearchHit, List<MatchedField>> matchedFieldsOf(
      @Nonnull OperationContext opContext, @Nullable String input) {
    if (v3SearchQueryBuilder == null) {
      return this::extractMatchedFields;
    }
    final List<String> fields =
        getV3MatchedFieldSources(opContext, opContext.getSearchContext().getSearchFlags(), input);
    if (fields.isEmpty()) {
      return hit -> List.of();
    }
    final V3MatchedFields matcher = new V3MatchedFields(input, v3MinWordLength);
    return hit -> {
      final Map<String, Object> source = hit.getSourceAsMap();
      return source == null ? List.of() : matcher.find(source, fields);
    };
  }

  @Nonnull
  private List<MatchedField> extractMatchedFields(@Nonnull SearchHit hit) {
    // getHighlightFields() and getMatchedQueries() can be null for a valid hit (no highlight / no
    // named-query match). Default them to empty: now that getResultSafely only catches
    // InvalidSearchHitException, an unguarded NPE here would fail the whole search instead of
    // skipping a single bad hit.
    Map<String, HighlightField> highlightedFields = hit.getHighlightFields();
    if (highlightedFields == null) {
      highlightedFields = Map.of();
    }
    // Keep track of unique field values that matched for a given field name
    Map<String, Set<String>> highlightedFieldNamesAndValues = new HashMap<>();
    for (Map.Entry<String, HighlightField> entry : highlightedFields.entrySet()) {
      // Get the field name from source e.g. name.delimited -> name
      Optional<String> fieldName = getFieldName(entry.getKey());
      if (fieldName.isEmpty()) {
        continue;
      }
      if (!highlightedFieldNamesAndValues.containsKey(fieldName.get())) {
        highlightedFieldNamesAndValues.put(fieldName.get(), new HashSet<>());
      }
      for (Text fieldValue : entry.getValue().getFragments()) {
        highlightedFieldNamesAndValues.get(fieldName.get()).add(fieldValue.string());
      }
    }
    // fallback matched query, non-analyzed field
    String[] matchedQueries =
        hit.getMatchedQueries() != null ? hit.getMatchedQueries() : new String[0];
    for (String queryName : matchedQueries) {
      if (!highlightedFieldNamesAndValues.containsKey(queryName)) {
        if (hit.getFields().containsKey(queryName)) {
          for (Object fieldValue : hit.getFields().get(queryName).getValues()) {
            highlightedFieldNamesAndValues
                .computeIfAbsent(queryName, k -> new HashSet<>())
                .add(fieldValue.toString());
          }
        } else {
          highlightedFieldNamesAndValues.put(queryName, Set.of(""));
        }
      }
    }
    return highlightedFieldNamesAndValues.entrySet().stream()
        .flatMap(
            entry ->
                entry.getValue().stream()
                    .map(value -> new MatchedField().setName(entry.getKey()).setValue(value)))
        .collect(Collectors.toList());
  }

  private HighlightBuilder getHighlightBuilder(
      @Nonnull OperationContext opContext, @Nonnull SearchFlags searchFlags) {

    // Get field configuration label
    String fieldConfigLabel =
        customizedQueryHandler.resolveFieldConfiguration(
            searchFlags, CustomConfiguration::getSearchFieldConfigDefault);

    // Check if highlighting is enabled for this configuration
    if (!customizedQueryHandler.isHighlightingEnabled(fieldConfigLabel)) {
      return new HighlightBuilder().numOfFragments(0); // Effectively disable highlighting
    }

    // Determine base fields to highlight
    Set<String> baseHighlightFields;
    Set<String> explicitlyConfigured = Set.of();

    if (CollectionUtils.isNotEmpty(searchFlags.getCustomHighlightingFields())) {
      // If custom highlighting fields are specified in search flags, use them as base
      // Use LinkedHashSet to prevent duplicates while maintaining order
      baseHighlightFields = new LinkedHashSet<>(searchFlags.getCustomHighlightingFields());
    } else {
      // Otherwise use default query fields with expansion
      // LinkedHashSet prevents duplicates from expansion
      baseHighlightFields =
          defaultQueryFieldNames.stream()
              .flatMap(field -> highlightFieldExpansion(opContext, field))
              .collect(Collectors.toCollection(LinkedHashSet::new));

      // Apply custom highlight field configuration only when not using custom fields from search
      // flags
      HighlightConfigurationResult highlightConfig =
          customizedQueryHandler.getHighlightFieldConfiguration(
              baseHighlightFields, fieldConfigLabel);

      baseHighlightFields = highlightConfig.getFieldsToHighlight();
      explicitlyConfigured = highlightConfig.getExplicitlyConfiguredFields();
    }

    // Build highlights with the configured fields using the existing method from BaseRequestHandler
    return buildHighlightsWithSelectiveExpansion(
        opContext, baseHighlightFields, explicitlyConfigured);
  }

  @Nonnull
  private Optional<String> getFieldName(String matchedField) {
    return defaultQueryFieldNames.stream().filter(matchedField::startsWith).findFirst();
  }

  private Map<String, Double> extractFeatures(@Nonnull SearchHit searchHit) {
    return ImmutableMap.of(
        Features.Name.SEARCH_BACKEND_SCORE.toString(), (double) searchHit.getScore());
  }

  private SearchEntity getResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchHit hit,
      @Nonnull Function<SearchHit, List<MatchedField>> matchedFields) {
    SearchEntity entity =
        new SearchEntity()
            .setEntity(getUrnFromSearchHit(hit))
            .setMatchedFields(new MatchedFieldArray(matchedFields.apply(hit)))
            .setScore(hit.getScore())
            .setFeatures(new DoubleMap(extractFeatures(hit)));
    SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    if (flags != null && CollectionUtils.isNotEmpty(flags.getFetchExtraFields())) {
      entity.setExtraFields(
          SearchResultUtils.toExtraFields(
              opContext.getObjectMapper(), hit.getSourceAsMap(), flags.getFetchExtraFields()));
    }
    // Extract and serialize explanation if available
    if (hit.getExplanation() != null) {
      try {
        String explanationJson =
            serializeExplanation(opContext.getObjectMapper(), hit.getExplanation());
        StringMap extraFields = entity.hasExtraFields() ? entity.getExtraFields() : new StringMap();
        extraFields.put("_explain", explanationJson);
        entity.setExtraFields(extraFields);
      } catch (Exception e) {
        log.warn("Failed to serialize explanation for document: {}", hit.getId(), e);
        // Continue without explanation rather than failing the search
      }
    }
    return entity;
  }

  /**
   * Serializes Elasticsearch Explanation to JSON string.
   *
   * @param objectMapper Jackson ObjectMapper for JSON serialization
   * @param explanation Elasticsearch Explanation object
   * @return JSON string representation of the explanation
   */
  private String serializeExplanation(
      @Nonnull ObjectMapper objectMapper, @Nonnull Explanation explanation)
      throws JsonProcessingException {
    Map<String, Object> explanationMap = explanationToMap(explanation);
    return objectMapper.writeValueAsString(explanationMap);
  }

  /**
   * Recursively converts Explanation to Map for JSON serialization.
   *
   * @param explanation Elasticsearch Explanation object
   * @return Map representation suitable for JSON serialization
   */
  private Map<String, Object> explanationToMap(@Nonnull Explanation explanation) {
    Map<String, Object> map = new HashMap<>();
    map.put("value", explanation.getValue().floatValue());
    map.put("description", explanation.getDescription());
    map.put("match", explanation.isMatch());

    Explanation[] details = explanation.getDetails();
    if (details != null && details.length > 0) {
      List<Map<String, Object>> detailsList = new ArrayList<>();
      for (Explanation detail : details) {
        detailsList.add(explanationToMap(detail));
      }
      map.put("details", detailsList);
    }

    return map;
  }

  /**
   * Builds a {@link SearchEntity} for a hit, returning empty (and skipping the hit) only when its
   * URN is missing or invalid — e.g. documents created by older bootstrap code (see issue #13181).
   *
   * <p>Any other failure is left to propagate: silently swallowing it would mask real bugs and
   * could drop valid results without surfacing the cause. The narrow {@link
   * InvalidSearchHitException} catch ensures only the known, recoverable data-quality condition is
   * tolerated.
   */
  private Optional<SearchEntity> getResultSafely(
      @Nonnull OperationContext opContext,
      @Nonnull SearchHit hit,
      @Nonnull Function<SearchHit, List<MatchedField>> matchedFields) {
    try {
      return Optional.of(getResult(opContext, hit, matchedFields));
    } catch (InvalidSearchHitException e) {
      log.warn(
          "Skipping search hit with invalid or missing URN. Index: {}, ID: {}",
          hit.getIndex(),
          hit.getId(),
          e);
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(SearchRequestHandler.class, "skippedInvalidSearchHit", 1));
      return Optional.empty();
    }
  }

  /**
   * Gets list of entities returned in the search response, skipping any hits with missing or
   * invalid URN fields (e.g. documents created by older bootstrap code) instead of crashing the
   * entire search operation.
   *
   * @param searchResponse the raw search response from search engine
   * @return List of search entities
   */
  @Nonnull
  private Collection<SearchEntity> getRestrictedResults(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      @Nullable String input) {
    final Function<SearchHit, List<MatchedField>> matchedFields = matchedFieldsOf(opContext, input);
    List<SearchEntity> results =
        Arrays.stream(searchResponse.getHits().getHits())
            .flatMap(hit -> getResultSafely(opContext, hit, matchedFields).stream())
            .collect(Collectors.toList());
    return ESAccessControlUtil.restrictSearchResult(opContext, results);
  }

  @Nonnull
  private Urn getUrnFromSearchHit(@Nonnull SearchHit hit) {
    return UrnExtractionUtils.extractUrnFromSearchHit(hit);
  }

  /**
   * Extracts SearchResultMetadata section.
   *
   * @param searchResponse the raw {@link SearchResponse} as obtained from the search engine
   * @param filter the provided Filter to use with Elasticsearch
   * @return {@link SearchResultMetadata} with aggregation and list of urns obtained from {@link
   *     SearchResponse}
   */
  @Nonnull
  private SearchResultMetadata extractSearchResultMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      @Nullable Filter filter) {
    final SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray());

    if (Boolean.FALSE.equals(searchFlags.isSkipAggregates())) {
      final List<AggregationMetadata> aggregationMetadataList =
          aggregationQueryBuilder.extractAggregationMetadata(
              searchResponse, filter, opContext, opContext.getAspectRetriever());
      searchResultMetadata.setAggregations(new AggregationMetadataArray(aggregationMetadataList));
    }

    final List<SearchSuggestion> searchSuggestions = extractSearchSuggestions(searchResponse);
    searchResultMetadata.setSuggestions(new SearchSuggestionArray(searchSuggestions));

    return searchResultMetadata;
  }

  private List<SearchSuggestion> extractSearchSuggestions(@Nonnull SearchResponse searchResponse) {
    final List<SearchSuggestion> searchSuggestions = new ArrayList<>();
    if (searchResponse.getSuggest() != null) {
      TermSuggestion termSuggestion = searchResponse.getSuggest().getSuggestion(NAME_SUGGESTION);
      if (termSuggestion != null && !termSuggestion.getEntries().isEmpty()) {
        termSuggestion
            .getEntries()
            .get(0)
            .getOptions()
            .forEach(
                suggestOption -> {
                  SearchSuggestion searchSuggestion = new SearchSuggestion();
                  searchSuggestion.setText(String.valueOf(suggestOption.getText()));
                  searchSuggestion.setFrequency(suggestOption.getFreq());
                  searchSuggestion.setScore(suggestOption.getScore());
                  searchSuggestions.add(searchSuggestion);
                });
      }
    }
    return searchSuggestions;
  }

  /**
   * Enhanced cache key implementation to prevent handler cross-contamination in tests.
   *
   * <p>Background: Flaky tests occurred because the cache key (previously just entitySpecs) didn't
   * account for all configuration variants. Identical entitySpecs with different search
   * configurations would incorrectly share handlers, leading to test instability.
   *
   * <p>This key ensures each unique configuration combination gets its own handler instance.
   */
  @Value
  private static class SearchHandlerKey {
    @Nonnull private final List<EntitySpec> entitySpecs;
    @Nonnull private final ElasticSearchConfiguration configs;
    @Nullable private final CustomSearchConfiguration customSearchConfiguration;
    @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
    @Nonnull private final SearchServiceConfiguration searchServiceConfiguration;
  }
}
