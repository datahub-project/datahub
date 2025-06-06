package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.api.SearchDocFieldFetchConfig.*;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.MappingsBuilder.ALIAS_FIELD_TYPE;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.MappingsBuilder.PATH;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.*;
import static com.linkedin.metadata.search.utils.ESUtils.DATE_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.OBJECT_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.applyDefaultSearchFilters;
import static com.linkedin.metadata.search.utils.ESUtils.applyResultLimit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
import com.linkedin.metadata.search.elasticsearch.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterContext;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.ESPredicateUtils;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
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

  private static final Map<List<EntitySpec>, SearchRequestHandler> REQUEST_HANDLER_BY_ENTITY_NAME =
      new ConcurrentHashMap<>();
  private static final String URN_FILTER = "urn";
  private static final String[] URN_FIELD = new String[] {"urn"};
  private final List<EntitySpec> entitySpecs;
  @Getter private final Set<String> defaultQueryFieldNames;
  @Nonnull private final HighlightBuilder highlights;

  private final ElasticSearchConfiguration configs;
  private final SearchQueryBuilder searchQueryBuilder;
  private final AggregationQueryBuilder aggregationQueryBuilder;
  private final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes;
  private final Map<PathSpec, String> searchableFieldPaths;

  private final QueryFilterRewriteChain queryFilterRewriteChain;

  private SearchRequestHandler(
      @Nonnull OperationContext opContext,
      @Nonnull EntitySpec entitySpec,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    this(
        opContext,
        ImmutableList.of(entitySpec),
        configs,
        customSearchConfiguration,
        queryFilterRewriteChain);
  }

  private SearchRequestHandler(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    this.entitySpecs = entitySpecs;
    Map<EntitySpec, List<SearchableAnnotation>> entitySearchAnnotations =
        getSearchableAnnotations();
    List<SearchableAnnotation> annotations =
        entitySearchAnnotations.values().stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    defaultQueryFieldNames = getDefaultQueryFieldNames(annotations);
    highlights = getDefaultHighlights(opContext);
    searchQueryBuilder = new SearchQueryBuilder(configs.getSearch(), customSearchConfiguration);
    aggregationQueryBuilder =
        new AggregationQueryBuilder(configs.getSearch(), entitySearchAnnotations);
    this.configs = configs;
    this.searchableFieldTypes =
        buildSearchableFieldTypes(opContext.getEntityRegistry(), entitySpecs);
    searchableFieldPaths =
        this.entitySpecs.stream()
            .flatMap(entitySpec -> entitySpec.getSearchableFieldPathMap().entrySet().stream())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (s1, s2) -> {
                      if (!StringUtils.equals(s1, s2)) {
                        log.error("Merging values {} and {}, field paths should be unique", s1, s2);
                      }
                      return s1;
                    }));
    this.queryFilterRewriteChain = queryFilterRewriteChain;
  }

  public static SearchRequestHandler getBuilder(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntitySpec entitySpec,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    return REQUEST_HANDLER_BY_ENTITY_NAME.computeIfAbsent(
        ImmutableList.of(entitySpec),
        k ->
            new SearchRequestHandler(
                systemOperationContext,
                entitySpec,
                configs,
                customSearchConfiguration,
                queryFilterRewriteChain));
  }

  public static SearchRequestHandler getBuilder(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull ElasticSearchConfiguration configs,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    return REQUEST_HANDLER_BY_ENTITY_NAME.computeIfAbsent(
        ImmutableList.copyOf(entitySpecs),
        k ->
            new SearchRequestHandler(
                systemOperationContext,
                entitySpecs,
                configs,
                customSearchConfiguration,
                queryFilterRewriteChain));
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
    return getFilterQuery(opContext, filter, searchableFieldTypes, queryFilterRewriteChain);
  }

  public static BoolQueryBuilder getFilterQuery(
      @Nonnull OperationContext opContext,
      @Nullable Filter filter,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    BoolQueryBuilder filterQuery =
        ESUtils.buildFilterQuery(
            filter, false, searchableFieldTypes, opContext, queryFilterRewriteChain);
    return applyDefaultSearchFilters(opContext, filter, filterQuery);
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
      int size,
      @Nonnull List<String> facets) {

    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filter);
    SearchSourceBuilder searchSourceBuilder = constructSearchSourceBuilder(from, size);

    SearchRequest searchRequest = new SearchRequest();
    return buildSearchRequestPageAgnostic(
        opContext,
        searchRequest,
        searchSourceBuilder,
        input,
        searchFlags,
        filterQuery,
        facets,
        sortCriteria);
  }

  private SearchSourceBuilder constructSearchSourceBuilder(int from, int size) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.from(from);
    searchSourceBuilder.size(applyResultLimit(configs, size));
    searchSourceBuilder.fetchSource(DEFAULT_FIELDS_TO_FETCH_ON_SEARCH.toArray(new String[0]), null);
    return searchSourceBuilder;
  }

  /** Used for both searchAfter and from -> size requests */
  private SearchRequest buildSearchRequestPageAgnostic(
      OperationContext opContext,
      final SearchRequest searchRequest,
      final SearchSourceBuilder searchSourceBuilder,
      final String input,
      final SearchFlags searchFlags,
      final BoolQueryBuilder filterQuery,
      @Nonnull final List<String> facets,
      final List<SortCriterion> sortCriteria) {
    searchSourceBuilder.query(
        QueryBuilders.boolQuery()
            .must(getQuery(opContext, input, Boolean.TRUE.equals(searchFlags.isFulltext())))
            .filter(filterQuery));
    if (Boolean.FALSE.equals(searchFlags.isSkipAggregates())) {
      aggregationQueryBuilder
          .getAggregations(opContext, facets)
          .forEach(searchSourceBuilder::aggregation);
    }
    if (Boolean.FALSE.equals(searchFlags.isSkipHighlighting())) {
      if (CollectionUtils.isNotEmpty(searchFlags.getCustomHighlightingFields())) {
        searchSourceBuilder.highlighter(
            getHighlights(opContext, searchFlags.getCustomHighlightingFields()));
      } else {
        searchSourceBuilder.highlighter(highlights);
      }
    }

    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);

    if (Boolean.TRUE.equals(searchFlags.isGetSuggestions())) {
      ESUtils.buildNameSuggestions(searchSourceBuilder, input);
    }

    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: " + searchRequest);
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
      int size,
      @Nonnull List<String> facets,
      @Nullable SearchDocFieldFetchConfig fieldFetchConfig) {
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filter);
    SearchRequest searchRequest = new PITAwareSearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);

    searchSourceBuilder.size(applyResultLimit(configs, size));
    searchSourceBuilder.fetchSource("urn", null);

    if (fieldFetchConfig == null) {
      fieldFetchConfig = new SearchDocFieldFetchConfig();
    }
    if (Boolean.FALSE.equals(searchFlags.isSkipHighlighting())) {
      searchSourceBuilder.highlighter(highlights);
    }
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);
    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: " + searchRequest);
    searchRequest.indicesOptions(null);

    searchSourceBuilder.fetchSource(fieldFetchConfig.fieldsToFetch().toArray(new String[0]), null);

    return buildSearchRequestPageAgnostic(
            opContext,
            searchRequest,
            searchSourceBuilder,
            input,
            searchFlags,
            filterQuery,
            facets,
            sortCriteria)
        .indicesOptions(null);
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
      int size) {
    SearchRequest searchRequest = new SearchRequest();

    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filters);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQuery);
    searchSourceBuilder.from(from).size(applyResultLimit(configs, size));
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  /**
   * Returns a {@link SearchRequest} given filters to be applied to search query and sort criterion
   * to be applied to search results with scrolling capabilities, uses new SearchAfter instead of
   * legacy scroll
   *
   * @param filters {@link Filter} list of conditions with fields and values
   * @param sortCriteria list of {@link SortCriterion} to be applied to the search results
   * @param size the number of search hits to return
   * @param keepAliveDuration duration the search context should be kept alive i.e. 10s, 1m
   * @return {@link SearchRequest} that contains the filtered query
   */
  @Nonnull
  public SearchRequest getSearchAfterRequest(
      @Nonnull OperationContext opContext,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int size,
      @Nullable String keepAliveDuration,
      @Nullable String pitId,
      @Nullable Object[] sort,
      @Nullable SearchDocFieldFetchConfig fieldFetchConfig) {
    SearchRequest searchRequest = new SearchRequest();

    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filters);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQuery);
    searchSourceBuilder.size(size);
    if (fieldFetchConfig == null) {
      fieldFetchConfig = new SearchDocFieldFetchConfig();
    }
    searchSourceBuilder.fetchSource(fieldFetchConfig.fieldsToFetch().toArray(new String[0]), null);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);
    searchRequest.source(searchSourceBuilder);
    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAliveDuration);

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
      int limit) {

    SearchRequest searchRequest = new SearchRequest();
    BoolQueryBuilder filterQuery = getFilterQuery(opContext, filter);

    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQuery);
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(
        AggregationBuilders.terms(field)
            .field(ESUtils.toKeywordField(field, false, opContext.getAspectRetriever()))
            .size(limit));
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  public QueryBuilder getQuery(
      @Nonnull OperationContext opContext, @Nonnull String query, boolean fulltext) {
    return searchQueryBuilder.buildQuery(opContext, entitySpecs, query, fulltext);
  }

  @Override
  protected Stream<String> highlightFieldExpansion(
      @Nonnull OperationContext opContext, @Nonnull String fieldName) {
    return Stream.of(fieldName, fieldName + ".*");
  }

  @WithSpan
  public SearchResult extractResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Filter filter,
      int from,
      int size) {
    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    Collection<SearchEntity> resultList = getRestrictedResults(opContext, searchResponse);
    SearchResultMetadata searchResultMetadata =
        extractSearchResultMetadata(opContext, searchResponse, filter);

    return new SearchResult()
        .setEntities(new SearchEntityArray(resultList))
        .setMetadata(searchResultMetadata)
        .setFrom(from)
        .setPageSize(size)
        .setNumEntities(totalCount);
  }

  @WithSpan
  public ScrollResult extractScrollResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Filter filter,
      @Nullable String keepAlive,
      int size,
      boolean supportsPointInTime) {
    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    Collection<SearchEntity> resultList = getRestrictedResults(opContext, searchResponse);
    SearchResultMetadata searchResultMetadata =
        extractSearchResultMetadata(opContext, searchResponse, filter);
    SearchHit[] searchHits = searchResponse.getHits().getHits();
    // Only return next scroll ID if there are more results, indicated by full size results
    String nextScrollId = null;
    if (searchHits.length == size) {
      Object[] sort = searchHits[searchHits.length - 1].getSortValues();
      long expirationTimeMs = 0L;
      if (keepAlive != null && supportsPointInTime) {
        expirationTimeMs =
            TimeValue.parseTimeValue(keepAlive, "expirationTime").getMillis()
                + System.currentTimeMillis();
      }
      nextScrollId =
          new SearchAfterWrapper(sort, searchResponse.pointInTimeId(), expirationTimeMs)
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

  @Nonnull
  private List<MatchedField> extractMatchedFields(@Nonnull SearchHit hit) {
    Map<String, HighlightField> highlightedFields = hit.getHighlightFields();
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
    for (String queryName : hit.getMatchedQueries()) {
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

  @Nonnull
  private Optional<String> getFieldName(String matchedField) {
    return defaultQueryFieldNames.stream().filter(matchedField::startsWith).findFirst();
  }

  private Map<String, Double> extractFeatures(@Nonnull SearchHit searchHit) {
    Map<String, Double> features = new HashMap<>();
    features.put(Features.Name.SEARCH_BACKEND_SCORE.toString(), (double) searchHit.getScore());
    Optional.ofNullable(searchHit.getSourceAsMap().get("usageCountLast30Days"))
        .ifPresent(
            value ->
                features.put(Features.Name.QUERY_COUNT.toString(), ((Number) value).doubleValue()));
    return features;
  }

  private static StringMap getStringMap(
      @Nonnull ObjectMapper objectMapper, Map<String, Object> sourceAsMap) {
    StringMap stringMap = new StringMap();
    sourceAsMap.forEach(
        (key, value) -> {
          try {
            stringMap.put(key, objectMapper.writeValueAsString(value));
          } catch (IOException e) {
            log.warn("Failed to serialize extra field: " + key, e);
          }
        });
    return stringMap;
  }

  private SearchEntity getResult(@Nonnull ObjectMapper objectMapper, @Nonnull SearchHit hit) {
    return new SearchEntity()
        .setEntity(getUrnFromSearchHit(hit))
        .setMatchedFields(new MatchedFieldArray(extractMatchedFields(hit)))
        .setScore(hit.getScore())
        .setFeatures(new DoubleMap(extractFeatures(hit)))
        .setExtraFields(getStringMap(objectMapper, hit.getSourceAsMap()));
  }

  /**
   * Gets list of entities returned in the search response
   *
   * @param searchResponse the raw search response from search engine
   * @return List of search entities
   */
  @Nonnull
  private Collection<SearchEntity> getRestrictedResults(
      @Nonnull OperationContext opContext, @Nonnull SearchResponse searchResponse) {
    return ESAccessControlUtil.restrictSearchResult(
        opContext,
        Arrays.stream(searchResponse.getHits().getHits())
            .map(r -> getResult(opContext.getObjectMapper(), r))
            .collect(Collectors.toList()));
  }

  @Nonnull
  private Urn getUrnFromSearchHit(@Nonnull SearchHit hit) {
    try {
      return Urn.createFromString(hit.getSourceAsMap().get("urn").toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid urn in search document " + e);
    }
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
              searchResponse, filter, opContext.getAspectRetriever());
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
   * Calculate the field types based on annotations if available, with fallback to ES mappings
   *
   * @param entitySpecs entitySepcts
   * @return Field name to annotation field types
   */
  private static Map<String, Set<SearchableAnnotation.FieldType>> buildSearchableFieldTypes(
      @Nonnull EntityRegistry entityRegistry, @Nonnull List<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .flatMap(
            entitySpec -> {
              Map<String, Set<SearchableAnnotation.FieldType>> annotationFieldTypes =
                  entitySpec.getSearchableFieldTypes();

              // fallback to mappings
              Map<String, Map<String, Object>> rawMappingTypes =
                  ((Map<String, Object>)
                          MappingsBuilder.getMappings(entityRegistry, entitySpec)
                              .getOrDefault("properties", Map.<String, Object>of()))
                      .entrySet().stream()
                          .filter(
                              entry ->
                                  !annotationFieldTypes.containsKey(entry.getKey())
                                      && ((Map<String, Object>) entry.getValue()).containsKey(TYPE))
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey, e -> (Map<String, Object>) e.getValue()));

              Map<String, Set<SearchableAnnotation.FieldType>> mappingFieldTypes =
                  rawMappingTypes.entrySet().stream()
                      .map(
                          entry -> Map.entry(entry.getKey(), entry.getValue().get(TYPE).toString()))
                      .map(
                          entry ->
                              Map.entry(
                                  entry.getKey(),
                                  fallbackMappingToAnnotation(entry.getValue()).stream()
                                      .collect(Collectors.toSet())))
                      .filter(entry -> !entry.getValue().isEmpty())
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

              // aliases - pull from annotations
              Map<String, Set<SearchableAnnotation.FieldType>> aliasFieldTypes =
                  rawMappingTypes.entrySet().stream()
                      .filter(
                          entry -> ALIAS_FIELD_TYPE.equals(entry.getValue().get(TYPE).toString()))
                      .map(
                          entry ->
                              Map.entry(
                                  entry.getKey(),
                                  annotationFieldTypes.getOrDefault(
                                      entry.getValue().get(PATH).toString(),
                                      Collections.emptySet())))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

              List<SearchableFieldSpec> objectFieldSpec =
                  entitySpec.getSearchableFieldSpecs().stream()
                      .filter(
                          searchableFieldSpec ->
                              searchableFieldSpec.getSearchableAnnotation().getFieldType()
                                  == SearchableAnnotation.FieldType.OBJECT)
                      .collect(Collectors.toList());

              Map<String, Set<SearchableAnnotation.FieldType>> objectFieldTypes = new HashMap<>();

              objectFieldSpec.forEach(
                  fieldSpec -> {
                    String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
                    DataSchema.Type dataType =
                        ((MapDataSchema) fieldSpec.getPegasusSchema()).getValues().getType();

                    Set<SearchableAnnotation.FieldType> fieldType;

                    switch (dataType) {
                      case BOOLEAN:
                        fieldType = Set.of(SearchableAnnotation.FieldType.BOOLEAN);
                        break;
                      case INT:
                        fieldType = Set.of(SearchableAnnotation.FieldType.COUNT);
                        break;
                      case DOUBLE:
                      case LONG:
                      case FLOAT:
                        fieldType = Set.of(SearchableAnnotation.FieldType.DOUBLE);
                        break;
                      default:
                        fieldType = Set.of(SearchableAnnotation.FieldType.TEXT);
                        break;
                    }
                    objectFieldTypes.put(fieldName, fieldType);
                    annotationFieldTypes.remove(fieldName);
                  });

              return Stream.concat(
                  Stream.concat(
                      objectFieldTypes.entrySet().stream(),
                      annotationFieldTypes.entrySet().stream()),
                  Stream.concat(
                      mappingFieldTypes.entrySet().stream(), aliasFieldTypes.entrySet().stream()));
            })
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (set1, set2) -> {
                  set1.addAll(set2);
                  return set1;
                }));
  }

  private static Set<SearchableAnnotation.FieldType> fallbackMappingToAnnotation(
      @Nonnull String mappingType) {
    switch (mappingType) {
      case KEYWORD_FIELD_TYPE:
        return Set.of(SearchableAnnotation.FieldType.KEYWORD);
      case DATE_FIELD_TYPE:
        return Set.of(SearchableAnnotation.FieldType.DATETIME);
      case OBJECT_FIELD_TYPE:
        return Set.of(SearchableAnnotation.FieldType.OBJECT);
    }
    return Collections.emptySet();
  }

  // SAAS ONLY - Predicate support for filters

  /**
   * Constructs the search query based on the query request.
   *
   * <p>TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or
   * later
   *
   * @param input the search input text
   * @param predicate the search filter in predicate form
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @param facets list of facets we want aggregations for
   * @return a valid search request
   */
  @Nonnull
  @WithSpan
  public SearchRequest getPredicateSearchRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String input,
      @Nullable Predicate predicate,
      @Nullable List<SortCriterion> sortCriteria,
      int from,
      int size,
      @Nonnull List<String> facets) {
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    BoolQueryBuilder filterQuery = getFilterQuery(opContext, predicate);
    SearchSourceBuilder searchSourceBuilder = constructSearchSourceBuilder(from, size);

    SearchRequest searchRequest = new SearchRequest();
    return buildSearchRequestPageAgnostic(
        opContext,
        searchRequest,
        searchSourceBuilder,
        input,
        searchFlags,
        filterQuery,
        facets,
        sortCriteria);
  }

  public BoolQueryBuilder getFilterQuery(
      @Nonnull OperationContext opContext, @Nullable Predicate predicate) {
    BoolQueryBuilder filterQuery =
        ESPredicateUtils.buildFilterQuery(
            predicate,
            false,
            searchableFieldPaths,
            searchableFieldTypes,
            opContext,
            queryFilterRewriteChain);

    filterQuery = ESPredicateUtils.applyDefaultSearchFilters(opContext, predicate, filterQuery);

    filterQuery =
        queryFilterRewriteChain.rewrite(
            opContext,
            QueryFilterRewriterContext.builder()
                .searchFlags(opContext.getSearchContext().getSearchFlags())
                .queryFilterRewriteChain(queryFilterRewriteChain)
                .searchType(QueryFilterRewriterSearchType.PREDICATE)
                .build(false),
            filterQuery);

    return filterQuery;
  }

  @WithSpan
  public SearchResult extractPredicateResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Predicate predicate,
      int from,
      int size) {
    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    Collection<SearchEntity> resultList = getRestrictedResults(opContext, searchResponse);
    SearchResultMetadata searchResultMetadata =
        extractPredicateSearchResultMetadata(opContext, searchResponse, predicate);

    return new SearchResult()
        .setEntities(new SearchEntityArray(resultList))
        .setMetadata(searchResultMetadata)
        .setFrom(from)
        .setPageSize(size)
        .setNumEntities(totalCount);
  }

  /**
   * Extracts SearchResultMetadata section.
   *
   * @param searchResponse the raw {@link SearchResponse} as obtained from the search engine
   * @param predicate the provided predicate to use with Elasticsearch
   * @return {@link SearchResultMetadata} with aggregation and list of urns obtained from {@link
   *     SearchResponse}
   */
  @Nonnull
  private SearchResultMetadata extractPredicateSearchResultMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      @Nullable Predicate predicate) {
    final SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray());

    if (Boolean.FALSE.equals(searchFlags.isSkipAggregates())) {
      final List<AggregationMetadata> aggregationMetadataList =
          aggregationQueryBuilder.extractPredicateAggregationMetadata(
              searchResponse, predicate, opContext, searchableFieldPaths, searchableFieldTypes);
      searchResultMetadata.setAggregations(new AggregationMetadataArray(aggregationMetadataList));
    }

    final List<SearchSuggestion> searchSuggestions = extractSearchSuggestions(searchResponse);
    searchResultMetadata.setSuggestions(new SearchSuggestionArray(searchSuggestions));

    return searchResultMetadata;
  }

  @Nonnull
  @WithSpan
  public SearchRequest getPredicateSearchRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String input,
      @Nullable Predicate predicate,
      List<SortCriterion> sortCriteria,
      @Nullable Object[] sort,
      @Nullable String pitId,
      @Nullable String keepAlive,
      int size,
      @Nonnull List<String> facets,
      @Nullable SearchDocFieldFetchConfig fieldFetchConfig) {
    SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
    BoolQueryBuilder filterQuery = getFilterQuery(opContext, predicate);
    SearchRequest searchRequest = new PITAwareSearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);

    searchSourceBuilder.size(size);

    if (fieldFetchConfig == null) {
      fieldFetchConfig = new SearchDocFieldFetchConfig();
    }
    if (Boolean.FALSE.equals(searchFlags.isSkipHighlighting())) {
      searchSourceBuilder.highlighter(highlights);
    }
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriteria, entitySpecs);
    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: " + searchRequest);
    searchRequest.indicesOptions(null);

    searchSourceBuilder.fetchSource(fieldFetchConfig.fieldsToFetch().toArray(new String[0]), null);

    return buildSearchRequestPageAgnostic(
            opContext,
            searchRequest,
            searchSourceBuilder,
            input,
            searchFlags,
            filterQuery,
            facets,
            sortCriteria)
        .indicesOptions(null);
  }

  @WithSpan
  public ScrollResult extractPredicateScrollResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      Predicate predicate,
      @Nullable String keepAlive,
      int size,
      boolean supportsPointInTime) {
    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    Collection<SearchEntity> resultList = getRestrictedResults(opContext, searchResponse);
    SearchResultMetadata searchResultMetadata =
        extractPredicateSearchResultMetadata(opContext, searchResponse, predicate);
    SearchHit[] searchHits = searchResponse.getHits().getHits();
    // Only return next scroll ID if there are more results, indicated by full size results
    String nextScrollId = null;
    if (searchHits.length == size) {
      Object[] sort = searchHits[searchHits.length - 1].getSortValues();
      long expirationTimeMs = 0L;
      if (keepAlive != null && supportsPointInTime) {
        expirationTimeMs =
            TimeValue.parseTimeValue(keepAlive, "expirationTime").getMillis()
                + System.currentTimeMillis();
      }
      nextScrollId =
          new SearchAfterWrapper(sort, searchResponse.pointInTimeId(), expirationTimeMs)
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

  // END SAAS ONLY -- Add methods prior to this section
}
