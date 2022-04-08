package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.features.Features;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.SearchUtil;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;


@Slf4j
public class SearchRequestHandler {

  private static final Map<EntitySpec, SearchRequestHandler> REQUEST_HANDLER_BY_ENTITY_NAME = new ConcurrentHashMap<>();

  private final EntitySpec _entitySpec;
  private final Set<String> _facetFields;
  private final Set<String> _defaultQueryFieldNames;
  private final Map<String, String> _filtersToDisplayName;
  private final int _maxTermBucketSize = 100;
  private static final String REMOVED = "removed";

  private SearchRequestHandler(@Nonnull EntitySpec entitySpec) {
    _entitySpec = entitySpec;
    _facetFields = getFacetFields();
    _defaultQueryFieldNames = getDefaultQueryFieldNames();
    _filtersToDisplayName = _entitySpec.getSearchableFieldSpecs()
        .stream()
        .filter(spec -> spec.getSearchableAnnotation().isAddToFilters())
        .collect(Collectors.toMap(spec -> spec.getSearchableAnnotation().getFieldName(),
            spec -> spec.getSearchableAnnotation().getFilterName()));
  }

  public static SearchRequestHandler getBuilder(@Nonnull EntitySpec entitySpec) {
    return REQUEST_HANDLER_BY_ENTITY_NAME.computeIfAbsent(entitySpec, k -> new SearchRequestHandler(entitySpec));
  }

  private Set<String> getFacetFields() {
    return _entitySpec.getSearchableFieldSpecs()
        .stream()
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .filter(SearchableAnnotation::isAddToFilters)
        .map(SearchableAnnotation::getFieldName)
        .collect(Collectors.toSet());
  }

  private Set<String> getDefaultQueryFieldNames() {
    return _entitySpec.getSearchableFieldSpecs()
        .stream()
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .filter(SearchableAnnotation::isQueryByDefault)
        .map(SearchableAnnotation::getFieldName)
        .collect(Collectors.toSet());
  }

  public static BoolQueryBuilder getFilterQuery(@Nullable Filter filter) {
    BoolQueryBuilder filterQuery = ESUtils.buildFilterQuery(filter);

    boolean removedInOrFilter = false;
    if (filter != null) {
      removedInOrFilter = filter.getOr().stream().anyMatch(
              or -> or.getAnd().stream().anyMatch(criterion -> criterion.getField().equals(REMOVED))
      );
    }
    // Filter out entities that are marked "removed" if and only if filter does not contain a criterion referencing it.
    if (!removedInOrFilter) {
      filterQuery.mustNot(QueryBuilders.matchQuery(REMOVED, true));
    }

    return filterQuery;
  }

  /**
   * Constructs the search query based on the query request.
   *
   * <p>TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   *
   * @param input the search input text
   * @param filter the search filter
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a valid search request
   */
  @Nonnull
  @WithSpan
  public SearchRequest getSearchRequest(@Nonnull String input, @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion, int from, int size) {
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);
    searchSourceBuilder.fetchSource("urn", null);

    BoolQueryBuilder filterQuery = getFilterQuery(filter);
    searchSourceBuilder.query(QueryBuilders.boolQuery().must(getQuery(input)).must(filterQuery));
    getAggregations().forEach(searchSourceBuilder::aggregation);
    searchSourceBuilder.highlighter(getHighlights());
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);
    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: " + searchRequest.toString());

    return searchRequest;
  }

  /**
   * Returns a {@link SearchRequest} given filters to be applied to search query and sort criterion to be applied to
   * search results.
   *
   * @param filters {@link Filter} list of conditions with fields and values
   * @param sortCriterion {@link SortCriterion} to be applied to the search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return {@link SearchRequest} that contains the filtered query
   */
  @Nonnull
  public SearchRequest getFilterRequest(@Nullable Filter filters, @Nullable SortCriterion sortCriterion, int from,
      int size) {
    SearchRequest searchRequest = new SearchRequest();

    BoolQueryBuilder filterQuery = getFilterQuery(filters);
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQuery);
    searchSourceBuilder.from(from).size(size);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);
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
  public static SearchRequest getAggregationRequest(@Nonnull String field, @Nullable Filter filter, int limit) {
    SearchRequest searchRequest = new SearchRequest();
    BoolQueryBuilder filterQuery = getFilterQuery(filter);

    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(filterQuery);
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(AggregationBuilders.terms(field).field(field + ESUtils.KEYWORD_SUFFIX).size(limit));
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  private QueryBuilder getQuery(@Nonnull String query) {
    return SearchQueryBuilder.buildQuery(_entitySpec, query);
  }

  private List<AggregationBuilder> getAggregations() {
    List<AggregationBuilder> aggregationBuilders = new ArrayList<>();
    for (String facet : _facetFields) {
      // All facet fields must have subField keyword
      AggregationBuilder aggBuilder =
          AggregationBuilders.terms(facet).field(facet + ESUtils.KEYWORD_SUFFIX).size(_maxTermBucketSize);
      aggregationBuilders.add(aggBuilder);
    }
    return aggregationBuilders;
  }

  private HighlightBuilder getHighlights() {
    HighlightBuilder highlightBuilder = new HighlightBuilder();
    // Don't set tags to get the original field value
    highlightBuilder.preTags("");
    highlightBuilder.postTags("");
    // Check for each field name and any subfields
    _defaultQueryFieldNames.forEach(fieldName -> highlightBuilder.field(fieldName).field(fieldName + ".*"));
    return highlightBuilder;
  }

  @WithSpan
  public SearchResult extractResult(@Nonnull SearchResponse searchResponse, int from, int size) {
    int totalCount = (int) searchResponse.getHits().getTotalHits().value;
    List<SearchEntity> resultList = getResults(searchResponse);
    SearchResultMetadata searchResultMetadata = extractSearchResultMetadata(searchResponse);

    return new SearchResult().setEntities(new SearchEntityArray(resultList))
        .setMetadata(searchResultMetadata)
        .setFrom(from)
        .setPageSize(size)
        .setNumEntities(totalCount);
  }

  @Nonnull
  private List<MatchedField> extractMatchedFields(@Nonnull Map<String, HighlightField> highlightedFields) {
    // Keep track of unique field values that matched for a given field name
    Map<String, Set<String>> highlightedFieldNamesAndValues = new HashMap<>();
    for (Map.Entry<String, HighlightField> entry : highlightedFields.entrySet()) {
      // Get the field name from source e.g. name.delimited -> name
      Optional<String> fieldName = getFieldName(entry.getKey());
      if (!fieldName.isPresent()) {
        continue;
      }
      if (!highlightedFieldNamesAndValues.containsKey(fieldName.get())) {
        highlightedFieldNamesAndValues.put(fieldName.get(), new HashSet<>());
      }
      for (Text fieldValue : entry.getValue().getFragments()) {
        highlightedFieldNamesAndValues.get(fieldName.get()).add(fieldValue.string());
      }
    }
    return highlightedFieldNamesAndValues.entrySet()
        .stream()
        .flatMap(
            entry -> entry.getValue().stream().map(value -> new MatchedField().setName(entry.getKey()).setValue(value)))
        .collect(Collectors.toList());
  }

  @Nonnull
  private Optional<String> getFieldName(String matchedField) {
    return _defaultQueryFieldNames.stream().filter(matchedField::startsWith).findFirst();
  }

  private Map<String, Double> extractFeatures(@Nonnull SearchHit searchHit) {
    return ImmutableMap.of(Features.Name.SEARCH_BACKEND_SCORE.toString(), (double) searchHit.getScore());
  }

  private SearchEntity getResult(@Nonnull SearchHit hit) {
    return new SearchEntity().setEntity(getUrnFromSearchHit(hit))
        .setMatchedFields(new MatchedFieldArray(extractMatchedFields(hit.getHighlightFields())))
        .setScore(hit.getScore())
        .setFeatures(new DoubleMap(extractFeatures(hit)));
  }

  /**
   * Gets list of entities returned in the search response
   *
   * @param searchResponse the raw search response from search engine
   * @return List of search entities
   */
  @Nonnull
  private List<SearchEntity> getResults(@Nonnull SearchResponse searchResponse) {
    return Arrays.stream(searchResponse.getHits().getHits()).map(this::getResult).collect(Collectors.toList());
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
   * @return {@link SearchResultMetadata} with aggregation and list of urns obtained from {@link SearchResponse}
   */
  @Nonnull
  private SearchResultMetadata extractSearchResultMetadata(@Nonnull SearchResponse searchResponse) {
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray());

    final List<AggregationMetadata> aggregationMetadataList = extractAggregationMetadata(searchResponse);
    if (!aggregationMetadataList.isEmpty()) {
      searchResultMetadata.setAggregations(new AggregationMetadataArray(aggregationMetadataList));
    }

    return searchResultMetadata;
  }

  private List<AggregationMetadata> extractAggregationMetadata(@Nonnull SearchResponse searchResponse) {
    final List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();

    if (searchResponse.getAggregations() == null) {
      return aggregationMetadataList;
    }

    for (Map.Entry<String, Aggregation> entry : searchResponse.getAggregations().getAsMap().entrySet()) {
      final Map<String, Long> oneTermAggResult = extractTermAggregations((ParsedTerms) entry.getValue());
      if (oneTermAggResult.isEmpty()) {
        continue;
      }
      final AggregationMetadata aggregationMetadata = new AggregationMetadata().setName(entry.getKey())
          .setDisplayName(_filtersToDisplayName.get(entry.getKey()))
          .setAggregations(new LongMap(oneTermAggResult))
          .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(oneTermAggResult)));
      aggregationMetadataList.add(aggregationMetadata);
    }

    return aggregationMetadataList;
  }

  @WithSpan
  public static Map<String, Long> extractTermAggregations(@Nonnull SearchResponse searchResponse,
      @Nonnull String aggregationName) {
    if (searchResponse.getAggregations() == null) {
      return Collections.emptyMap();
    }

    Aggregation aggregation = searchResponse.getAggregations().get(aggregationName);
    if (aggregation == null) {
      return Collections.emptyMap();
    }
    return extractTermAggregations((ParsedTerms) aggregation);
  }

  /**
   * Extracts term aggregations give a parsed term.
   *
   * @param terms an abstract parse term, input can be either ParsedStringTerms ParsedLongTerms
   * @return a map with aggregation key and corresponding doc counts
   */
  @Nonnull
  private static Map<String, Long> extractTermAggregations(@Nonnull ParsedTerms terms) {

    final Map<String, Long> aggResult = new HashMap<>();
    List<? extends Terms.Bucket> bucketList = terms.getBuckets();

    for (Terms.Bucket bucket : bucketList) {
      String key = bucket.getKeyAsString();
      // Gets filtered sub aggregation doc count if exist
      long docCount = bucket.getDocCount();
      if (docCount > 0) {
        aggResult.put(key, docCount);
      }
    }

    return aggResult;
  }
}
