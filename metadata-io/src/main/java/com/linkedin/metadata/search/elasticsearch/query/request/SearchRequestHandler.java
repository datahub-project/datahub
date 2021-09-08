package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.MatchMetadata;
import com.linkedin.metadata.query.MatchMetadataArray;
import com.linkedin.metadata.query.MatchedField;
import com.linkedin.metadata.query.MatchedFieldArray;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import static com.linkedin.metadata.dao.utils.SearchUtils.getQueryBuilderFromCriterion;


@Slf4j
public class SearchRequestHandler {

  private static final Map<EntitySpec, SearchRequestHandler> REQUEST_HANDLER_BY_ENTITY_NAME = new ConcurrentHashMap<>();

  private final EntitySpec _entitySpec;
  private final Set<String> _facetFields;
  private final Set<String> _defaultQueryFieldNames;
  private final int _maxTermBucketSize = 100;

  private SearchRequestHandler(@Nonnull EntitySpec entitySpec) {
    _entitySpec = entitySpec;
    _facetFields = getFacetFields();
    _defaultQueryFieldNames = getDefaultQueryFieldNames();
  }

  public static SearchRequestHandler getBuilder(@Nonnull EntitySpec entitySpec) {
    return REQUEST_HANDLER_BY_ENTITY_NAME.computeIfAbsent(entitySpec, k -> new SearchRequestHandler(entitySpec));
  }

  public Set<String> getFacetFields() {
    return _entitySpec.getSearchableFieldSpecs()
        .stream()
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .filter(SearchableAnnotation::isAddToFilters)
        .map(SearchableAnnotation::getFieldName)
        .collect(Collectors.toSet());
  }

  public Set<String> getDefaultQueryFieldNames() {
    return _entitySpec.getSearchableFieldSpecs()
        .stream()
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .filter(SearchableAnnotation::isQueryByDefault)
        .map(SearchableAnnotation::getFieldName)
        .collect(Collectors.toSet());
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

    searchSourceBuilder.query(getQuery(input));

    BoolQueryBuilder filterQuery = ESUtils.buildFilterQuery(filter);
    // Filter out entities that are marked "removed"
    filterQuery.mustNot(QueryBuilders.matchQuery("removed", true));
    searchSourceBuilder.query(QueryBuilders.boolQuery().must(getQuery(input)).must(filterQuery));
    getAggregations(filter).forEach(searchSourceBuilder::aggregation);
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

    final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
    if (filters != null) {
      filters.getCriteria().forEach(criterion -> {
        if (!criterion.getValue().trim().isEmpty()) {
          boolQueryBuilder.filter(getQueryBuilderFromCriterion(criterion));
        }
      });
    }
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(boolQueryBuilder);
    searchSourceBuilder.from(from).size(size);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  public QueryBuilder getQuery(@Nonnull String query) {
    return SearchQueryBuilder.buildQuery(_entitySpec, query);
  }

  public List<AggregationBuilder> getAggregations(@Nullable Filter filter) {
    List<AggregationBuilder> aggregationBuilders = new ArrayList<>();
    for (String facet : _facetFields) {
      // All facet fields must have subField keyword
      AggregationBuilder aggBuilder =
          AggregationBuilders.terms(facet).field(facet + ".keyword").size(_maxTermBucketSize);
      Optional.ofNullable(filter).map(Filter::getCriteria).ifPresent(criteria -> {
        for (Criterion criterion : criteria) {
          if (!_facetFields.contains(criterion.getField()) || criterion.getField().equals(facet)) {
            continue;
          }
          QueryBuilder filterQueryBuilder = ESUtils.getQueryBuilderFromCriterionForSearch(criterion);
          aggBuilder.subAggregation(AggregationBuilders.filter(criterion.getField(), filterQueryBuilder));
        }
      });
      aggregationBuilders.add(aggBuilder);
    }
    return aggregationBuilders;
  }

  public HighlightBuilder getHighlights() {
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
    List<Urn> resultList = getResults(searchResponse);
    SearchResultMetadata searchResultMetadata = extractSearchResultMetadata(searchResponse);
    searchResultMetadata.setUrns(new UrnArray(resultList));

    return new SearchResult().setEntities(new UrnArray(resultList))
        .setMetadata(searchResultMetadata)
        .setFrom(from)
        .setPageSize(size)
        .setNumEntities(totalCount);
  }

  /**
   * Gets list of urns returned in the search response
   *
   * @param searchResponse the raw search response from search engine
   * @return List of entity urns
   */
  @Nonnull
  List<Urn> getResults(@Nonnull SearchResponse searchResponse) {
    return Arrays.stream(searchResponse.getHits().getHits())
        .map(this::getUrnFromSearchHit)
        .collect(Collectors.toList());
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
  SearchResultMetadata extractSearchResultMetadata(@Nonnull SearchResponse searchResponse) {
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray()).setUrns(new UrnArray());

    final List<AggregationMetadata> aggregationMetadataList = extractAggregation(searchResponse);
    if (!aggregationMetadataList.isEmpty()) {
      searchResultMetadata.setSearchResultMetadatas(new AggregationMetadataArray(aggregationMetadataList));
    }

    final List<MatchMetadata> highlightMetadataList = extractMatchMetadataList(searchResponse);
    if (!highlightMetadataList.isEmpty()) {
      searchResultMetadata.setMatches(new MatchMetadataArray(highlightMetadataList));
    }

    return searchResultMetadata;
  }

  public List<AggregationMetadata> extractAggregation(@Nonnull SearchResponse searchResponse) {
    final List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();

    if (searchResponse.getAggregations() == null) {
      return aggregationMetadataList;
    }

    for (Map.Entry<String, Aggregation> entry : searchResponse.getAggregations().getAsMap().entrySet()) {
      final Map<String, Long> oneTermAggResult = extractTermAggregations((ParsedTerms) entry.getValue());
      if (oneTermAggResult.isEmpty()) {
        continue;
      }
      final AggregationMetadata aggregationMetadata =
          new AggregationMetadata().setName(entry.getKey()).setAggregations(new LongMap(oneTermAggResult));
      aggregationMetadataList.add(aggregationMetadata);
    }

    return aggregationMetadataList;
  }

  /**
   * Extracts term aggregations give a parsed term.
   *
   * @param terms an abstract parse term, input can be either ParsedStringTerms ParsedLongTerms
   * @return a map with aggregation key and corresponding doc counts
   */
  @Nonnull
  private Map<String, Long> extractTermAggregations(@Nonnull ParsedTerms terms) {

    final Map<String, Long> aggResult = new HashMap<>();
    List<? extends Terms.Bucket> bucketList = terms.getBuckets();

    for (Terms.Bucket bucket : bucketList) {
      String key = bucket.getKeyAsString();
      ParsedFilter parsedFilter = extractBucketAggregations(bucket);
      // Gets filtered sub aggregation doc count if exist
      Long docCount = parsedFilter != null ? parsedFilter.getDocCount() : bucket.getDocCount();
      if (docCount > 0) {
        aggResult.put(key, docCount);
      }
    }

    return aggResult;
  }

  /**
   * Extracts sub aggregations from one term bucket.
   *
   * @param bucket a term bucket
   * @return a parsed filter if exist
   */
  @Nullable
  private ParsedFilter extractBucketAggregations(@Nonnull Terms.Bucket bucket) {

    ParsedFilter parsedFilter = null;
    Map<String, Aggregation> bucketAggregations = bucket.getAggregations().getAsMap();
    for (Map.Entry<String, Aggregation> entry : bucketAggregations.entrySet()) {
      parsedFilter = (ParsedFilter) entry.getValue();
      // TODO: implement and test multi parsed filters
    }

    return parsedFilter;
  }

  @Nonnull
  private List<MatchMetadata> extractMatchMetadataList(@Nonnull SearchResponse searchResponse) {
    final List<MatchMetadata> highlightMetadataList = new ArrayList<>(searchResponse.getHits().getHits().length);
    for (SearchHit hit : searchResponse.getHits().getHits()) {
      highlightMetadataList.add(extractMatchMetadata(hit.getHighlightFields()));
    }
    return highlightMetadataList;
  }

  @Nonnull
  private MatchMetadata extractMatchMetadata(@Nonnull Map<String, HighlightField> highlightedFields) {
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
    return new MatchMetadata().setMatchedFields(new MatchedFieldArray(highlightedFieldNamesAndValues.entrySet()
        .stream()
        .flatMap(
            entry -> entry.getValue().stream().map(value -> new MatchedField().setName(entry.getKey()).setValue(value)))
        .collect(Collectors.toList())));
  }

  @Nonnull
  private Optional<String> getFieldName(String matchedField) {
    return _defaultQueryFieldNames.stream().filter(matchedField::startsWith).findFirst();
  }
}
