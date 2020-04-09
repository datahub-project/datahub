package com.linkedin.metadata.dao.search;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.SearchResult;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.query.AggregationMetadata;
import com.linkedin.metadata.query.AggregationMetadataArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static com.linkedin.metadata.dao.utils.SearchUtils.*;


/**
 * A search DAO for Elasticsearch backend.
 */
@Slf4j
public class ESSearchDAO<DOCUMENT extends RecordTemplate> extends BaseSearchDAO<DOCUMENT> {

  private static final Integer DEFAULT_TERM_BUCKETS_SIZE_100 = 100;
  private static final String URN_FIELD = "urn";

  private RestHighLevelClient _client;
  private BaseSearchConfig _config;
  private BaseESAutoCompleteQuery _autoCompleteQueryForLowCardFields;
  private BaseESAutoCompleteQuery _autoCompleteQueryForHighCardFields;

  // TODO: Currently takes elastic search client, in future, can take other clients such as galene
  // TODO: take params and settings needed to create the client
  public ESSearchDAO(@Nonnull RestHighLevelClient esClient, @Nonnull Class<DOCUMENT> documentClass,
      @Nonnull BaseSearchConfig config) {
    super(documentClass);
    _client = esClient;
    _config = config;
    _autoCompleteQueryForLowCardFields = new ESAutoCompleteQueryForLowCardinalityFields(_config);
    _autoCompleteQueryForHighCardFields = new ESAutoCompleteQueryForHighCardinalityFields(_config);
  }

  @Nonnull
  protected BaseESAutoCompleteQuery getAutocompleteQueryGenerator(@Nonnull String field) {
    if (_config.getLowCardinalityFields() != null && _config.getLowCardinalityFields().contains(field)) {
      return _autoCompleteQueryForLowCardFields;
    }
    return _autoCompleteQueryForHighCardFields;
  }

  /**
   * Constructs the base query string given input
   *
   * @param input the search input text
   * @return built query
   */
  @Nonnull
  QueryBuilder buildQueryString(@Nonnull String input) {
    final String query = _config.getSearchQueryTemplate().replace("$INPUT", input);
    return QueryBuilders.wrapperQuery(query);
  }

  @Nonnull
  private SearchResult<DOCUMENT> executeAndExtract(@Nonnull SearchRequest searchRequest, int from, int size) {
    try {
      final SearchResponse searchResponse = _client.search(searchRequest);
      // extract results, validated against document model as well
      return extractQueryResult(searchResponse, from, size);
    } catch (Exception e) {
      log.error("Search query failed:" + e.getMessage());
      throw new ESQueryException("Search query failed:", e);
    }
  }

  /**
   * TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   */
  @Override
  @Nonnull
  public SearchResult<DOCUMENT> search(@Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size) {

    // Step 0: TODO: Add type casting if needed and  add request params validation against the model
    // Step 1: construct the query
    final SearchRequest req = constructSearchQuery(input, postFilters, sortCriterion, from, size);
    // Step 2: execute the query and extract results, validated against document model as well
    return executeAndExtract(req, from, size);
  }

  @Override
  @Nonnull
  public SearchResult<DOCUMENT> filter(@Nullable Filter filters, @Nullable SortCriterion sortCriterion,
      int from, int size) {

    final SearchRequest searchRequest = getFilteredSearchQuery(filters, sortCriterion, from, size);
    return executeAndExtract(searchRequest, from, size);
  }

  /**
   * Returns a {@link SearchRequest} given filters to be applied to search query and sort criterion to be applied to search results
   *
   * @param filters {@link Filter} list of conditions with fields and values
   * @param sortCriterion {@link SortCriterion} to be applied to the search results
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return {@link SearchRequest} that contains the filtered query
   */
  @Nonnull
  SearchRequest getFilteredSearchQuery(@Nullable Filter filters, @Nullable SortCriterion sortCriterion,
      int from, int size) {

    final BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
    if (filters != null) {
      filters.getCriteria().forEach(criterion -> {
            if (!criterion.getValue().trim().isEmpty()) {
              boolQueryBuilder.filter(getQueryBuilderFromCriterion(criterion));
            }
          });
    }
    final SearchRequest searchRequest = new SearchRequest(_config.getIndexName());
    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(boolQueryBuilder);
    searchSourceBuilder.from(from).size(size);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  /**
   * Constructs the search query based on the query request
   *
   * @param input the search input text
   * @param requestParams the request map with fields and values
   * @param from index to start the search from
   * @param size the number of search hits to return
   * @return a valid search request
   * TODO: This part will be replaced by searchTemplateAPI when the elastic is upgraded to 6.4 or later
   */
  @Nonnull
  public SearchRequest constructSearchQuery(@Nonnull String input, @Nullable Filter requestParams,
      @Nullable SortCriterion sortCriterion, int from, int size) {

    SearchRequest searchRequest = new SearchRequest(_config.getIndexName());
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    Map<String, String> requestMap = SearchUtils.getRequestMap(requestParams);

    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);

    searchSourceBuilder.query(buildQueryString(input));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(requestMap));
    buildAggregations(searchSourceBuilder, requestMap);
    ESUtils.buildSortOrder(searchSourceBuilder, sortCriterion);

    searchRequest.source(searchSourceBuilder);
    log.debug("Search request is: " + searchRequest.toString());
    return searchRequest;
  }

  /**
   * Constructs the aggregations and sub-aggregations by adding other facets' filters if they are set in request
   *
   * Retrieves dynamic aggregation bucket values when the selections change on the fly
   *
   * @param searchSourceBuilder the builder to build search source for search request
   * @param requestMap the search request map with fields and its values
   */
  private void buildAggregations(@Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nonnull Map<String, String> requestMap) {
    Set<String> facetFields = _config.getFacetFields();
    for (String facet : facetFields) {
      AggregationBuilder aggBuilder = AggregationBuilders.terms(facet).field(facet).size(DEFAULT_TERM_BUCKETS_SIZE_100);

      for (Map.Entry<String, String> entry : requestMap.entrySet()) {
        if (!facetFields.contains(entry.getKey()) || entry.getKey().equals(facet) || entry.getValue() == null) {
          continue;
        }
        BoolQueryBuilder oFilters = new BoolQueryBuilder();
        Arrays.stream(entry.getValue().split(","))
            .forEach(elem -> oFilters.should(QueryBuilders.matchQuery(entry.getKey(), elem)));

        aggBuilder.subAggregation(AggregationBuilders.filter(entry.getKey(), oFilters));
      }
      searchSourceBuilder.aggregation(aggBuilder);
    }
  }

  /**
   * Extracts a list of documents from the raw search response
   *
   * @param searchResponse the raw search response from search engine
   * @param from offset from the first result you want to fetch
   * @param size page size
   * @return collection of a list of documents and related search result metadata
   */
  @Nonnull
  public SearchResult<DOCUMENT> extractQueryResult(@Nonnull SearchResponse searchResponse, int from, int size) {

    int totalCount = (int) searchResponse.getHits().getTotalHits();
    int totalPageCount = QueryUtils.getTotalPageCount(totalCount, size);

    return SearchResult.<DOCUMENT>builder()
        // format
        .documentList(getDocuments(searchResponse))
        .searchResultMetadata(extractSearchResultMetadata(searchResponse))
        .from(from)
        .pageSize(size)
        .havingMore(QueryUtils.isHavingMore(from, size, totalPageCount))
        .totalCount(totalCount)
        .totalPageCount(totalPageCount)
        .build();
  }

  /**
   * Gets list of documents from search hits
   *
   * @param searchResponse the raw search response from search engine
   * @return List of documents
   */
  @Nonnull
  List<DOCUMENT> getDocuments(@Nonnull SearchResponse searchResponse) {
    return (Arrays.stream(searchResponse.getHits().getHits())).map(hit ->
      newDocument(buildDocumentsDataMap(hit.getSourceAsMap()))).collect(Collectors.toList());
  }

  /**
   * Builds data map for documents
   *
   * @param objectMap an object map represents one raw search hit
   * @return a data map
   */
  @Nonnull
  DataMap buildDocumentsDataMap(@Nonnull Map<String, Object> objectMap) {

    final DataMap dataMap = new DataMap();
    for (Map.Entry<String, Object> entry : objectMap.entrySet()) {
      if (entry.getValue() instanceof ArrayList) {
        dataMap.put(entry.getKey(), new DataList((ArrayList<String>) entry.getValue()));
      } else if (entry.getValue() != null) {
        dataMap.put(entry.getKey(), entry.getValue());
      }
    }
    return dataMap;
  }

  @Override
  @Nonnull
  public AutoCompleteResult autoComplete(@Nonnull String query, @Nullable String field, @Nullable Filter requestParams,
      int limit) {
    if (field == null) {
      field = _config.getDefaultAutocompleteField();
    }
    try {
      SearchRequest req = constructAutoCompleteQuery(query, field, requestParams);
      SearchResponse searchResponse = _client.search(req);
      return extractAutoCompleteResult(searchResponse, query, field, limit);
    } catch (Exception e) {
      log.error("Auto complete query failed:" + e.getMessage());
      throw new ESQueryException("Auto complete query failed:", e);
    }
  }

  @Nonnull
  public AutoCompleteResult extractAutoCompleteResult(@Nonnull SearchResponse searchResponse, @Nonnull String input,
      @Nonnull String field, int limit) {
    return getAutocompleteQueryGenerator(field).extractAutoCompleteResult(searchResponse, input, field, limit);
  }

  @Nonnull
  public SearchRequest constructAutoCompleteQuery(@Nonnull String input, @Nonnull String field,
      @Nullable Filter requestParams) {
    return getAutocompleteQueryGenerator(field).constructAutoCompleteQuery(input, field, requestParams);
  }

  /**
   * Extracts SearchResultMetadata section
   *
   * @param searchResponse the raw {@link SearchResponse} as obtained from the search engine
   * @return {@link SearchResultMetadata} with aggregation and list of urns obtained from {@link SearchResponse}
   */
  @Nonnull
  SearchResultMetadata extractSearchResultMetadata(@Nonnull SearchResponse searchResponse) {
    final SearchResultMetadata searchResultMetadata =
        new SearchResultMetadata().setSearchResultMetadatas(new AggregationMetadataArray()).setUrns(new UrnArray());

    try {
      // populate the urns from search response
      if (searchResponse.getHits() != null && searchResponse.getHits().getHits() != null) {
        searchResultMetadata.setUrns(Arrays.stream(searchResponse.getHits().getHits())
            .map(this::getUrnFromSearchHit)
            .collect(Collectors.toCollection(UrnArray::new)));
      }
    } catch (NullPointerException e) {
      throw new RuntimeException("Missing urn field in search document " + e);
    }

    final Aggregations aggregations = searchResponse.getAggregations();
    if (aggregations == null) {
      return searchResultMetadata;
    }

    final AggregationMetadataArray aggregationMetadataArray = new AggregationMetadataArray();

    for (Map.Entry<String, Aggregation> entry : aggregations.getAsMap().entrySet()) {
      final Map<String, Long> oneTermAggResult = extractTermAggregations((ParsedTerms) entry.getValue());
      final AggregationMetadata aggregationMetadata =
          new AggregationMetadata().setName(entry.getKey()).setAggregations(new LongMap(oneTermAggResult));
      aggregationMetadataArray.add(aggregationMetadata);
    }

    return searchResultMetadata.setSearchResultMetadatas(aggregationMetadataArray);
  }

  /**
   * Extracts term aggregations give a parsed term
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
   * Extracts sub aggregations from one term bucket
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
  private Urn getUrnFromSearchHit(@Nonnull SearchHit hit) {
    try {
      return Urn.createFromString(hit.getSourceAsMap().get(URN_FIELD).toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid urn in search document " + e);
    }
  }

}
