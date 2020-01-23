package com.linkedin.metadata.dao.search;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.query.Filter;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

@Slf4j
public class ESAutoCompleteQueryForLowCardinalityFields extends BaseESAutoCompleteQuery {

  private static final String DEFAULT_QUERY_ANALYZER = "lowercase_keyword";
  private BaseSearchConfig _config;

  ESAutoCompleteQueryForLowCardinalityFields(BaseSearchConfig config) {
    this._config = config;
  }

  @Nonnull
  SearchRequest constructAutoCompleteQuery(@Nonnull String input, @Nonnull String field,
      @Nullable Filter requestParams) {

    SearchRequest searchRequest = new SearchRequest(_config.getIndexName());
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    Map<String, String> requestMap = SearchUtils.getRequestMap(requestParams);

    searchSourceBuilder.query(buildAutoCompleteQueryString(input, field, requestMap));
    searchSourceBuilder.aggregation(AggregationBuilders.terms(field).field(field));
    searchRequest.source(searchSourceBuilder);
    log.debug("Auto complete request is: " + searchRequest.toString());
    return searchRequest;
  }

  /**
   * Constructs auto complete query given request
   *
   * @param input the type ahead query text
   * @param field the field name for the auto complete
   * @return built autocomplete query
   */
  @Nonnull
  QueryBuilder buildAutoCompleteQueryString(@Nonnull String input, @Nonnull String field,
      @Nonnull Map<String, String> requestMap) {
    String subFieldDelimitEdgeNgram = field + ".delimited_edgengram";
    String subFieldEdgeNgram = field + ".edgengram";
    BoolQueryBuilder query = ESUtils.buildFilterQuery(requestMap);
    if (input.length() > 0) {
      query.must(QueryBuilders
          .queryStringQuery(input)
          .fields(ImmutableMap.of(field, 1f, subFieldDelimitEdgeNgram, 1f, subFieldEdgeNgram, 1f))
          .analyzer(DEFAULT_QUERY_ANALYZER));
    }
    return query;
  }

  @Nonnull
  StringArray getSuggestionList(@Nonnull SearchResponse searchResponse, @Nonnull String field,
      @Nonnull String input, int limit) {
    Set<String> autoCompletionList = new LinkedHashSet<>();
    Aggregation aggregation = searchResponse.getAggregations().get(field);
    ((ParsedTerms) aggregation)
        .getBuckets()
        .stream()
        .forEach(b -> autoCompletionList.add(b.getKeyAsString()));
    return new StringArray(autoCompletionList);
  }
}
