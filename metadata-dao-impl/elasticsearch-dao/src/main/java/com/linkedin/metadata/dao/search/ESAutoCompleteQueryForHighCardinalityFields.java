package com.linkedin.metadata.dao.search;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.dao.utils.SearchUtils;
import com.linkedin.metadata.query.Filter;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

@Slf4j
public class ESAutoCompleteQueryForHighCardinalityFields extends BaseESAutoCompleteQuery {
  private static final Integer DEFAULT_AUTOCOMPLETE_QUERY_SIZE =  100;
  private BaseSearchConfig _config;

  ESAutoCompleteQueryForHighCardinalityFields(BaseSearchConfig config) {
    this._config = config;
  }

  @Nonnull
  SearchRequest constructAutoCompleteQuery(@Nonnull String input, @Nonnull String field,
      @Nullable Filter requestParams) {

    SearchRequest searchRequest = new SearchRequest(_config.getIndexName());
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    Map<String, String> requestMap = SearchUtils.getRequestMap(requestParams);

    searchSourceBuilder.size(DEFAULT_AUTOCOMPLETE_QUERY_SIZE);
    searchSourceBuilder.query(buildAutoCompleteQueryString(input, field));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(requestMap));
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
  QueryBuilder buildAutoCompleteQueryString(@Nonnull String input, @Nonnull String field) {
    String query = _config.getAutocompleteQueryTemplate();
    query = query.replace("$INPUT", input).replace("$FIELD", field);
    return QueryBuilders.wrapperQuery(query);
  }


  @Nonnull
  StringArray getSuggestionList(@Nonnull SearchResponse searchResponse, @Nonnull String field,
      @Nonnull String input, int limit) {
    Set<String> autoCompletionList = new LinkedHashSet<>();
    SearchHit[] hits = searchResponse.getHits().getHits();
    Integer count = 0;
    for (SearchHit hit : hits) {
      Map<String, Object> source = hit.getSource();
      if (count >= limit) {
        break;
      }
      if (source.containsKey(field)) {
        autoCompletionList.addAll(decoupleArrayToGetSubstringMatch(source.get(field), input));
        count = autoCompletionList.size();
      }
    }
    return new StringArray(autoCompletionList);
  }

  /**
   * Obtains relevant string from an object of which the input string is a prefix of.
   *
   * <p> This is useful for autocomplete queries where the field is indexed as an array of strings but the returned
   * value should be a string from this array that completes the input string </p>
   *
   * <p> If the field is instead stored as a string, the function returns field's value as it is </p>
   *
   * <p> If the field contains multiple elements, but none of them completes the input string, field's value is
   * returned as it is</p>
   *
   * @param fieldVal value of the field stored in ES. This could be a string, list of strings, etc
   * @param input the string that needs to be completed
   * @return String obtained from fieldVal that completes the input string
   */
  @Nonnull
  static List<String> decoupleArrayToGetSubstringMatch(@Nonnull Object fieldVal, @Nonnull String input) {
    if (!(fieldVal instanceof List)) {
      return Collections.singletonList(fieldVal.toString());
    }
    List<Object> stringVals = (List<Object>) fieldVal;
    return stringVals.stream()
        .map(Object::toString)
        .filter(x -> x.toLowerCase().contains(input.toLowerCase()))
        .collect(Collectors.toList());
  }
}
