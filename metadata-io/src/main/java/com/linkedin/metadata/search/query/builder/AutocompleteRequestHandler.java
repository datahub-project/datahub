package com.linkedin.metadata.search.query.builder;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.dao.utils.ESUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;


@Slf4j
public class AutocompleteRequestHandler {

  private final List<String> _defaultAutocompleteFields;

  private static final String ANALYZER = "word_delimited";

  private static final Map<EntitySpec, AutocompleteRequestHandler> AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME =
      new ConcurrentHashMap<>();

  public AutocompleteRequestHandler(@Nonnull EntitySpec entitySpec) {
    _defaultAutocompleteFields = entitySpec.getSearchableFieldSpecs()
        .stream()
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .filter(SearchableAnnotation::isEnableAutocomplete)
        .map(SearchableAnnotation::getFieldName)
        .collect(Collectors.toList());
  }

  public static AutocompleteRequestHandler getBuilder(@Nonnull EntitySpec entitySpec) {
    return AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME.computeIfAbsent(entitySpec,
        k -> new AutocompleteRequestHandler(entitySpec));
  }

  public SearchRequest getSearchRequest(@Nonnull EntitySpec entitySpec, @Nonnull String input, @Nullable String field,
      @Nullable Filter filter, int limit) {
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    AutocompleteRequestHandler builder = AutocompleteRequestHandler.getBuilder(entitySpec);
    searchSourceBuilder.size(limit);
    searchSourceBuilder.query(builder.getQuery(input, field));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(filter));
    searchSourceBuilder.highlighter(builder.getHighlights(field));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  private QueryBuilder getQuery(@Nonnull String query, @Nullable String field) {
    // Search for exact matches with higher boost and ngram matches
    List<String> fieldNames = getAutocompleteFields(field).stream()
        .flatMap(fieldName -> Stream.of(fieldName, fieldName + ".ngram"))
        .collect(Collectors.toList());
    MultiMatchQueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(query, fieldNames.toArray(new String[0]));
    queryBuilder.analyzer(ANALYZER);
    return queryBuilder;
  }

  // Get HighllightBuilder to highlight the matched field
  private HighlightBuilder getHighlights(@Nullable String field) {
    HighlightBuilder highlightBuilder = new HighlightBuilder();
    // Don't set tags to get the original field value
    highlightBuilder.preTags("");
    highlightBuilder.postTags("");
    // Check for each field name and any subfields
    getAutocompleteFields(field).forEach(fieldName -> highlightBuilder.field(fieldName).field(fieldName + ".*"));
    return highlightBuilder;
  }

  private List<String> getAutocompleteFields(@Nullable String field) {
    if (field != null) {
      return ImmutableList.of(field);
    }
    return _defaultAutocompleteFields;
  }

  public AutoCompleteResult extractResult(@Nonnull SearchResponse searchResponse, @Nonnull String input) {
    List<String> results = new ArrayList<>();
    for (SearchHit hit : searchResponse.getHits()) {
      Optional<String> matchedFieldValue = hit.getHighlightFields()
          .entrySet()
          .stream()
          .findFirst()
          .map(entry -> entry.getValue().getFragments()[0].string());
      if (matchedFieldValue.isPresent()) {
        results.add(matchedFieldValue.get());
      } else {
        log.info("No highlighted field for query {}, hit {}", input, hit);
      }
    }
    return new AutoCompleteResult().setQuery(input).setSuggestions(new StringArray(results));
  }
}
