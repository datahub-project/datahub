package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.utils.ESUtils;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;


@Slf4j
public class AutocompleteRequestHandler {

  private static final String ANALYZER = "word_delimited";
  private final List<String> _defaultAutocompleteFields;

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

  public SearchRequest getSearchRequest(@Nonnull String input, @Nullable String field, @Nullable Filter filter,
      int limit) {
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(limit);
    searchSourceBuilder.query(getQuery(input, field));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(filter));
    searchSourceBuilder.highlighter(getHighlights(field));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  private QueryBuilder getQuery(@Nonnull String query, @Nullable String field) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    // Search for exact matches with higher boost and ngram matches
    QueryStringQueryBuilder autocompleteQueryBuilder = QueryBuilders.queryStringQuery(query);
    autocompleteQueryBuilder.analyzer(ANALYZER);
    autocompleteQueryBuilder.defaultOperator(Operator.AND);
    getAutocompleteFields(field).forEach(fieldName -> {
      autocompleteQueryBuilder.field(fieldName, 4);
      autocompleteQueryBuilder.field(fieldName + ".ngram");
    });
    finalQuery.must(autocompleteQueryBuilder);

    finalQuery.mustNot(QueryBuilders.matchQuery("removed", true));
    return finalQuery;
  }

  // Get HighlightBuilder to highlight the matched field
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
    if (field != null && !field.isEmpty()) {
      return ImmutableList.of(field);
    }
    return _defaultAutocompleteFields;
  }

  public AutoCompleteResult extractResult(@Nonnull SearchResponse searchResponse, @Nonnull String input) {
    Set<String> results = new LinkedHashSet<>();
    Set<AutoCompleteEntity> entityResults = new HashSet<>();
    for (SearchHit hit : searchResponse.getHits()) {
      Optional<String> matchedFieldValue = hit.getHighlightFields()
          .entrySet()
          .stream()
          .findFirst()
          .map(entry -> entry.getValue().getFragments()[0].string());
      Optional<String> matchedUrn = Optional.ofNullable((String) hit.getSourceAsMap().get("urn"));
      try {
        if (matchedUrn.isPresent()) {
          entityResults.add(new AutoCompleteEntity().setUrn(Urn.createFromString(matchedUrn.get())));
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to create urn %s", matchedUrn.get()), e);
      }
      matchedFieldValue.ifPresent(results::add);
    }
    return new AutoCompleteResult()
        .setQuery(input)
        .setSuggestions(new StringArray(results))
        .setEntities(new AutoCompleteEntityArray(entityResults));
  }
}
