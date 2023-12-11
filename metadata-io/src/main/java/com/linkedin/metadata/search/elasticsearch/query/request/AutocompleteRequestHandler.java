package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;

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
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

@Slf4j
public class AutocompleteRequestHandler {

  private final List<String> _defaultAutocompleteFields;

  private static final Map<EntitySpec, AutocompleteRequestHandler>
      AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME = new ConcurrentHashMap<>();

  public AutocompleteRequestHandler(@Nonnull EntitySpec entitySpec) {
    _defaultAutocompleteFields =
        Stream.concat(
                entitySpec.getSearchableFieldSpecs().stream()
                    .map(SearchableFieldSpec::getSearchableAnnotation)
                    .filter(SearchableAnnotation::isEnableAutocomplete)
                    .map(SearchableAnnotation::getFieldName),
                Stream.of("urn"))
            .collect(Collectors.toList());
  }

  public static AutocompleteRequestHandler getBuilder(@Nonnull EntitySpec entitySpec) {
    return AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME.computeIfAbsent(
        entitySpec, k -> new AutocompleteRequestHandler(entitySpec));
  }

  public SearchRequest getSearchRequest(
      @Nonnull String input, @Nullable String field, @Nullable Filter filter, int limit) {
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(limit);
    searchSourceBuilder.query(getQuery(input, field));
    searchSourceBuilder.postFilter(ESUtils.buildFilterQuery(filter, false));
    searchSourceBuilder.highlighter(getHighlights(field));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  private QueryBuilder getQuery(@Nonnull String query, @Nullable String field) {
    return getQuery(getAutocompleteFields(field), query);
  }

  public static QueryBuilder getQuery(List<String> autocompleteFields, @Nonnull String query) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    // Search for exact matches with higher boost and ngram matches
    MultiMatchQueryBuilder autocompleteQueryBuilder =
        QueryBuilders.multiMatchQuery(query).type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    final float urnBoost =
        Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"));
    autocompleteFields.forEach(
        fieldName -> {
          if ("urn".equals(fieldName)) {
            autocompleteQueryBuilder.field(fieldName + ".ngram", urnBoost);
            autocompleteQueryBuilder.field(fieldName + ".ngram._2gram", urnBoost);
            autocompleteQueryBuilder.field(fieldName + ".ngram._3gram", urnBoost);
            autocompleteQueryBuilder.field(fieldName + ".ngram._4gram", urnBoost);
          } else {
            autocompleteQueryBuilder.field(fieldName + ".ngram");
            autocompleteQueryBuilder.field(fieldName + ".ngram._2gram");
            autocompleteQueryBuilder.field(fieldName + ".ngram._3gram");
            autocompleteQueryBuilder.field(fieldName + ".ngram._4gram");
          }

          finalQuery.should(QueryBuilders.matchPhrasePrefixQuery(fieldName + ".delimited", query));
        });

    finalQuery.should(autocompleteQueryBuilder);

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
    getAutocompleteFields(field)
        .forEach(
            fieldName ->
                highlightBuilder
                    .field(fieldName)
                    .field(fieldName + ".*")
                    .field(fieldName + ".ngram")
                    .field(fieldName + ".delimited"));
    return highlightBuilder;
  }

  private List<String> getAutocompleteFields(@Nullable String field) {
    if (field != null && !field.isEmpty()) {
      return ImmutableList.of(field);
    }
    return _defaultAutocompleteFields;
  }

  public AutoCompleteResult extractResult(
      @Nonnull SearchResponse searchResponse, @Nonnull String input) {
    Set<String> results = new LinkedHashSet<>();
    Set<AutoCompleteEntity> entityResults = new HashSet<>();
    for (SearchHit hit : searchResponse.getHits()) {
      Optional<String> matchedFieldValue =
          hit.getHighlightFields().entrySet().stream()
              .findFirst()
              .map(entry -> entry.getValue().getFragments()[0].string());
      Optional<String> matchedUrn = Optional.ofNullable((String) hit.getSourceAsMap().get("urn"));
      try {
        if (matchedUrn.isPresent()) {
          entityResults.add(
              new AutoCompleteEntity().setUrn(Urn.createFromString(matchedUrn.get())));
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
