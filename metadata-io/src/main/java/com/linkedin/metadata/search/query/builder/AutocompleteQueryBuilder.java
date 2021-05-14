package com.linkedin.metadata.search.query.builder;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.AutoCompleteResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;


public class AutocompleteQueryBuilder {

  private final EntitySpec _entitySpec;
  private final List<String> _defaultAutocompleteFields;

  private static final String ANALYZER = "word_delimited";

  public AutocompleteQueryBuilder(@Nonnull String entityName) {
    _entitySpec = SnapshotEntityRegistry.getInstance().getEntitySpec(entityName);
    _defaultAutocompleteFields = _entitySpec.getSearchableFieldSpecs()
        .stream()
        .filter(SearchableFieldSpec::isDefaultAutocomplete)
        .map(SearchableFieldSpec::getFieldName)
        .collect(Collectors.toList());
  }

  private static final Map<String, AutocompleteQueryBuilder> AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME =
      new HashMap<>();

  public static AutocompleteQueryBuilder getBuilder(@Nonnull String entityName) {
    return AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME.computeIfAbsent(entityName,
        k -> new AutocompleteQueryBuilder(entityName));
  }

  public QueryBuilder getQuery(@Nonnull String query, @Nullable String field) {
    // Search for exact matches with higher boost and ngram matches
    List<String> fieldNames = getAutocompleteFields(field).stream()
        .flatMap(fieldName -> Stream.of(fieldName + "^4", fieldName + ".ngram"))
        .collect(Collectors.toList());
    MultiMatchQueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(query, fieldNames.toArray(new String[0]));
    queryBuilder.analyzer(ANALYZER);
    return queryBuilder;
  }

  // Get HighllightBuilder to highlight the matched field
  public HighlightBuilder getHighlights(@Nullable String field) {
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
    return null;
  }
}
