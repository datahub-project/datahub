package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.utils.ESAccessControlUtil.restrictUrn;
import static com.linkedin.metadata.search.utils.ESUtils.applyDefaultSearchFilters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.AutocompleteConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.AutoCompleteEntity;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.*;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

@Slf4j
public class AutocompleteRequestHandler extends BaseRequestHandler {

  private final List<Pair<String, String>> _defaultAutocompleteFields;
  private final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes;

  private static final Map<EntitySpec, AutocompleteRequestHandler>
      AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME = new ConcurrentHashMap<>();

  private final CustomizedQueryHandler customizedQueryHandler;

  private final EntitySpec entitySpec;
  private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final SearchConfiguration searchConfiguration;
  @Nonnull private final HighlightBuilder highlights;

  public AutocompleteRequestHandler(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntitySpec entitySpec,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchConfiguration searchConfiguration) {
    this.entitySpec = entitySpec;
    List<SearchableFieldSpec> fieldSpecs = entitySpec.getSearchableFieldSpecs();
    this.customizedQueryHandler = CustomizedQueryHandler.builder(customSearchConfiguration).build();
    _defaultAutocompleteFields =
        Stream.concat(
                fieldSpecs.stream()
                    .map(SearchableFieldSpec::getSearchableAnnotation)
                    .filter(SearchableAnnotation::isEnableAutocomplete)
                    .map(
                        searchableAnnotation ->
                            Pair.of(
                                searchableAnnotation.getFieldName(),
                                Double.toString(searchableAnnotation.getBoostScore()))),
                Stream.of(Pair.of("urn", "1.0")))
            .collect(Collectors.toList());
    this.highlights = getDefaultHighlights(systemOperationContext);
    searchableFieldTypes =
        fieldSpecs.stream()
            .collect(
                Collectors.toMap(
                    searchableFieldSpec ->
                        searchableFieldSpec.getSearchableAnnotation().getFieldName(),
                    searchableFieldSpec ->
                        new HashSet<>(
                            Collections.singleton(
                                searchableFieldSpec.getSearchableAnnotation().getFieldType())),
                    (set1, set2) -> {
                      set1.addAll(set2);
                      return set1;
                    }));
    this.queryFilterRewriteChain = queryFilterRewriteChain;
    this.searchConfiguration = searchConfiguration;
  }

  public static AutocompleteRequestHandler getBuilder(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntitySpec entitySpec,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull SearchConfiguration searchConfiguration) {
    return AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME.computeIfAbsent(
        entitySpec,
        k ->
            new AutocompleteRequestHandler(
                systemOperationContext,
                entitySpec,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchConfiguration));
  }

  public SearchRequest getSearchRequest(
      @Nonnull OperationContext opContext,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filter,
      int limit) {
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(limit);

    AutocompleteConfiguration customAutocompleteConfig =
        customizedQueryHandler.lookupAutocompleteConfig(input).orElse(null);
    QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(input).orElse(null);

    BoolQueryBuilder baseQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

    // Initial query with input filters
    BoolQueryBuilder filterQuery =
        ESUtils.buildFilterQuery(
            filter, false, searchableFieldTypes, opContext, queryFilterRewriteChain);
    baseQuery.filter(filterQuery);

    // Add autocomplete query
    baseQuery.should(getQuery(opContext.getObjectMapper(), customAutocompleteConfig, input, field));

    // Apply default filters
    BoolQueryBuilder queryWithDefaultFilters =
        applyDefaultSearchFilters(opContext, filter, baseQuery);

    // Apply scoring
    FunctionScoreQueryBuilder functionScoreQueryBuilder =
        Optional.ofNullable(customAutocompleteConfig)
            .flatMap(
                cac ->
                    CustomizedQueryHandler.functionScoreQueryBuilder(
                        opContext.getObjectMapper(),
                        cac,
                        queryWithDefaultFilters,
                        customQueryConfig,
                        input))
            .orElse(
                SearchQueryBuilder.buildScoreFunctions(
                    opContext,
                    customQueryConfig,
                    List.of(entitySpec),
                    input,
                    queryWithDefaultFilters));
    searchSourceBuilder.query(functionScoreQueryBuilder);

    ESUtils.buildSortOrder(searchSourceBuilder, null, List.of(entitySpec));

    // wire inner non-scored query
    searchSourceBuilder.highlighter(
        field == null || field.isEmpty() ? highlights : getHighlights(opContext, List.of(field)));
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  private BoolQueryBuilder getQuery(
      @Nonnull ObjectMapper objectMapper,
      @Nullable AutocompleteConfiguration customAutocompleteConfig,
      @Nonnull String query,
      @Nullable String field) {
    return getQuery(objectMapper, customAutocompleteConfig, getAutocompleteFields(field), query);
  }

  public BoolQueryBuilder getQuery(
      @Nonnull ObjectMapper objectMapper,
      @Nullable AutocompleteConfiguration customAutocompleteConfig,
      List<Pair<String, String>> autocompleteFields,
      @Nonnull String query) {

    BoolQueryBuilder finalQuery =
        Optional.ofNullable(customAutocompleteConfig)
            .flatMap(cac -> CustomizedQueryHandler.boolQueryBuilder(objectMapper, cac, query))
            .orElse(QueryBuilders.boolQuery());

    getAutocompleteQuery(customAutocompleteConfig, autocompleteFields, query)
        .ifPresent(finalQuery::should);

    if (!finalQuery.should().isEmpty()) {
      finalQuery.minimumShouldMatch(1);
    }

    return finalQuery;
  }

  private Optional<QueryBuilder> getAutocompleteQuery(
      @Nullable AutocompleteConfiguration customConfig,
      List<Pair<String, String>> autocompleteFields,
      @Nonnull String query) {
    Optional<QueryBuilder> result = Optional.empty();

    if (customConfig == null || customConfig.isDefaultQuery()) {
      result = Optional.of(defaultQuery(autocompleteFields, query));
    }

    return result;
  }

  private BoolQueryBuilder defaultQuery(
      List<Pair<String, String>> autocompleteFields, @Nonnull String query) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

    // Search for exact matches with higher boost and ngram matches
    MultiMatchQueryBuilder multiMatchQueryBuilder =
        QueryBuilders.multiMatchQuery(query).type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);

    autocompleteFields.forEach(
        pair -> {
          final String fieldName = (String) pair.getLeft();
          final float boostScore = Float.parseFloat((String) pair.getRight());
          multiMatchQueryBuilder.field(fieldName + ".ngram");
          multiMatchQueryBuilder.field(fieldName + ".ngram._2gram");
          multiMatchQueryBuilder.field(fieldName + ".ngram._3gram");
          multiMatchQueryBuilder.field(fieldName + ".ngram._4gram");
          multiMatchQueryBuilder.field(fieldName + ".delimited");
          if (!fieldName.equalsIgnoreCase("urn")) {
            multiMatchQueryBuilder.field(fieldName + ".ngram", boostScore);
            multiMatchQueryBuilder.field(
                fieldName + ".ngram._2gram",
                boostScore * (searchConfiguration.getWordGram().getTwoGramFactor()));
            multiMatchQueryBuilder.field(
                fieldName + ".ngram._3gram",
                boostScore * (searchConfiguration.getWordGram().getThreeGramFactor()));
            multiMatchQueryBuilder.field(
                fieldName + ".ngram._4gram",
                boostScore * (searchConfiguration.getWordGram().getFourGramFactor()));
            finalQuery.should(
                QueryBuilders.matchQuery(fieldName + ".keyword", query).boost(boostScore));
          }
          finalQuery.should(QueryBuilders.matchPhrasePrefixQuery(fieldName + ".delimited", query));
        });
    finalQuery.should(multiMatchQueryBuilder);
    return finalQuery;
  }

  @Override
  public Collection<String> getDefaultQueryFieldNames() {
    return _defaultAutocompleteFields.stream().map(Pair::getKey).collect(Collectors.toList());
  }

  @Override
  protected Collection<String> getValidQueryFieldNames() {
    return searchableFieldTypes.keySet();
  }

  @Override
  protected Stream<String> highlightFieldExpansion(
      @Nonnull OperationContext opContext, @Nonnull String fieldName) {
    return Stream.concat(
        Stream.of(fieldName, fieldName + ".*", fieldName + ".ngram", fieldName + ".delimited"),
        Stream.of(ESUtils.toKeywordField(fieldName, false, opContext.getAspectRetriever())));
  }

  private List<Pair<String, String>> getAutocompleteFields(@Nullable String field) {
    if (field != null && !field.isEmpty() && !field.equalsIgnoreCase("urn")) {
      return ImmutableList.of(Pair.of(field, "10.0"));
    }
    return _defaultAutocompleteFields;
  }

  public AutoCompleteResult extractResult(
      @Nonnull OperationContext opContext,
      @Nonnull SearchResponse searchResponse,
      @Nonnull String input) {
    // use lists to preserve ranking
    List<String> results = new ArrayList<>();
    List<AutoCompleteEntity> entityResults = new ArrayList<>();

    for (SearchHit hit : searchResponse.getHits()) {
      Optional<String> matchedFieldValue =
          hit.getHighlightFields().entrySet().stream()
              .findFirst()
              .map(entry -> entry.getValue().getFragments()[0].string());
      Optional<String> matchedUrn = Optional.ofNullable((String) hit.getSourceAsMap().get("urn"));
      try {
        if (matchedUrn.isPresent()) {
          Urn autoCompleteUrn = Urn.createFromString(matchedUrn.get());
          if (!restrictUrn(opContext, autoCompleteUrn)) {
            matchedFieldValue.ifPresent(
                value -> {
                  entityResults.add(new AutoCompleteEntity().setUrn(autoCompleteUrn));
                  results.add(value);
                });
          }
        }
      } catch (URISyntaxException e) {
        log.warn(String.format("Failed to create urn %s", matchedUrn.get()));
      }
    }
    return new AutoCompleteResult()
        .setQuery(input)
        .setSuggestions(new StringArray(results))
        .setEntities(new AutoCompleteEntityArray(entityResults));
  }
}
