package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.utils.ESAccessControlUtil.restrictUrn;
import static com.linkedin.metadata.search.utils.ESUtils.applyDefaultSearchFilters;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
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

  // Keyed by the V3 read decision too: a handler builds V2 or V3 field names from its configuration
  private static final Map<Pair<EntitySpec, Boolean>, AutocompleteRequestHandler>
      AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME = new ConcurrentHashMap<>();

  // Search V3 field names. They must match the effective V3 mapping: the bundled
  // search_entity_mapping_config.yaml as MultiEntityMappingsUtils.buildSearchSection extends it.
  // Entity names copy into tier 1, whose text subfield is stored, so it can highlight
  private static final String V3_TIER_1_TEXT_FIELD = "_search.tier_1.full";
  private static final String V3_ENTITY_NAME_FIELD = "_search.entityName";
  // Types Search V3 maps to keyword or text roots (FieldTypeMapper), which take a prefix query
  private static final Set<SearchableAnnotation.FieldType> V3_PREFIX_FIELD_TYPES =
      Set.of(
          SearchableAnnotation.FieldType.KEYWORD,
          SearchableAnnotation.FieldType.TEXT,
          SearchableAnnotation.FieldType.TEXT_PARTIAL,
          SearchableAnnotation.FieldType.WORD_GRAM,
          SearchableAnnotation.FieldType.URN,
          SearchableAnnotation.FieldType.URN_PARTIAL,
          SearchableAnnotation.FieldType.BROWSE_PATH,
          SearchableAnnotation.FieldType.BROWSE_PATH_V2);

  private final CustomizedQueryHandler customizedQueryHandler;

  private final EntitySpec entitySpec;
  private final QueryFilterRewriteChain queryFilterRewriteChain;
  private final ElasticSearchConfiguration searchConfiguration;
  @Nonnull private final HighlightBuilder highlights;
  @Nonnull private final SearchServiceConfiguration searchServiceConfig;

  /**
   * Search V3 entity indices keep analyzed text only in the {@code _search.tier_N} fields, and
   * their root fields have no subfields.
   */
  private final boolean v3KeywordReadEnabled;

  public AutocompleteRequestHandler(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntitySpec entitySpec,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull ElasticSearchConfiguration searchConfiguration,
      @Nonnull SearchServiceConfiguration searchServiceConfiguration) {
    this.v3KeywordReadEnabled =
        EntitySearchIndexResolver.shouldReadV3(searchConfiguration.getEntityIndex());
    this.entitySpec = entitySpec;
    List<SearchableFieldSpec> fieldSpecs = entitySpec.getSearchableFieldSpecs();
    this.customizedQueryHandler =
        CustomizedQueryHandler.builder(
                searchConfiguration.getSearch().getCustom(), customSearchConfiguration)
            .build();
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
    this.searchServiceConfig = searchServiceConfiguration;
  }

  public static AutocompleteRequestHandler getBuilder(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntitySpec entitySpec,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull ElasticSearchConfiguration searchConfiguration,
      @Nonnull SearchServiceConfiguration searchServiceConfiguration) {
    return AUTOCOMPLETE_QUERY_BUILDER_BY_ENTITY_NAME.computeIfAbsent(
        Pair.of(
            entitySpec,
            EntitySearchIndexResolver.shouldReadV3(searchConfiguration.getEntityIndex())),
        k ->
            new AutocompleteRequestHandler(
                systemOperationContext,
                entitySpec,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchConfiguration,
                searchServiceConfiguration));
  }

  public SearchRequest getSearchRequest(
      @Nonnull OperationContext opContext,
      @Nullable String entityName,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filter,
      @Nullable Integer limit) {
    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(ConfigUtils.applyLimit(searchServiceConfig, limit));

    AutocompleteConfiguration customAutocompleteConfig =
        customizedQueryHandler.lookupAutocompleteConfig(input).orElse(null);
    QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(input).orElse(null);

    BoolQueryBuilder baseQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

    // Initial query with input filters
    BoolQueryBuilder filterQuery =
        ESUtils.buildFilterQuery(
            v3KeywordReadEnabled ? ESUtils.toV3EntityFilter(opContext, filter) : filter,
            false,
            v3KeywordReadEnabled,
            searchableFieldTypes,
            opContext,
            queryFilterRewriteChain);
    baseQuery.filter(filterQuery);

    // Apply field configuration to autocomplete fields
    List<Pair<String, String>> baseAutocompleteFields = getAutocompleteFields(field);
    List<Pair<String, String>> configuredFields =
        customizedQueryHandler.applyAutocompleteFieldConfiguration(
            baseAutocompleteFields,
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getAutoCompleteFieldConfigDefault));

    // Add autocomplete query
    final boolean defaultFields = isDefaultFieldsRequest(field);
    baseQuery.should(
        getQuery(opContext, customAutocompleteConfig, configuredFields, input, defaultFields));

    // Apply default filters
    BoolQueryBuilder queryWithDefaultFilters =
        applyDefaultSearchFilters(
            opContext,
            entityName != null ? List.of(entityName) : Collections.emptyList(),
            filter,
            baseQuery,
            searchConfiguration.getEntityIndex());

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

    // Apply highlight field configuration. On V3 a urn request matches the default fields, so it
    // highlights them too
    HighlightBuilder highlightBuilder =
        buildConfiguredHighlights(
            opContext,
            v3KeywordReadEnabled && defaultFields ? null : field,
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getAutoCompleteFieldConfigDefault));
    if (highlightBuilder != null) {
      if (v3KeywordReadEnabled && defaultFields) {
        // Root fields highlight where their prefix matched and tier 1 where a name word matched,
        // so each suggestion is a matched value. A hit without a highlight is dropped
        highlightBuilder.field(V3_TIER_1_TEXT_FIELD);
      }
      searchSourceBuilder.highlighter(highlightBuilder);
    }

    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  // Helper method to build highlights with field configuration
  private HighlightBuilder buildConfiguredHighlights(
      @Nonnull OperationContext opContext,
      @Nullable String field,
      @Nullable String fieldConfigLabel) {

    // Check if highlighting is enabled for this configuration
    if (!customizedQueryHandler.isHighlightingEnabled(fieldConfigLabel)) {
      return null;
    }

    // Determine base highlight fields
    Set<String> baseHighlightFields;
    if (field != null && !field.isEmpty()) {
      baseHighlightFields = Set.of(field);
    } else {
      // Get default highlight fields from autocomplete fields
      baseHighlightFields =
          _defaultAutocompleteFields.stream().map(Pair::getLeft).collect(Collectors.toSet());
    }

    // Apply field configuration
    HighlightConfigurationResult configResult =
        customizedQueryHandler.getHighlightFieldConfiguration(
            baseHighlightFields, fieldConfigLabel);

    if (configResult.getFieldsToHighlight().isEmpty()) {
      // If no fields after configuration, use the base implementation with defaults
      return getDefaultHighlights(opContext);
    }

    // Build highlights with configured fields
    return buildHighlightsWithSelectiveExpansion(
        opContext,
        configResult.getFieldsToHighlight(),
        configResult.getExplicitlyConfiguredFields());
  }

  private BoolQueryBuilder getQuery(
      @Nonnull OperationContext operationContext,
      @Nullable AutocompleteConfiguration customAutocompleteConfig,
      @Nonnull String query,
      @Nullable String field) {
    return getQuery(
        operationContext, customAutocompleteConfig, getAutocompleteFields(field), query);
  }

  public BoolQueryBuilder getQuery(
      @Nonnull OperationContext operationContext,
      @Nullable AutocompleteConfiguration customAutocompleteConfig,
      List<Pair<String, String>> baseFields,
      @Nonnull String query) {
    return getQuery(operationContext, customAutocompleteConfig, baseFields, query, true);
  }

  private BoolQueryBuilder getQuery(
      @Nonnull OperationContext operationContext,
      @Nullable AutocompleteConfiguration customAutocompleteConfig,
      List<Pair<String, String>> baseFields,
      @Nonnull String query,
      final boolean defaultFields) {

    // Apply field configuration
    List<Pair<String, String>> configuredFields =
        customizedQueryHandler.applyAutocompleteFieldConfiguration(
            baseFields,
            customizedQueryHandler.resolveFieldConfiguration(
                operationContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getAutoCompleteFieldConfigDefault));

    BoolQueryBuilder finalQuery =
        Optional.ofNullable(customAutocompleteConfig)
            .flatMap(
                cac ->
                    CustomizedQueryHandler.boolQueryBuilder(
                        operationContext.getObjectMapper(), cac, query))
            .orElse(QueryBuilders.boolQuery());

    getAutocompleteQuery(customAutocompleteConfig, configuredFields, query, defaultFields)
        .ifPresent(finalQuery::should);

    if (!finalQuery.should().isEmpty()) {
      finalQuery.minimumShouldMatch(1);
    }

    return finalQuery;
  }

  private Optional<QueryBuilder> getAutocompleteQuery(
      @Nullable AutocompleteConfiguration customConfig,
      List<Pair<String, String>> autocompleteFields,
      @Nonnull String query,
      final boolean defaultFields) {
    Optional<QueryBuilder> result = Optional.empty();

    if (customConfig == null || customConfig.isDefaultQuery()) {
      result = Optional.of(defaultQuery(autocompleteFields, query, defaultFields));
    }

    return result;
  }

  private BoolQueryBuilder defaultQuery(
      List<Pair<String, String>> autocompleteFields,
      @Nonnull String query,
      final boolean defaultFields) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

    if (v3KeywordReadEnabled) {
      if (defaultFields) {
        // Entity names copy into tier 1 and _search.entityName
        finalQuery
            .should(QueryBuilders.matchBoolPrefixQuery(V3_TIER_1_TEXT_FIELD, query))
            .should(QueryBuilders.prefixQuery(V3_ENTITY_NAME_FIELD, query).caseInsensitive(true));
      }
      // Fields without a search tier have no text copy, so each string field is also prefix
      // matched on its root value; the engine rejects a prefix on other types. Every urn starts
      // with "urn:", so default urn fields only take urn input: any other prefix of it would scan
      // every document
      final boolean urnInput = query.regionMatches(true, 0, "urn:", 0, 4);
      autocompleteFields.stream()
          .filter(pair -> isPrefixField(pair.getLeft()))
          .filter(pair -> !defaultFields || urnInput || !isUrnField(pair.getLeft()))
          .forEach(
              pair ->
                  finalQuery.should(
                      QueryBuilders.prefixQuery(pair.getLeft(), query)
                          .caseInsensitive(true)
                          .boost(Float.parseFloat(pair.getRight()))));
      if (finalQuery.should().isEmpty()) {
        // A requested field no prefix can match: a bool without clauses would match everything
        finalQuery.should(new MatchNoneQueryBuilder());
      }
      return finalQuery;
    }

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
                boostScore * (searchConfiguration.getSearch().getWordGram().getTwoGramFactor()));
            multiMatchQueryBuilder.field(
                fieldName + ".ngram._3gram",
                boostScore * (searchConfiguration.getSearch().getWordGram().getThreeGramFactor()));
            multiMatchQueryBuilder.field(
                fieldName + ".ngram._4gram",
                boostScore * (searchConfiguration.getSearch().getWordGram().getFourGramFactor()));
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
    if (fieldName.endsWith(".*") || v3KeywordReadEnabled) {
      return Stream.of(fieldName);
    }

    return Stream.concat(
        Stream.of(fieldName, fieldName + ".*", fieldName + ".ngram", fieldName + ".delimited"),
        Stream.of(
            ESUtils.toKeywordField(opContext, fieldName, false, opContext.getAspectRetriever())));
  }

  private List<Pair<String, String>> getAutocompleteFields(@Nullable String field) {
    if (!isDefaultFieldsRequest(field)) {
      return ImmutableList.of(Pair.of(field, "10.0"));
    }
    return _defaultAutocompleteFields;
  }

  /** No field, or the urn, means the entity's default autocomplete fields. */
  private static boolean isDefaultFieldsRequest(@Nullable String field) {
    return field == null || field.isEmpty() || field.equalsIgnoreCase("urn");
  }

  private boolean isUrnField(@Nonnull String fieldName) {
    Set<SearchableAnnotation.FieldType> fieldTypes =
        searchableFieldTypes.getOrDefault(fieldName, Set.of());
    return fieldName.equalsIgnoreCase("urn")
        || fieldTypes.contains(SearchableAnnotation.FieldType.URN)
        || fieldTypes.contains(SearchableAnnotation.FieldType.URN_PARTIAL);
  }

  /** The urn, or a field whose Search V3 root is a keyword or text field. */
  private boolean isPrefixField(@Nonnull String fieldName) {
    Set<SearchableAnnotation.FieldType> fieldTypes = searchableFieldTypes.get(fieldName);
    return fieldName.equalsIgnoreCase("urn")
        || (fieldTypes != null && V3_PREFIX_FIELD_TYPES.containsAll(fieldTypes));
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
