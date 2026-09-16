package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.utils.ESAccessControlUtil.restrictUrn;
import static com.linkedin.metadata.search.utils.ESUtils.applyDefaultSearchFilters;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.search.AutocompleteQueryConfiguration;
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
import java.util.regex.Pattern;
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
  private final ElasticSearchConfiguration searchConfiguration;
  @Nonnull private final HighlightBuilder highlights;
  @Nonnull private final SearchServiceConfiguration searchServiceConfig;

  public AutocompleteRequestHandler(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull EntitySpec entitySpec,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull ElasticSearchConfiguration searchConfiguration,
      @Nonnull SearchServiceConfiguration searchServiceConfiguration) {
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
        entitySpec,
        k ->
            new AutocompleteRequestHandler(
                systemOperationContext,
                entitySpec,
                customSearchConfiguration,
                queryFilterRewriteChain,
                searchConfiguration,
                searchServiceConfiguration));
  }

  /**
   * Autocomplete runs as up to two passes (see {@code ESSearchDAO#autoComplete}):
   *
   * <ul>
   *   <li>{@link #STRICT_ALL_TOKENS} — the first pass. For the entity types listed in {@code
   *       allTokensMustPrefixMatchEntities} and a query of two or more tokens, every token must
   *       prefix-match (one MUST clause per token, see {@link #perTokenPrefixMusts}). For every
   *       other entity or a single token this is the same query as {@link #RANKING_ONLY}.
   *   <li>{@link #RANKING_ONLY} — the fallback pass, run only when the strict pass returned nothing
   *       for a query it actually narrowed ({@link #strictPassApplies}). The pre-existing query:
   *       SHOULD clauses rank, nothing is required, so results never go empty.
   * </ul>
   */
  public enum QueryMode {
    STRICT_ALL_TOKENS,
    RANKING_ONLY
  }

  public SearchRequest getSearchRequest(
      @Nonnull OperationContext opContext,
      @Nullable String entityName,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filter,
      @Nullable Integer limit) {
    return getSearchRequest(
        opContext, entityName, input, field, filter, limit, QueryMode.STRICT_ALL_TOKENS);
  }

  public SearchRequest getSearchRequest(
      @Nonnull OperationContext opContext,
      @Nullable String entityName,
      @Nonnull String input,
      @Nullable String field,
      @Nullable Filter filter,
      @Nullable Integer limit,
      @Nonnull QueryMode mode) {
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
            filter, false, searchableFieldTypes, opContext, queryFilterRewriteChain);
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
    baseQuery.should(getQuery(opContext, customAutocompleteConfig, configuredFields, input, mode));

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

    // Apply highlight field configuration
    HighlightBuilder highlightBuilder =
        buildConfiguredHighlights(
            opContext,
            field,
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getAutoCompleteFieldConfigDefault));
    if (highlightBuilder != null) {
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
    return getQuery(
        operationContext, customAutocompleteConfig, baseFields, query, QueryMode.STRICT_ALL_TOKENS);
  }

  public BoolQueryBuilder getQuery(
      @Nonnull OperationContext operationContext,
      @Nullable AutocompleteConfiguration customAutocompleteConfig,
      List<Pair<String, String>> baseFields,
      @Nonnull String query,
      @Nonnull QueryMode mode) {

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

    getAutocompleteQuery(customAutocompleteConfig, configuredFields, query, mode)
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
      @Nonnull QueryMode mode) {
    Optional<QueryBuilder> result = Optional.empty();

    if (customConfig == null || customConfig.isDefaultQuery()) {
      result = Optional.of(defaultQuery(autocompleteFields, query, mode));
    }

    return result;
  }

  private BoolQueryBuilder defaultQuery(
      List<Pair<String, String>> autocompleteFields,
      @Nonnull String query,
      @Nonnull QueryMode mode) {
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
    if (mode == QueryMode.STRICT_ALL_TOKENS) {
      perTokenPrefixMusts(autocompleteFields, query).forEach(finalQuery::must);
    }
    return finalQuery;
  }

  /** Never more MUST clauses than this, however long the pasted string is. */
  public static final int MAX_PREFIX_MATCH_TOKENS = 6;

  // Characters the standard tokenizer also breaks on; apostrophes stay inside a token
  // ("O'Brien" is indexed as one term), periods and quotes are trimmed off the ends ("J.K." ->
  // "J.K", "\"Bob\"" -> "Bob").
  private static final Pattern TOKEN_SEPARATORS = Pattern.compile("[\\s\\-_/,;:()\\[\\]{}<>|+*&]+");
  private static final Pattern TOKEN_TRIM = Pattern.compile("^[.'\"`]+|[.'\"`]+$");

  /**
   * Tokens of an autocomplete query the way the index side sees them: split on whitespace AND on
   * the punctuation the standard tokenizer splits on, trimmed of surrounding punctuation, empties
   * dropped, capped at {@link #MAX_PREFIX_MATCH_TOKENS}. "Mary-Jane O'Brien" -> [Mary, Jane,
   * O'Brien]; "Smith, John" -> [Smith, John]; "J.K. Rowling" -> [J.K, Rowling].
   */
  @VisibleForTesting
  public static List<String> prefixMatchTokens(@Nonnull String query) {
    List<String> tokens = new ArrayList<>();
    for (String raw : TOKEN_SEPARATORS.split(query.trim())) {
      String token = TOKEN_TRIM.matcher(raw).replaceAll("");
      if (!token.isEmpty()) {
        tokens.add(token);
      }
      if (tokens.size() == MAX_PREFIX_MATCH_TOKENS) {
        break;
      }
    }
    return tokens;
  }

  private boolean allTokensMustPrefixMatchEnabled() {
    AutocompleteQueryConfiguration config =
        Optional.ofNullable(searchConfiguration.getSearch().getAutocomplete())
            .orElseGet(AutocompleteQueryConfiguration::new);
    return Optional.ofNullable(config.getAllTokensMustPrefixMatchEntities())
        .orElse(List.of())
        .stream()
        .anyMatch(name -> name.trim().equalsIgnoreCase(entitySpec.getName()));
  }

  /**
   * True when the {@link QueryMode#STRICT_ALL_TOKENS} pass actually narrows this query (a listed
   * entity, two or more tokens) — i.e. when a zero-result strict pass is worth a {@link
   * QueryMode#RANKING_ONLY} fallback pass. False means both modes build the same query.
   */
  public boolean strictPassApplies(@Nonnull String query) {
    return allTokensMustPrefixMatchEnabled() && prefixMatchTokens(query).size() >= 2;
  }

  /**
   * People pickers (owners filter, add owners, assignees) send a name as several tokens. The
   * default {@code bool_prefix} multi_match treats only the LAST token as a prefix and scores a
   * prefix with a constant, so for "John K" the whole-term match "John Fitzgerald" outranks
   * "Johnathan Killroy" even though only the latter matches both tokens; a higher IDF for "john" on
   * a real-sized index makes the gap arbitrarily large, so no additive boost can fix ranking. For
   * the configured entities ({@code
   * elasticsearch.search.autocomplete.allTokensMustPrefixMatchEntities}) require EVERY typed token
   * to prefix-match some autocomplete field (one MUST per token); the existing clauses stay as
   * SHOULDs and keep doing the ranking among the survivors. Single-token queries are untouched, as
   * is every other entity.
   *
   * <p>Deliberately not {@code operator=AND} on the existing multi_match: there only the last token
   * is a prefix, so "john" would have to match "johnathan" as a whole term and the user would
   * disappear from the list.
   *
   * <p>Strictness costs recall ("Jon K" matches nobody), so {@code ESSearchDAO.autoComplete}
   * retries a zero-result strict pass as {@link QueryMode#RANKING_ONLY} ({@link
   * #strictPassApplies}): ranking improves wherever it can, results never disappear.
   */
  private List<QueryBuilder> perTokenPrefixMusts(
      List<Pair<String, String>> autocompleteFields, @Nonnull String query) {
    List<String> tokens = prefixMatchTokens(query);
    if (!allTokensMustPrefixMatchEnabled() || tokens.size() < 2) {
      return List.of();
    }
    List<QueryBuilder> musts = new ArrayList<>();
    for (String token : tokens) {
      // A single-term bool_prefix is a prefix query on the search_as_you_type field, so "john"
      // matches "johnathan".
      MultiMatchQueryBuilder tokenPrefix =
          QueryBuilders.multiMatchQuery(token).type(MultiMatchQueryBuilder.Type.BOOL_PREFIX);
      autocompleteFields.forEach(pair -> tokenPrefix.field(pair.getLeft() + ".ngram"));
      musts.add(tokenPrefix);
    }
    return musts;
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
    if (fieldName.endsWith(".*")) {
      return Stream.of(fieldName);
    }

    return Stream.concat(
        Stream.of(fieldName, fieldName + ".*", fieldName + ".ngram", fieldName + ".delimited"),
        Stream.of(
            ESUtils.toKeywordField(opContext, fieldName, false, opContext.getAspectRetriever())));
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
