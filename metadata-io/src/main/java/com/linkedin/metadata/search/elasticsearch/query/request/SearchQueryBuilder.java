package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.Constants.SKIP_REFERENCE_ASPECT;
import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.*;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.isQuoted;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.unquote;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchValidationConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableRefAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateCoordinator;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateRollout;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplates;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;

/**
 * Builds the keyword query of the V2 entity indices: per-analyzer simple query strings with AND
 * semantics plus exact and prefix matches, summed under a bool query. Search V3 keyword reads use
 * {@link com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder}.
 */
@Slf4j
public class SearchQueryBuilder {
  public static final String STRUCTURED_QUERY_PREFIX = "\\/q ";

  protected final ExactMatchConfiguration exactMatchConfiguration;
  protected final PartialConfiguration partialConfiguration;
  private final WordGramConfiguration wordGramConfiguration;
  protected final SearchValidationConfiguration searchValidationConfiguration;
  private final Pattern validationRegex;

  protected final CustomizedQueryHandler customizedQueryHandler;

  public SearchQueryBuilder(
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    SearchConfiguration resolved = SearchConfiguration.withSectionDefaults(searchConfiguration);
    this.exactMatchConfiguration = resolved.getExactMatch();
    this.partialConfiguration = resolved.getPartial();
    this.wordGramConfiguration = resolved.getWordGram();
    this.searchValidationConfiguration =
        searchConfiguration.getValidation() != null
            ? searchConfiguration.getValidation()
            : new SearchValidationConfiguration();
    this.validationRegex = Pattern.compile(this.searchValidationConfiguration.getRegex());
    this.customizedQueryHandler =
        CustomizedQueryHandler.builder(searchConfiguration.getCustom(), customSearchConfiguration)
            .build();
  }

  public QueryBuilder buildQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    SearchTemplateCoordinator coordinator = SearchTemplates.current();
    if (coordinator != null && coordinator.executes(opContext)) {
      return buildPlan(opContext, entitySpecs, query, fulltext, null, false).query();
    }
    if (coordinator != null
        && coordinator.effect(opContext) == SearchTemplateRollout.Effect.SHADOW) {
      coordinator.shadow(
          opContext,
          entitySpecs,
          query,
          fulltext,
          SearchTemplateRollout.scrollSurface(opContext.getSearchContext().getSearchSurface()));
    }
    return buildQuery(opContext, entitySpecs, query, fulltext, false);
  }

  /**
   * Builds the query, optionally the light Stage 1 query.
   *
   * @param skipExpensiveClauses on Search V3, build the light query: one multi_match instead of the
   *     fuzzy simple queries, and no wildcard, synonym-priority or full exact/prefix clauses.
   *     Callers run the full query (false) when the light query matches nothing. V2 queries ignore
   *     it.
   * @return the query, or null for a light query that a custom configuration leaves without any
   *     clause
   */
  @Nullable
  public QueryBuilder buildQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      boolean skipExpensiveClauses) {
    QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(query).orElse(null);

    // V2 ignores skipExpensiveClauses; the Search V3 builder overrides this method and uses it.
    final QueryBuilder queryBuilder =
        buildInternalQuery(opContext, customQueryConfig, entitySpecs, query, fulltext);
    if (queryBuilder == null) {
      return null;
    }
    return buildScoreFunctions(opContext, customQueryConfig, entitySpecs, query, queryBuilder);
  }

  @Nonnull
  public SearchQueryPlan buildPlan(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      @Nullable String renderUrn,
      boolean scroll) {
    SearchTemplateCoordinator coordinator = SearchTemplates.current();
    if (coordinator != null && coordinator.executes(opContext)) {
      if (searchValidationConfiguration.isEnabled()) {
        validateSearchQuery(query);
      }
      OperationContext bound =
          opContext.getSearchContext().getResolvedSearchTemplate() != null
              ? opContext
              : coordinator.bind(
                  opContext, query, scroll, entitySpecs.stream().map(EntitySpec::getName).toList());
      var rendered =
          coordinator.queryBuilder().render(bound, entitySpecs, query, fulltext, renderUrn, scroll);
      return new SearchQueryPlan(rendered.query(), rendered);
    }
    if (coordinator != null
        && coordinator.effect(opContext) == SearchTemplateRollout.Effect.SHADOW) {
      coordinator.shadow(opContext, entitySpecs, query, fulltext, scroll);
    }
    QueryBuilder queryBuilder = buildQuery(opContext, entitySpecs, query, fulltext, false);
    if (queryBuilder == null) {
      throw new IllegalStateException("Search query produced no clause");
    }
    return SearchQueryPlan.legacy(queryBuilder);
  }

  /**
   * Constructs the search query.
   *
   * @param customQueryConfig custom configuration
   * @param entitySpecs entities being searched
   * @param query search string
   * @param fulltext use fulltext queries
   * @return query builder
   */
  private QueryBuilder buildInternalQuery(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    if (searchValidationConfiguration.isEnabled()) {
      validateSearchQuery(query);
    }
    final String sanitizedQuery = query.replaceFirst("^:+", "");
    final BoolQueryBuilder finalQuery =
        Optional.ofNullable(customQueryConfig)
            .flatMap(
                cqc ->
                    CustomizedQueryHandler.boolQueryBuilder(
                        opContext.getObjectMapper(), cqc, sanitizedQuery))
            .orElse(QueryBuilders.boolQuery());

    if (fulltext && !query.startsWith(STRUCTURED_QUERY_PREFIX)) {
      getSimpleQuery(opContext, customQueryConfig, entitySpecs, sanitizedQuery)
          .ifPresent(finalQuery::should);
      getPrefixAndExactMatchQuery(opContext, customQueryConfig, entitySpecs, sanitizedQuery)
          .ifPresent(finalQuery::should);
    } else {
      final String withoutQueryPrefix =
          query.startsWith(STRUCTURED_QUERY_PREFIX)
              ? query.substring(STRUCTURED_QUERY_PREFIX.length())
              : query;
      getStructuredQuery(opContext, customQueryConfig, entitySpecs, withoutQueryPrefix)
          .ifPresent(finalQuery::should);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQuery(opContext, customQueryConfig, entitySpecs, withoutQueryPrefix)
            .ifPresent(finalQuery::should);
      }
    }

    if (!finalQuery.should().isEmpty()) {
      finalQuery.minimumShouldMatch(1);
    }

    return finalQuery;
  }

  /**
   * Validates search query input to block malicious payloads. Blocks Java deserialization attacks,
   * JNDI injection, and other exploits.
   *
   * @param input The raw search query string
   * @throws ValidationException if the query contains dangerous patterns
   */
  @Nonnull
  public void validateSearchQuery(@Nonnull String input) throws ValidationException {
    // Limit query length
    if (searchValidationConfiguration.isMaxLengthEnabled()
        && input.length() > searchValidationConfiguration.getMaxQueryLength()) {
      log.warn("Blocked excessively long search query: {} characters", input.length());
      throw new ValidationException(
          "Search query exceeds maximum length of "
              + searchValidationConfiguration.getMaxQueryLength()
              + " characters");
    }
    if (validationRegex.matcher(input).matches()) {
      log.warn("Blocked potentially malicious search query.");
      throw new ValidationException("Query rejected due to potentially malicious structure.");
    }
  }

  /**
   * Gets searchable fields from all entities in the input collection. De-duplicates fields across
   * entities. V2 and V2.5 list these fields from the entity specs. Search V3 also uses {@code
   * opContext} to list structured properties.
   *
   * @param entitySpecs: Entity specs to extract searchable fields from
   * @return A set of SearchFieldConfigs containing the searchable fields from the input entities.
   */
  @VisibleForTesting
  public Set<SearchFieldConfig> getStandardFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    Set<SearchFieldConfig> fields = new HashSet<>();
    // Always present
    final float urnBoost =
        Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"));

    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn", urnBoost, SearchableAnnotation.FieldType.URN, true));
    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn.delimited",
            urnBoost * partialConfiguration.getUrnFactor(),
            SearchableAnnotation.FieldType.URN,
            true));

    entitySpecs.stream()
        .map(spec -> getFieldsFromEntitySpec(opContext.getEntityRegistry(), spec))
        .flatMap(Set::stream)
        .collect(Collectors.groupingBy(SearchFieldConfig::fieldName))
        .forEach(
            (key, value) ->
                fields.add(
                    new SearchFieldConfig(
                        key,
                        value.get(0).shortName(),
                        (float)
                            value.stream()
                                .mapToDouble(SearchFieldConfig::boost)
                                .average()
                                .getAsDouble(),
                        value.get(0).analyzer(),
                        value.stream().anyMatch(SearchFieldConfig::hasKeywordSubfield),
                        value.stream().anyMatch(SearchFieldConfig::hasDelimitedSubfield),
                        value.stream().anyMatch(SearchFieldConfig::hasWordGramSubfields),
                        true,
                        value.stream().anyMatch(SearchFieldConfig::isDelimitedSubfield),
                        value.stream().anyMatch(SearchFieldConfig::isKeywordSubfield),
                        value.stream().anyMatch(SearchFieldConfig::isWordGramSubfield))));

    return fields;
  }

  /**
   * The annotation an unqualified query uses for this field. Search V2 uses the annotation as
   * written. Search V3 uses the resolved annotation, so a {@code v3} override replaces the
   * top-level {@code queryByDefault} and {@code boostScore}.
   */
  @Nonnull
  protected Optional<SearchableAnnotation> unqualifiedQueryAnnotation(
      @Nonnull SearchableFieldSpec fieldSpec) {
    SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
    return annotation.isQueryByDefault() ? Optional.of(annotation) : Optional.empty();
  }

  /**
   * Return query by default fields
   *
   * @param entityRegistry entity registry with search annotations
   * @param entitySpec the entity spect
   * @return set of queryByDefault field configurations
   */
  @VisibleForTesting
  public Set<SearchFieldConfig> getFieldsFromEntitySpec(
      @Nonnull EntityRegistry entityRegistry, EntitySpec entitySpec) {
    Set<SearchFieldConfig> fields = new HashSet<>();
    List<SearchableFieldSpec> searchableFieldSpecs = entitySpec.getSearchableFieldSpecs();
    for (SearchableFieldSpec fieldSpec : searchableFieldSpecs) {
      Optional<SearchableAnnotation> queryAnnotation = unqualifiedQueryAnnotation(fieldSpec);
      if (queryAnnotation.isEmpty()) {
        continue;
      }
      SearchableAnnotation annotation = queryAnnotation.get();

      SearchFieldConfig searchFieldConfig = SearchFieldConfig.detectSubFieldType(annotation);
      fields.add(searchFieldConfig);

      if (searchFieldConfig.hasDelimitedSubfield()) {
        fields.add(
            SearchFieldConfig.detectSubFieldType(
                searchFieldConfig.fieldName() + ".delimited",
                searchFieldConfig.boost() * partialConfiguration.getFactor(),
                annotation.getFieldType(),
                true));

        if (searchFieldConfig.hasWordGramSubfields()) {
          addWordGramSearchConfig(fields, searchFieldConfig);
        }
      }
    }

    List<SearchableRefFieldSpec> searchableRefFieldSpecs = entitySpec.getSearchableRefFieldSpecs();
    for (SearchableRefFieldSpec refFieldSpec : searchableRefFieldSpecs) {
      if (!refFieldSpec.getSearchableRefAnnotation().isQueryByDefault()) {
        continue;
      }

      int depth = refFieldSpec.getSearchableRefAnnotation().getDepth();
      Set<SearchFieldConfig> searchFieldConfigs =
          SearchFieldConfig.detectSubFieldType(refFieldSpec, depth, entityRegistry).stream()
              .filter(SearchFieldConfig::isQueryByDefault)
              .collect(Collectors.toSet());
      fields.addAll(searchFieldConfigs);

      Map<String, SearchableAnnotation.FieldType> fieldTypeMap =
          getAllFieldTypeFromSearchableRef(refFieldSpec, depth, entityRegistry, "");
      for (SearchFieldConfig fieldConfig : searchFieldConfigs) {
        if (fieldConfig.hasDelimitedSubfield()) {
          fields.add(
              SearchFieldConfig.detectSubFieldType(
                  fieldConfig.fieldName() + ".delimited",
                  fieldConfig.boost() * partialConfiguration.getFactor(),
                  fieldTypeMap.get(fieldConfig.fieldName()),
                  fieldConfig.isQueryByDefault()));
        }

        if (fieldConfig.hasWordGramSubfields()) {
          addWordGramSearchConfig(fields, fieldConfig);
        }
      }
    }
    return fields;
  }

  private void addWordGramSearchConfig(
      Set<SearchFieldConfig> fields, SearchFieldConfig searchFieldConfig) {
    fields.add(
        SearchFieldConfig.builder()
            .fieldName(searchFieldConfig.fieldName() + ".wordGrams2")
            .boost(searchFieldConfig.boost() * wordGramConfiguration.getTwoGramFactor())
            .analyzer(WORD_GRAM_2_ANALYZER)
            .hasKeywordSubfield(true)
            .hasDelimitedSubfield(true)
            .hasWordGramSubfields(true)
            .isQueryByDefault(true)
            .build());
    fields.add(
        SearchFieldConfig.builder()
            .fieldName(searchFieldConfig.fieldName() + ".wordGrams3")
            .boost(searchFieldConfig.boost() * wordGramConfiguration.getThreeGramFactor())
            .analyzer(WORD_GRAM_3_ANALYZER)
            .hasKeywordSubfield(true)
            .hasDelimitedSubfield(true)
            .hasWordGramSubfields(true)
            .isQueryByDefault(true)
            .build());
    fields.add(
        SearchFieldConfig.builder()
            .fieldName(searchFieldConfig.fieldName() + ".wordGrams4")
            .boost(searchFieldConfig.boost() * wordGramConfiguration.getFourGramFactor())
            .analyzer(WORD_GRAM_4_ANALYZER)
            .hasKeywordSubfield(true)
            .hasDelimitedSubfield(true)
            .hasWordGramSubfields(true)
            .isQueryByDefault(true)
            .build());
  }

  /** The fields one entity searches, with the urn. */
  protected Set<SearchFieldConfig> getStandardFields(
      @Nonnull OperationContext opContext, @Nonnull EntitySpec entitySpec) {
    Set<SearchFieldConfig> fields = new HashSet<>();

    // Always present
    final float urnBoost =
        Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"));

    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn", urnBoost, SearchableAnnotation.FieldType.URN, true));
    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn.delimited",
            urnBoost * partialConfiguration.getUrnFactor(),
            SearchableAnnotation.FieldType.URN,
            true));

    fields.addAll(getFieldsFromEntitySpec(opContext.getEntityRegistry(), entitySpec));

    return fields;
  }

  /**
   * The fields of these entities that the field configuration {@code label} keeps, or {@code
   * fields} when it names none.
   */
  protected Set<SearchFieldConfig> applySearchFieldConfiguration(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Set<SearchFieldConfig> fields,
      @Nullable String label) {
    return customizedQueryHandler.applySearchFieldConfiguration(fields, label);
  }

  protected Optional<QueryBuilder> getSimpleQuery(
      @Nonnull OperationContext operationContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    }

    if (executeSimpleQuery) {
      BoolQueryBuilder simplePerField = QueryBuilders.boolQuery();

      // Get base fields
      Set<SearchFieldConfig> baseFields =
          entitySpecs.stream()
              .map(spec -> getStandardFields(operationContext, spec))
              .flatMap(Set::stream)
              .collect(Collectors.toSet());

      Set<SearchFieldConfig> configuredFields =
          applySearchFieldConfiguration(
              operationContext,
              entitySpecs,
              baseFields,
              customizedQueryHandler.resolveFieldConfiguration(
                  operationContext.getSearchContext().getSearchFlags(),
                  CustomConfiguration::getSearchFieldConfigDefault));

      /*
       * NOTE: This logic applies the queryByDefault annotations for each entity to ALL entities
       * If we ever have fields that are queryByDefault on some entities and not others, this section will need to be refactored
       * to apply an index filter AND the analyzers added here.
       */
      // Simple query string does not use per field analyzers
      // Group the fields by analyzer
      Map<String, List<SearchFieldConfig>> analyzerGroup =
          configuredFields.stream()
              .filter(SearchFieldConfig::isQueryByDefault)
              .collect(Collectors.groupingBy(SearchFieldConfig::analyzer));

      analyzerGroup.keySet().stream()
          .sorted()
          .filter(str -> !str.contains("word_gram"))
          .forEach(
              analyzer -> {
                List<SearchFieldConfig> fieldConfigs = analyzerGroup.get(analyzer);
                SimpleQueryStringBuilder simpleBuilder =
                    QueryBuilders.simpleQueryStringQuery(sanitizedQuery);
                simpleBuilder.analyzer(analyzer);
                simpleBuilder.defaultOperator(Operator.AND);
                Map<String, List<SearchFieldConfig>> fieldAnalyzers =
                    fieldConfigs.stream()
                        .collect(Collectors.groupingBy(SearchFieldConfig::fieldName));
                // De-duplicate fields across different indices
                for (Map.Entry<String, List<SearchFieldConfig>> fieldAnalyzer :
                    fieldAnalyzers.entrySet()) {
                  SearchFieldConfig cfg = fieldAnalyzer.getValue().get(0);
                  simpleBuilder.field(cfg.fieldName(), cfg.boost());
                }
                simplePerField.should(simpleBuilder);
              });

      if (!simplePerField.should().isEmpty()) {
        simplePerField.minimumShouldMatch(1);
      }

      result = Optional.of(simplePerField);
    }

    return result;
  }

  private Optional<QueryBuilder> getPrefixAndExactMatchQuery(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query) {

    final boolean isPrefixQuery =
        customQueryConfig == null
            ? exactMatchConfiguration.isWithPrefix()
            : customQueryConfig.isPrefixMatchQuery();
    final boolean isExactQuery = customQueryConfig == null || customQueryConfig.isExactMatchQuery();

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    String unquotedQuery = unquote(query);

    getStandardFields(opContext, entitySpecs)
        .forEach(
            searchFieldConfig -> {
              boolean caseSensitivityEnabled =
                  exactMatchConfiguration.getCaseSensitivityFactor() > 0.0f;
              float caseSensitivityFactor =
                  caseSensitivityEnabled
                      ? exactMatchConfiguration.getCaseSensitivityFactor()
                      : 1.0f;

              if (searchFieldConfig.isDelimitedSubfield() && isPrefixQuery) {
                finalQuery.should(
                    QueryBuilders.matchPhrasePrefixQuery(searchFieldConfig.fieldName(), query)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getPrefixFactor()
                                * caseSensitivityFactor)
                        .queryName(searchFieldConfig.shortName())); // less than exact
              }

              if (searchFieldConfig.isKeyword() && isExactQuery) {
                // It is important to use the subfield .keyword (it uses a different normalizer)
                // The non-.keyword field removes case information

                // Exact match case-sensitive
                if (caseSensitivityEnabled) {
                  finalQuery.should(
                      QueryBuilders.termQuery(
                              ESUtils.toKeywordField(
                                  opContext,
                                  searchFieldConfig.fieldName(),
                                  false,
                                  opContext.getAspectRetriever()),
                              unquotedQuery)
                          .caseInsensitive(false)
                          .boost(
                              searchFieldConfig.boost() * exactMatchConfiguration.getExactFactor())
                          .queryName(searchFieldConfig.shortName()));
                }

                // Exact match case-insensitive
                finalQuery.should(
                    QueryBuilders.termQuery(
                            ESUtils.toKeywordField(
                                opContext,
                                searchFieldConfig.fieldName(),
                                false,
                                opContext.getAspectRetriever()),
                            unquotedQuery)
                        .caseInsensitive(true)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getExactFactor()
                                * caseSensitivityFactor)
                        .queryName(searchFieldConfig.fieldName()));
              }

              if (searchFieldConfig.isWordGramSubfield() && isPrefixQuery) {
                finalQuery.should(
                    QueryBuilders.matchPhraseQuery(
                            ESUtils.toKeywordField(
                                opContext,
                                searchFieldConfig.fieldName(),
                                false,
                                opContext.getAspectRetriever()),
                            unquotedQuery)
                        .boost(
                            searchFieldConfig.boost()
                                * getWordGramFactor(searchFieldConfig.fieldName()))
                        .queryName(searchFieldConfig.shortName()));
              }
            });

    return finalQuery.should().size() > 0
        ? Optional.of(finalQuery.minimumShouldMatch(1))
        : Optional.empty();
  }

  private Optional<QueryBuilder> getStructuredQuery(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();

    final boolean executeStructuredQuery;
    if (customQueryConfig != null) {
      executeStructuredQuery = customQueryConfig.isStructuredQuery();
    } else {
      executeStructuredQuery = true;
    }

    if (executeStructuredQuery) {
      QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(sanitizedQuery);
      queryBuilder.defaultOperator(Operator.AND);
      getStandardFields(opContext, entitySpecs)
          .forEach(entitySpec -> queryBuilder.field(entitySpec.fieldName(), entitySpec.boost()));
      result = Optional.of(queryBuilder);
    }
    return result;
  }

  protected static FunctionScoreQueryBuilder buildScoreFunctions(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query,
      @Nonnull QueryBuilder queryBuilder) {

    if (customQueryConfig != null) {
      // Prefer configuration function scoring over annotation scoring
      return CustomizedQueryHandler.functionScoreQueryBuilder(
          opContext.getObjectMapper(), customQueryConfig, queryBuilder, query);
    } else {
      return QueryBuilders.functionScoreQuery(
              queryBuilder, buildAnnotationScoreFunctions(entitySpecs))
          .scoreMode(FunctionScoreQuery.ScoreMode.AVG) // Average score functions
          .boostMode(
              CombineFunction.MULTIPLY); // Multiply score function with the score from query;
    }
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder[] buildAnnotationScoreFunctions(
      @Nonnull List<EntitySpec> entitySpecs) {
    List<FunctionScoreQueryBuilder.FilterFunctionBuilder> finalScoreFunctions = new ArrayList<>();

    // Add a default weight of 1.0 to make sure the score function is larger than 1
    finalScoreFunctions.add(
        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
            ScoreFunctionBuilders.weightFactorFunction(1.0f)));

    Map<String, SearchableAnnotation> annotations =
        entitySpecs.stream()
            .map(EntitySpec::getSearchableFieldSpecs)
            .flatMap(List::stream)
            .map(SearchableFieldSpec::getSearchableAnnotation)
            .collect(
                Collectors.toMap(
                    SearchableAnnotation::getFieldName,
                    annotation -> annotation,
                    (annotation1, annotation2) -> annotation1));

    for (Map.Entry<String, SearchableAnnotation> annotationEntry : annotations.entrySet()) {
      SearchableAnnotation annotation = annotationEntry.getValue();
      annotation.getWeightsPerFieldValue().entrySet().stream()
          .map(
              entry ->
                  buildWeightFactorFunction(
                      annotation.getFieldName(), entry.getKey(), entry.getValue()))
          .forEach(finalScoreFunctions::add);
    }

    Map<String, SearchScoreAnnotation> searchScoreAnnotationMap =
        entitySpecs.stream()
            .map(EntitySpec::getSearchScoreFieldSpecs)
            .flatMap(List::stream)
            .map(SearchScoreFieldSpec::getSearchScoreAnnotation)
            .collect(
                Collectors.toMap(
                    SearchScoreAnnotation::getFieldName,
                    annotation -> annotation,
                    (annotation1, annotation2) -> annotation1));
    for (Map.Entry<String, SearchScoreAnnotation> searchScoreAnnotationEntry :
        searchScoreAnnotationMap.entrySet()) {
      SearchScoreAnnotation annotation = searchScoreAnnotationEntry.getValue();
      finalScoreFunctions.add(buildScoreFunctionFromSearchScoreAnnotation(annotation));
    }

    return finalScoreFunctions.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[0]);
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder buildWeightFactorFunction(
      @Nonnull String fieldName, @Nonnull Object fieldValue, double weight) {
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(
        QueryBuilders.termQuery(fieldName, fieldValue),
        ScoreFunctionBuilders.weightFactorFunction((float) weight));
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder
      buildScoreFunctionFromSearchScoreAnnotation(@Nonnull SearchScoreAnnotation annotation) {
    FieldValueFactorFunctionBuilder scoreFunction =
        ScoreFunctionBuilders.fieldValueFactorFunction(annotation.getFieldName());
    scoreFunction.factor((float) annotation.getWeight());
    scoreFunction.missing(annotation.getDefaultValue());
    annotation.getModifier().ifPresent(modifier -> scoreFunction.modifier(mapModifier(modifier)));
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(scoreFunction);
  }

  private static FieldValueFactorFunction.Modifier mapModifier(
      SearchScoreAnnotation.Modifier modifier) {
    switch (modifier) {
      case LOG:
        return FieldValueFactorFunction.Modifier.LOG1P;
      case LN:
        return FieldValueFactorFunction.Modifier.LN1P;
      case SQRT:
        return FieldValueFactorFunction.Modifier.SQRT;
      case SQUARE:
        return FieldValueFactorFunction.Modifier.SQUARE;
      case RECIPROCAL:
        return FieldValueFactorFunction.Modifier.RECIPROCAL;
      default:
        return FieldValueFactorFunction.Modifier.NONE;
    }
  }

  public float getWordGramFactor(String fieldName) {
    if (fieldName.endsWith("Grams2")) {
      return wordGramConfiguration.getTwoGramFactor();
    } else if (fieldName.endsWith("Grams3")) {
      return wordGramConfiguration.getThreeGramFactor();
    } else if (fieldName.endsWith("Grams4")) {
      return wordGramConfiguration.getFourGramFactor();
    }
    throw new IllegalArgumentException(fieldName + " does not end with Grams[2-4]");
  }

  // visible for unit test
  public Map<String, SearchableAnnotation.FieldType> getAllFieldTypeFromSearchableRef(
      SearchableRefFieldSpec refFieldSpec,
      int depth,
      EntityRegistry entityRegistry,
      String prefixField) {
    final SearchableRefAnnotation searchableRefAnnotation =
        refFieldSpec.getSearchableRefAnnotation();
    // contains fieldName as key and SearchableAnnotation as value
    Map<String, SearchableAnnotation.FieldType> fieldNameMap = new HashMap<>();
    EntitySpec refEntitySpec = entityRegistry.getEntitySpec(searchableRefAnnotation.getRefType());
    String fieldName = searchableRefAnnotation.getFieldName();
    final SearchableAnnotation.FieldType fieldType = searchableRefAnnotation.getFieldType();
    if (!prefixField.isEmpty()) {
      fieldName = prefixField + "." + fieldName;
    }

    if (depth == 0) {
      // at depth 0 only URN is present then add and return
      fieldNameMap.put(fieldName, fieldType);
      return fieldNameMap;
    }
    String urnFieldName = fieldName + ".urn";
    fieldNameMap.put(urnFieldName, SearchableAnnotation.FieldType.URN);
    List<AspectSpec> aspectSpecs = refEntitySpec.getAspectSpecs();
    for (AspectSpec aspectSpec : aspectSpecs) {
      if (!SKIP_REFERENCE_ASPECT.contains(aspectSpec.getName())) {
        for (SearchableFieldSpec searchableFieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          String refFieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
          refFieldName = fieldName + "." + refFieldName;
          final SearchableAnnotation searchableAnnotation =
              searchableFieldSpec.getSearchableAnnotation();
          final SearchableAnnotation.FieldType refFieldType = searchableAnnotation.getFieldType();
          fieldNameMap.put(refFieldName, refFieldType);
        }

        for (SearchableRefFieldSpec searchableRefFieldSpec :
            aspectSpec.getSearchableRefFieldSpecs()) {
          String refFieldName = searchableRefFieldSpec.getSearchableRefAnnotation().getFieldName();
          refFieldName = fieldName + "." + refFieldName;
          int newDepth =
              Math.min(depth - 1, searchableRefFieldSpec.getSearchableRefAnnotation().getDepth());
          fieldNameMap.putAll(
              getAllFieldTypeFromSearchableRef(
                  searchableRefFieldSpec, newDepth, entityRegistry, refFieldName));
        }
      }
    }
    return fieldNameMap;
  }
}
