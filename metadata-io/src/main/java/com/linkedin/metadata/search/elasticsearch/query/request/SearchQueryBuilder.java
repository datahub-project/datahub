package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.Constants.SKIP_REFERENCE_ASPECT;
import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.*;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.isQuoted;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.unquote;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.aspect.AspectRetriever;
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
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;

@Slf4j
public class SearchQueryBuilder {
  public static final String STRUCTURED_QUERY_PREFIX = "\\/q ";
  private final ExactMatchConfiguration exactMatchConfiguration;
  private final PartialConfiguration partialConfiguration;
  private final WordGramConfiguration wordGramConfiguration;
  private final SearchValidationConfiguration searchValidationConfiguration;
  private final Pattern validationRegex;
  private final boolean enableSearchV2_5Default;
  private final boolean enableFuzzyMatchingDefault;

  private final CustomizedQueryHandler customizedQueryHandler;

  /**
   * V2.5 boost overrides: maps original boostScore to V2.5 replacement value. V2.5 compresses the
   * boost range so Stage 1 is more uniform and Stage 2 rescore (rescore_config.yaml) handles
   * nuanced ranking with sigmoid-normalized signals.
   */
  @VisibleForTesting
  static final Map<Float, Float> V2_5_BOOST_OVERRIDES =
      Map.of(
          10.0f, 2.5f, // entity names: reduce dominant name boost
          8.0f, 1.5f, // ML/entity keys: reduce key field boost
          4.0f, 1.5f, // tool fields (ChartKey, DashboardKey, DataProcessKey)
          2.0f, 2.5f, // ldap field (CorpUserKey)
          0.5f, 0.8f, // URN references (tags, glossary terms): slight increase
          0.1f, 1.0f, // field descriptions: increase from near-zero
          0.2f, 1.0f // field labels: increase from near-zero
          );

  private static SearchFieldConfig applyV2_5Boost(SearchFieldConfig cfg) {
    Float override = V2_5_BOOST_OVERRIDES.get(cfg.boost());
    if (override != null) {
      return cfg.toBuilder().boost(override).build();
    }
    return cfg;
  }

  public SearchQueryBuilder(
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    this.exactMatchConfiguration = searchConfiguration.getExactMatch();
    this.partialConfiguration = searchConfiguration.getPartial();
    this.wordGramConfiguration = searchConfiguration.getWordGram();
    this.searchValidationConfiguration = searchConfiguration.getValidation();
    this.validationRegex = Pattern.compile(searchConfiguration.getValidation().getRegex());
    this.enableSearchV2_5Default = searchConfiguration.isEnableSearchV2_5();
    this.enableFuzzyMatchingDefault = searchConfiguration.isEnableFuzzyMatching();
    this.customizedQueryHandler =
        CustomizedQueryHandler.builder(searchConfiguration.getCustom(), customSearchConfiguration)
            .build();
  }

  /** Entry point for query building. Dispatches to V2 or V2.5 based on configuration. */
  @Nonnull
  public QueryBuilder buildQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(query).orElse(null);

    // Resolve search version for this request
    boolean useV2_5 = resolveSearchV2_5Enabled(opContext);

    if (log.isDebugEnabled()) {
      log.debug("Building search query using version: {}", useV2_5 ? "V2.5" : "V2");
    }

    final QueryBuilder queryBuilder;
    if (useV2_5) {
      queryBuilder =
          buildInternalQueryV2_5(opContext, customQueryConfig, entitySpecs, query, fulltext);
    } else {
      queryBuilder =
          buildInternalQueryV2(opContext, customQueryConfig, entitySpecs, query, fulltext);
    }

    return buildScoreFunctions(opContext, customQueryConfig, entitySpecs, query, queryBuilder);
  }

  /**
   * Build query using V2 logic (acryl-main branch behavior). This is the EXACT code from acryl-main
   * to ensure zero regression.
   *
   * <p>DO NOT MODIFY THIS METHOD - it must match acryl-main exactly. All changes should go in
   * buildInternalQueryV2_5() instead.
   *
   * @param customQueryConfig custom configuration
   * @param entitySpecs entities being searched
   * @param query search string
   * @param fulltext use fulltext queries
   * @return query builder
   */
  private QueryBuilder buildInternalQueryV2(
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
      getSimpleQueryV2(opContext, customQueryConfig, entitySpecs, sanitizedQuery)
          .ifPresent(finalQuery::should);
      getPrefixAndExactMatchQuery(
              opContext.getEntityRegistry(),
              customQueryConfig,
              entitySpecs,
              sanitizedQuery,
              opContext.getAspectRetriever(),
              false)
          .ifPresent(finalQuery::should);
    } else {
      final String withoutQueryPrefix =
          query.startsWith(STRUCTURED_QUERY_PREFIX)
              ? query.substring(STRUCTURED_QUERY_PREFIX.length())
              : query;
      getStructuredQuery(
              opContext.getEntityRegistry(),
              customQueryConfig,
              entitySpecs,
              withoutQueryPrefix,
              false)
          .ifPresent(finalQuery::should);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQuery(
                opContext.getEntityRegistry(),
                customQueryConfig,
                entitySpecs,
                withoutQueryPrefix,
                opContext.getAspectRetriever(),
                false)
            .ifPresent(finalQuery::should);
      }
    }

    if (!finalQuery.should().isEmpty()) {
      finalQuery.minimumShouldMatch(1);
    }

    return finalQuery;
  }

  /**
   * Constructs the search query using V2.5 logic. This contains all the new ranking improvements: -
   * Fuzzy matching with ~1/~2 edit distance - OR operator for lenient matching - DisMaxQuery
   * structure - SUM + SUM scoring mode - Updated field boosts
   *
   * @param customQueryConfig custom configuration
   * @param entitySpecs entities being searched
   * @param query search string
   * @param fulltext use fulltext queries
   * @return query builder
   */
  private QueryBuilder buildInternalQueryV2_5(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    if (searchValidationConfiguration.isEnabled()) {
      validateSearchQuery(query);
    }
    final String sanitizedQuery = query.replaceFirst("^:+", "");

    // Use dis_max instead of bool/should to prevent score accumulation
    // Takes MAX field score + (tie_breaker × sum_of_other_scores)
    // This ensures exact name matches win over partial matches across many fields
    final DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();

    // Set tie_breaker to 0.1 (10% contribution from other matching queries)
    // This allows secondary matches to slightly influence ranking when primary scores are equal
    disMaxQuery.tieBreaker(0.1f);

    // Check if custom query config provides a bool query wrapper (for filters, must clauses)
    final BoolQueryBuilder customBoolQuery =
        Optional.ofNullable(customQueryConfig)
            .flatMap(
                cqc ->
                    CustomizedQueryHandler.boolQueryBuilder(
                        opContext.getObjectMapper(), cqc, sanitizedQuery))
            .orElse(null);

    if (fulltext && !query.startsWith(STRUCTURED_QUERY_PREFIX)) {
      getSimpleQueryV2_5(opContext, customQueryConfig, entitySpecs, sanitizedQuery)
          .ifPresent(disMaxQuery::add);
      getPrefixAndExactMatchQuery(
              opContext.getEntityRegistry(),
              customQueryConfig,
              entitySpecs,
              sanitizedQuery,
              opContext.getAspectRetriever(),
              true)
          .ifPresent(disMaxQuery::add);
    } else {
      final String withoutQueryPrefix =
          query.startsWith(STRUCTURED_QUERY_PREFIX)
              ? query.substring(STRUCTURED_QUERY_PREFIX.length())
              : query;
      getStructuredQuery(
              opContext.getEntityRegistry(),
              customQueryConfig,
              entitySpecs,
              withoutQueryPrefix,
              true)
          .ifPresent(disMaxQuery::add);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQuery(
                opContext.getEntityRegistry(),
                customQueryConfig,
                entitySpecs,
                withoutQueryPrefix,
                opContext.getAspectRetriever(),
                true)
            .ifPresent(disMaxQuery::add);
      }
    }

    // Check if dis_max has any queries (it requires at least one sub-query)
    boolean hasDisMaxQueries = !disMaxQuery.innerQueries().isEmpty();

    // If custom bool query exists (with filters/must clauses), wrap dis_max inside it
    if (customBoolQuery != null
        && (!customBoolQuery.filter().isEmpty()
            || !customBoolQuery.must().isEmpty()
            || !customBoolQuery.mustNot().isEmpty())) {
      if (hasDisMaxQueries) {
        customBoolQuery.must(disMaxQuery);
      }
      return customBoolQuery;
    }

    // If dis_max has no queries, return match_all to avoid malformed query error
    if (!hasDisMaxQueries) {
      return QueryBuilders.matchAllQuery();
    }

    return disMaxQuery;
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
   * Adds fuzzy matching operators to query terms for typo tolerance.
   *
   * <p>Appends ~2 to each term to enable fuzzy matching with up to 2 character edits. This handles
   * common typos like "notiphication" → "notification" without requiring exact matches.
   *
   * <p>Edit distance scaling: - Terms 1-2 chars: No fuzzy (exact match) - Terms 3-5 chars: ~1 (1
   * edit) - Terms 6+ chars: ~2 (2 edits)
   *
   * @param query The sanitized search query
   * @return Query with fuzzy operators added to terms
   */
  @Nonnull
  private static String makeFuzzyQuery(@Nonnull String query) {
    // Split on whitespace and add ~N fuzzy operator to each term
    // Use different edit distances based on term length (mimics Fuzziness.AUTO)
    String[] terms = query.split("\\s+");
    StringBuilder fuzzyQuery = new StringBuilder();

    for (int i = 0; i < terms.length; i++) {
      String term = terms[i].trim();
      if (term.isEmpty()) {
        continue;
      }

      fuzzyQuery.append(term);

      // Add fuzzy operator based on term length (similar to Fuzziness.AUTO)
      int termLength = term.length();
      if (termLength >= 6) {
        fuzzyQuery.append("~2"); // Allow 2 edits for longer terms
      } else if (termLength >= 3) {
        fuzzyQuery.append("~1"); // Allow 1 edit for medium terms
      }
      // Terms < 3 chars get no fuzzy operator (exact match)

      if (i < terms.length - 1) {
        fuzzyQuery.append(" ");
      }
    }

    return fuzzyQuery.toString();
  }

  /**
   * V2: Simple query with exact matching, AND operator, and strict behavior. This is the EXACT code
   * from acryl-main to ensure zero regression.
   */
  private Optional<QueryBuilder> getSimpleQueryV2(
      @Nonnull OperationContext operationContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();
    EntityRegistry entityRegistry = operationContext.getEntityRegistry();

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    }

    if (executeSimpleQuery) {
      BoolQueryBuilder simplePerField = QueryBuilders.boolQuery();

      // Get base fields (V2: use original annotation boosts)
      Set<SearchFieldConfig> baseFields =
          entitySpecs.stream()
              .map(spec -> getStandardFields(entityRegistry, spec, false))
              .flatMap(Set::stream)
              .collect(Collectors.toSet());

      Set<SearchFieldConfig> configuredFields =
          customizedQueryHandler.applySearchFieldConfiguration(
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

  /**
   * Gets searchable fields from all entities in the input collection. De-duplicates fields across
   * entities.
   *
   * @param entitySpecs: Entity specs to extract searchable fields from
   * @return A set of SearchFieldConfigs containing the searchable fields from the input entities.
   */
  @VisibleForTesting
  public Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<EntitySpec> entitySpecs) {
    return getStandardFields(entityRegistry, entitySpecs, false);
  }

  private Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull Collection<EntitySpec> entitySpecs,
      boolean useV2_5) {
    Set<SearchFieldConfig> fields = new HashSet<>();
    // Always present — URN boost is NOT overridden for V2.5
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
        .map(spec -> getFieldsFromEntitySpec(entityRegistry, spec, useV2_5))
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
   * Return query by default fields
   *
   * @param entityRegistry entity registry with search annotations
   * @param entitySpec the entity spect
   * @return set of queryByDefault field configurations
   */
  @VisibleForTesting
  public Set<SearchFieldConfig> getFieldsFromEntitySpec(
      @Nonnull EntityRegistry entityRegistry, EntitySpec entitySpec) {
    return getFieldsFromEntitySpec(entityRegistry, entitySpec, false);
  }

  private Set<SearchFieldConfig> getFieldsFromEntitySpec(
      @Nonnull EntityRegistry entityRegistry, EntitySpec entitySpec, boolean useV2_5) {
    Set<SearchFieldConfig> fields = new HashSet<>();
    List<SearchableFieldSpec> searchableFieldSpecs = entitySpec.getSearchableFieldSpecs();
    for (SearchableFieldSpec fieldSpec : searchableFieldSpecs) {
      if (!fieldSpec.getSearchableAnnotation().isQueryByDefault()) {
        continue;
      }

      SearchFieldConfig searchFieldConfig = SearchFieldConfig.detectSubFieldType(fieldSpec);
      if (useV2_5) {
        searchFieldConfig = applyV2_5Boost(searchFieldConfig);
      }
      fields.add(searchFieldConfig);

      if (SearchFieldConfig.detectSubFieldType(fieldSpec).hasDelimitedSubfield()) {
        final SearchableAnnotation searchableAnnotation = fieldSpec.getSearchableAnnotation();

        fields.add(
            SearchFieldConfig.detectSubFieldType(
                searchFieldConfig.fieldName() + ".delimited",
                searchFieldConfig.boost() * partialConfiguration.getFactor(),
                searchableAnnotation.getFieldType(),
                searchableAnnotation.isQueryByDefault()));

        if (SearchFieldConfig.detectSubFieldType(fieldSpec).hasWordGramSubfields()) {
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
      if (useV2_5) {
        searchFieldConfigs =
            searchFieldConfigs.stream()
                .map(SearchQueryBuilder::applyV2_5Boost)
                .collect(Collectors.toSet());
      }
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

  private Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec, boolean useV2_5) {
    Set<SearchFieldConfig> fields = new HashSet<>();

    // Always present — URN boost is NOT overridden for V2.5
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

    fields.addAll(getFieldsFromEntitySpec(entityRegistry, entitySpec, useV2_5));

    return fields;
  }

  /** V2.5: Simple query with fuzzy matching, OR operator, and lenient behavior. */
  private Optional<QueryBuilder> getSimpleQueryV2_5(
      @Nonnull OperationContext operationContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();
    EntityRegistry entityRegistry = operationContext.getEntityRegistry();

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    }

    if (executeSimpleQuery) {
      BoolQueryBuilder simplePerField = QueryBuilders.boolQuery();

      // Get base fields (V2.5: use compressed boost values)
      Set<SearchFieldConfig> baseFields =
          entitySpecs.stream()
              .map(spec -> getStandardFields(entityRegistry, spec, true))
              .flatMap(Set::stream)
              .collect(Collectors.toSet());

      Set<SearchFieldConfig> configuredFields =
          customizedQueryHandler.applySearchFieldConfiguration(
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

                // Conditionally apply fuzzy matching based on configuration
                String queryToUse;
                if (enableFuzzyMatchingDefault) {
                  // Add fuzzy operator (~) to query terms for typo tolerance
                  // ~2 allows up to 2 character edits (insertions, deletions, substitutions)
                  queryToUse = makeFuzzyQuery(sanitizedQuery);
                } else {
                  // Use exact terms without fuzzy matching
                  queryToUse = sanitizedQuery;
                }

                SimpleQueryStringBuilder simpleBuilder =
                    QueryBuilders.simpleQueryStringQuery(queryToUse);
                simpleBuilder.analyzer(analyzer);

                // Use OR operator to handle queries with garbage/non-existent terms
                // At least one term must match (minimumShouldMatch enforced by parent query)
                simpleBuilder.defaultOperator(Operator.OR);

                // Configure fuzzy matching parameters (only if enabled)
                if (enableFuzzyMatchingDefault) {
                  simpleBuilder.fuzzyPrefixLength(1); // First character must match
                  simpleBuilder.fuzzyMaxExpansions(50); // Cap expansions for performance
                  simpleBuilder.fuzzyTranspositions(true); // Allow character swaps (teh → the)
                }

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
      @Nonnull EntityRegistry entityRegistry,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query,
      @Nullable AspectRetriever aspectRetriever,
      boolean useV2_5) {

    final boolean isPrefixQuery =
        customQueryConfig == null
            ? exactMatchConfiguration.isWithPrefix()
            : customQueryConfig.isPrefixMatchQuery();
    final boolean isExactQuery = customQueryConfig == null || customQueryConfig.isExactMatchQuery();

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    String unquotedQuery = unquote(query);

    getStandardFields(entityRegistry, entitySpecs, useV2_5)
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
                                  searchFieldConfig.fieldName(), false, aspectRetriever),
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
                                searchFieldConfig.fieldName(), false, aspectRetriever),
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
                                searchFieldConfig.fieldName(), false, aspectRetriever),
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
      @Nonnull EntityRegistry entityRegistry,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery,
      boolean useV2_5) {
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
      getStandardFields(entityRegistry, entitySpecs, useV2_5)
          .forEach(entitySpec -> queryBuilder.field(entitySpec.fieldName(), entitySpec.boost()));
      result = Optional.of(queryBuilder);
    }
    return result;
  }

  static FunctionScoreQueryBuilder buildScoreFunctions(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query,
      @Nonnull QueryBuilder queryBuilder) {

    // Priority 1: Check for UI-provided function score override (for debugging)
    if (opContext.getSearchContext() != null
        && opContext.getSearchContext().getSearchFlags() != null
        && opContext.getSearchContext().getSearchFlags().hasFunctionScoreOverride()) {
      String overrideJson =
          opContext.getSearchContext().getSearchFlags().getFunctionScoreOverride();
      log.info("Using function score override from SearchFlags for debugging: {}", overrideJson);

      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> functionScoreMap =
            opContext.getObjectMapper().readValue(overrideJson, Map.class);

        return CustomizedQueryHandler.toFunctionScoreQueryBuilder(
            opContext.getObjectMapper(), queryBuilder, functionScoreMap, query);
      } catch (Exception e) {
        log.error(
            "Failed to parse functionScoreOverride, falling back to config: {}", overrideJson, e);
        // Fall through to YAML/PDL config
      }
    }

    // Priority 2: Check for YAML configuration (production)
    if (customQueryConfig != null) {
      // Prefer configuration function scoring over annotation scoring
      return CustomizedQueryHandler.functionScoreQueryBuilder(
          opContext.getObjectMapper(), customQueryConfig, queryBuilder, query);
    }

    // Priority 3: Use PDL annotations (legacy fallback)
    return QueryBuilders.functionScoreQuery(
            queryBuilder, buildAnnotationScoreFunctions(entitySpecs))
        .scoreMode(FunctionScoreQuery.ScoreMode.AVG) // Average score functions
        .boostMode(CombineFunction.MULTIPLY); // Multiply score function with the score from query
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

  /**
   * Resolves whether Search V2.5 is enabled for the current request. Priority: per-request
   * SearchFlags > server configuration
   *
   * @param opContext operation context containing search flags
   * @return true if V2.5 features should be used, false for V2 (main branch behavior)
   */
  private boolean resolveSearchV2_5Enabled(@Nonnull OperationContext opContext) {
    // Check per-request override first
    if (opContext.getSearchContext() != null
        && opContext.getSearchContext().getSearchFlags() != null
        && opContext.getSearchContext().getSearchFlags().hasSearchVersionV2_5()) {
      return opContext.getSearchContext().getSearchFlags().isSearchVersionV2_5();
    }

    // Fall back to server configuration
    return enableSearchV2_5Default;
  }
}
