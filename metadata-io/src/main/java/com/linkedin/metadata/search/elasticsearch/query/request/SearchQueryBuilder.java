package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.*;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.BoolQueryConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.utils.ESUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.search.SearchModule;

@Slf4j
public class SearchQueryBuilder {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(
                    Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH,
                    Constants.MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private static final NamedXContentRegistry X_CONTENT_REGISTRY;

  static {
    SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
    X_CONTENT_REGISTRY = new NamedXContentRegistry(searchModule.getNamedXContents());
  }

  public static final String STRUCTURED_QUERY_PREFIX = "\\\\/q ";
  private final ExactMatchConfiguration exactMatchConfiguration;
  private final PartialConfiguration partialConfiguration;
  private final WordGramConfiguration wordGramConfiguration;

  private final CustomizedQueryHandler customizedQueryHandler;

  public SearchQueryBuilder(
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    this.exactMatchConfiguration = searchConfiguration.getExactMatch();
    this.partialConfiguration = searchConfiguration.getPartial();
    this.wordGramConfiguration = searchConfiguration.getWordGram();
    this.customizedQueryHandler = CustomizedQueryHandler.builder(customSearchConfiguration).build();
  }

  public QueryBuilder buildQuery(
      @Nonnull List<EntitySpec> entitySpecs, @Nonnull String query, boolean fulltext) {
    QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(query).orElse(null);

    final QueryBuilder queryBuilder =
        buildInternalQuery(customQueryConfig, entitySpecs, query, fulltext);
    return buildScoreFunctions(customQueryConfig, entitySpecs, queryBuilder);
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
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    final String sanitizedQuery = query.replaceFirst("^:+", "");
    final BoolQueryBuilder finalQuery =
        Optional.ofNullable(customQueryConfig)
            .flatMap(cqc -> boolQueryBuilder(cqc, sanitizedQuery))
            .orElse(QueryBuilders.boolQuery());

    if (fulltext && !query.startsWith(STRUCTURED_QUERY_PREFIX)) {
      getSimpleQuery(customQueryConfig, entitySpecs, sanitizedQuery).ifPresent(finalQuery::should);
      getPrefixAndExactMatchQuery(customQueryConfig, entitySpecs, sanitizedQuery)
          .ifPresent(finalQuery::should);
    } else {
      final String withoutQueryPrefix =
          query.startsWith(STRUCTURED_QUERY_PREFIX)
              ? query.substring(STRUCTURED_QUERY_PREFIX.length())
              : query;

      QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(withoutQueryPrefix);
      queryBuilder.defaultOperator(Operator.AND);
      getStandardFields(entitySpecs)
          .forEach(entitySpec -> queryBuilder.field(entitySpec.fieldName(), entitySpec.boost()));
      finalQuery.should(queryBuilder);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQuery(null, entitySpecs, withoutQueryPrefix)
            .ifPresent(finalQuery::should);
      }
    }

    return finalQuery;
  }

  /**
   * Gets searchable fields from all entities in the input collection. De-duplicates fields across
   * entities.
   *
   * @param entitySpecs: Entity specs to extract searchable fields from
   * @return A set of SearchFieldConfigs containing the searchable fields from the input entities.
   */
  @VisibleForTesting
  public Set<SearchFieldConfig> getStandardFields(@Nonnull Collection<EntitySpec> entitySpecs) {
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
        .map(this::getFieldsFromEntitySpec)
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

  @VisibleForTesting
  public Set<SearchFieldConfig> getFieldsFromEntitySpec(EntitySpec entitySpec) {
    Set<SearchFieldConfig> fields = new HashSet<>();
    List<SearchableFieldSpec> searchableFieldSpecs = entitySpec.getSearchableFieldSpecs();
    for (SearchableFieldSpec fieldSpec : searchableFieldSpecs) {
      if (!fieldSpec.getSearchableAnnotation().isQueryByDefault()) {
        continue;
      }

      SearchFieldConfig searchFieldConfig = SearchFieldConfig.detectSubFieldType(fieldSpec);
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
      }
    }
    return fields;
  }

  private Set<SearchFieldConfig> getStandardFields(@Nonnull EntitySpec entitySpec) {
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

    fields.addAll(getFieldsFromEntitySpec(entitySpec));

    return fields;
  }

  private static String unquote(String query) {
    return query.replaceAll("[\"']", "");
  }

  private static boolean isQuoted(String query) {
    return Stream.of("\"", "'").anyMatch(query::contains);
  }

  private Optional<QueryBuilder> getSimpleQuery(
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
      /*
       * NOTE: This logic applies the queryByDefault annotations for each entity to ALL entities
       * If we ever have fields that are queryByDefault on some entities and not others, this section will need to be refactored
       * to apply an index filter AND the analyzers added here.
       */
      BoolQueryBuilder simplePerField = QueryBuilders.boolQuery();
      // Simple query string does not use per field analyzers
      // Group the fields by analyzer
      Map<String, List<SearchFieldConfig>> analyzerGroup =
          entitySpecs.stream()
              .map(this::getStandardFields)
              .flatMap(Set::stream)
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

      result = Optional.of(simplePerField);
    }

    return result;
  }

  private Optional<QueryBuilder> getPrefixAndExactMatchQuery(
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

    getStandardFields(entitySpecs)
        .forEach(
            searchFieldConfig -> {
              if (searchFieldConfig.isDelimitedSubfield() && isPrefixQuery) {
                finalQuery.should(
                    QueryBuilders.matchPhrasePrefixQuery(searchFieldConfig.fieldName(), query)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getPrefixFactor()
                                * exactMatchConfiguration.getCaseSensitivityFactor())
                        .queryName(searchFieldConfig.shortName())); // less than exact
              }

              if (searchFieldConfig.isKeyword() && isExactQuery) {
                // It is important to use the subfield .keyword (it uses a different normalizer)
                // The non-.keyword field removes case information

                // Exact match case-sensitive
                finalQuery.should(
                    QueryBuilders.termQuery(
                            ESUtils.toKeywordField(searchFieldConfig.fieldName(), false),
                            unquotedQuery)
                        .caseInsensitive(false)
                        .boost(searchFieldConfig.boost() * exactMatchConfiguration.getExactFactor())
                        .queryName(searchFieldConfig.shortName()));

                // Exact match case-insensitive
                finalQuery.should(
                    QueryBuilders.termQuery(
                            ESUtils.toKeywordField(searchFieldConfig.fieldName(), false),
                            unquotedQuery)
                        .caseInsensitive(true)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getExactFactor()
                                * exactMatchConfiguration.getCaseSensitivityFactor())
                        .queryName(searchFieldConfig.fieldName()));
              }

              if (searchFieldConfig.isWordGramSubfield() && isPrefixQuery) {
                finalQuery.should(
                    QueryBuilders.matchPhraseQuery(
                            ESUtils.toKeywordField(searchFieldConfig.fieldName(), false),
                            unquotedQuery)
                        .boost(
                            searchFieldConfig.boost()
                                * getWordGramFactor(searchFieldConfig.fieldName()))
                        .queryName(searchFieldConfig.shortName()));
              }
            });

    return finalQuery.should().size() > 0 ? Optional.of(finalQuery) : Optional.empty();
  }

  private FunctionScoreQueryBuilder buildScoreFunctions(
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull QueryBuilder queryBuilder) {

    if (customQueryConfig != null) {
      // Prefer configuration function scoring over annotation scoring
      return functionScoreQueryBuilder(customQueryConfig, queryBuilder);
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

  public FunctionScoreQueryBuilder functionScoreQueryBuilder(
      QueryConfiguration customQueryConfiguration, QueryBuilder queryBuilder) {
    return toFunctionScoreQueryBuilder(queryBuilder, customQueryConfiguration.getFunctionScore());
  }

  public Optional<BoolQueryBuilder> boolQueryBuilder(
      QueryConfiguration customQueryConfiguration, String query) {
    if (customQueryConfiguration.getBoolQuery() != null) {
      log.debug(
          "Using custom query configuration queryRegex: {}",
          customQueryConfiguration.getQueryRegex());
    }
    return Optional.ofNullable(customQueryConfiguration.getBoolQuery())
        .map(bq -> toBoolQueryBuilder(query, bq));
  }

  private BoolQueryBuilder toBoolQueryBuilder(String query, BoolQueryConfiguration boolQuery) {
    try {
      String jsonFragment =
          OBJECT_MAPPER
              .writeValueAsString(boolQuery)
              .replace("\"{{query_string}}\"", OBJECT_MAPPER.writeValueAsString(query))
              .replace(
                  "\"{{unquoted_query_string}}\"",
                  OBJECT_MAPPER.writeValueAsString(unquote(query)));
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, jsonFragment);
      return BoolQueryBuilder.fromXContent(parser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private FunctionScoreQueryBuilder toFunctionScoreQueryBuilder(
      QueryBuilder queryBuilder, Map<String, Object> params) {
    try {
      HashMap<String, Object> body = new HashMap<>(params);
      if (!body.isEmpty()) {
        log.debug("Using custom scoring functions: {}", body);
      }

      body.put("query", OBJECT_MAPPER.readValue(queryBuilder.toString(), Map.class));

      String jsonFragment = OBJECT_MAPPER.writeValueAsString(Map.of("function_score", body));
      XContentParser parser =
          XContentType.JSON
              .xContent()
              .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, jsonFragment);
      return (FunctionScoreQueryBuilder) FunctionScoreQueryBuilder.parseInnerQueryBuilder(parser);
    } catch (IOException e) {
      throw new RuntimeException(e);
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
}
