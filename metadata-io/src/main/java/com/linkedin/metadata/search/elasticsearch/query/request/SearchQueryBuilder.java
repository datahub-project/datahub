package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.linkedin.metadata.search.utils.ESUtils;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;

import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;


public class SearchQueryBuilder {

  public static final String STRUCTURED_QUERY_PREFIX = "\\\\/q ";
  private final ExactMatchConfiguration exactMatchConfiguration;
  private final PartialConfiguration partialConfiguration;

  private final CustomizedQueryHandler customizedQueryHandler;

  public SearchQueryBuilder(@Nonnull SearchConfiguration searchConfiguration,
                            @Nullable CustomSearchConfiguration customSearchConfiguration) {
    this.exactMatchConfiguration = searchConfiguration.getExactMatch();
    this.partialConfiguration = searchConfiguration.getPartial();
    this.customizedQueryHandler = CustomizedQueryHandler.builder(customSearchConfiguration).build();
  }

  public QueryBuilder buildQuery(@Nonnull List<EntitySpec> entitySpecs, @Nonnull String query, boolean fulltext) {
    QueryConfiguration customQueryConfig = customizedQueryHandler.lookupQueryConfig(query).orElse(null);

    final QueryBuilder queryBuilder = buildInternalQuery(customQueryConfig, entitySpecs, query, fulltext);
    return buildScoreFunctions(customQueryConfig, entitySpecs, queryBuilder);
  }

  /**
   * Constructs the search query.
   * @param customQueryConfig custom configuration
   * @param entitySpecs entities being searched
   * @param query search string
   * @param fulltext use fulltext queries
   * @return query builder
   */
  private QueryBuilder buildInternalQuery(@Nullable QueryConfiguration customQueryConfig, @Nonnull List<EntitySpec> entitySpecs,
                                          @Nonnull String query, boolean fulltext) {
    final String sanitizedQuery = query.replaceFirst("^:+", "");
    final BoolQueryBuilder finalQuery = Optional.ofNullable(customQueryConfig)
            .flatMap(cqc -> cqc.boolQueryBuilder(sanitizedQuery))
            .orElse(QueryBuilders.boolQuery());

    if (fulltext && !query.startsWith(STRUCTURED_QUERY_PREFIX)) {
      getSimpleQuery(customQueryConfig, entitySpecs, sanitizedQuery).ifPresent(finalQuery::should);
      getPrefixAndExactMatchQuery(customQueryConfig, entitySpecs, sanitizedQuery).ifPresent(finalQuery::should);
    } else {
      final String withoutQueryPrefix = query.startsWith(STRUCTURED_QUERY_PREFIX) ? query.substring(STRUCTURED_QUERY_PREFIX.length()) : query;

      QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(withoutQueryPrefix);
      queryBuilder.defaultOperator(Operator.AND);
      entitySpecs.stream()
          .map(this::getStandardFields)
          .flatMap(Set::stream)
          .distinct()
          .forEach(cfg -> queryBuilder.field(cfg.fieldName(), cfg.boost()));
      finalQuery.should(queryBuilder);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQuery(null, entitySpecs, withoutQueryPrefix).ifPresent(finalQuery::should);
      }
    }

    return finalQuery;
  }

  private Set<SearchFieldConfig> getStandardFields(@Nonnull EntitySpec entitySpec) {
    Set<SearchFieldConfig> fields = new HashSet<>();

    // Always present
    final float urnBoost = Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"));

    fields.add(SearchFieldConfig.detectSubFieldType("urn", urnBoost, SearchableAnnotation.FieldType.URN, true));
    fields.add(SearchFieldConfig.detectSubFieldType("urn.delimited", urnBoost * partialConfiguration.getUrnFactor(),
            SearchableAnnotation.FieldType.URN, true));

    List<SearchableFieldSpec> searchableFieldSpecs = entitySpec.getSearchableFieldSpecs();
    for (SearchableFieldSpec fieldSpec : searchableFieldSpecs) {
      if (!fieldSpec.getSearchableAnnotation().isQueryByDefault()) {
        continue;
      }

      SearchFieldConfig searchFieldConfig = SearchFieldConfig.detectSubFieldType(fieldSpec);
      fields.add(searchFieldConfig);

      if (SearchFieldConfig.detectSubFieldType(fieldSpec).hasDelimitedSubfield()) {
        final SearchableAnnotation searchableAnnotation = fieldSpec.getSearchableAnnotation();

        fields.add(SearchFieldConfig.detectSubFieldType(searchFieldConfig.fieldName() + ".delimited",
                searchFieldConfig.boost() * partialConfiguration.getFactor(),
                searchableAnnotation.getFieldType(), searchableAnnotation.isQueryByDefault()));
      }
    }

    return fields;
  }

  private static String unquote(String query) {
    return query.replaceAll("[\"']", "");
  }

  private static boolean isQuoted(String query) {
    return Stream.of("\"", "'").anyMatch(query::contains);
  }

  private Optional<QueryBuilder> getSimpleQuery(@Nullable QueryConfiguration customQueryConfig,
                                                List<EntitySpec> entitySpecs,
                                                String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !isQuoted(sanitizedQuery) || !exactMatchConfiguration.isExclusive();
    }

    if (executeSimpleQuery) {
      BoolQueryBuilder simplePerField = QueryBuilders.boolQuery();
      // Simple query string does not use per field analyzers
      // Group the fields by analyzer
      Map<String, List<SearchFieldConfig>> analyzerGroup = entitySpecs.stream()
              .map(this::getStandardFields)
              .flatMap(Set::stream)
              .filter(SearchFieldConfig::isQueryByDefault)
              .collect(Collectors.groupingBy(SearchFieldConfig::analyzer));

      analyzerGroup.keySet().stream().sorted().forEach(analyzer -> {
        List<SearchFieldConfig> fieldConfigs = analyzerGroup.get(analyzer);
        SimpleQueryStringBuilder simpleBuilder = QueryBuilders.simpleQueryStringQuery(sanitizedQuery);
        simpleBuilder.analyzer(analyzer);
        simpleBuilder.defaultOperator(Operator.AND);
        fieldConfigs.forEach(cfg -> simpleBuilder.field(cfg.fieldName(), cfg.boost()));
        simplePerField.should(simpleBuilder);
      });

      result = Optional.of(simplePerField);
    }

    return result;
  }

  private Optional<QueryBuilder> getPrefixAndExactMatchQuery(@Nullable QueryConfiguration customQueryConfig,
                                                             @Nonnull List<EntitySpec> entitySpecs,
                                                             String query) {

    final boolean isPrefixQuery = customQueryConfig == null ? exactMatchConfiguration.isWithPrefix() : customQueryConfig.isPrefixMatchQuery();
    final boolean isExactQuery = customQueryConfig == null || customQueryConfig.isExactMatchQuery();

    BoolQueryBuilder finalQuery =  QueryBuilders.boolQuery();
    String unquotedQuery = unquote(query);

    entitySpecs.stream()
            .map(this::getStandardFields)
            .flatMap(Set::stream)
            .filter(SearchFieldConfig::isQueryByDefault)
            .forEach(searchFieldConfig -> {

              if (searchFieldConfig.isDelimitedSubfield() && isPrefixQuery) {
                finalQuery.should(QueryBuilders.matchPhrasePrefixQuery(searchFieldConfig.fieldName(), query)
                        .boost(searchFieldConfig.boost()
                                * exactMatchConfiguration.getPrefixFactor()
                                * exactMatchConfiguration.getCaseSensitivityFactor())
                        .queryName(searchFieldConfig.shortName())); // less than exact
              }

              if (searchFieldConfig.isKeyword() && isExactQuery) {
                // It is important to use the subfield .keyword (it uses a different normalizer)
                // The non-.keyword field removes case information

                // Exact match case-sensitive
                finalQuery.should(QueryBuilders
                        .termQuery(ESUtils.toKeywordField(searchFieldConfig.fieldName(), false), unquotedQuery)
                        .caseInsensitive(false)
                        .boost(searchFieldConfig.boost()
                                * exactMatchConfiguration.getExactFactor())
                        .queryName(searchFieldConfig.shortName()));

                // Exact match case-insensitive
                finalQuery.should(QueryBuilders
                        .termQuery(ESUtils.toKeywordField(searchFieldConfig.fieldName(), false), unquotedQuery)
                        .caseInsensitive(true)
                        .boost(searchFieldConfig.boost()
                                * exactMatchConfiguration.getExactFactor()
                                * exactMatchConfiguration.getCaseSensitivityFactor())
                        .queryName(searchFieldConfig.fieldName()));
              }
            });

    return finalQuery.should().size() > 0 ? Optional.of(finalQuery) : Optional.empty();
  }

  private FunctionScoreQueryBuilder buildScoreFunctions(@Nullable QueryConfiguration customQueryConfig,
                                                        @Nonnull List<EntitySpec> entitySpecs,
                                                        @Nonnull QueryBuilder queryBuilder) {

    if (customQueryConfig != null) {
      // Prefer configuration function scoring over annotation scoring
      return customQueryConfig.functionScoreQueryBuilder(queryBuilder);
    } else {
      return QueryBuilders.functionScoreQuery(queryBuilder, buildAnnotationScoreFunctions(entitySpecs))
              .scoreMode(FunctionScoreQuery.ScoreMode.AVG) // Average score functions
              .boostMode(CombineFunction.MULTIPLY); // Multiply score function with the score from query;
    }
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder[] buildAnnotationScoreFunctions(@Nonnull List<EntitySpec> entitySpecs) {
    List<FunctionScoreQueryBuilder.FilterFunctionBuilder> finalScoreFunctions = new ArrayList<>();

    // Add a default weight of 1.0 to make sure the score function is larger than 1
    finalScoreFunctions.add(
            new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.weightFactorFunction(1.0f)));

    entitySpecs.stream()
        .map(EntitySpec::getSearchableFieldSpecs)
        .flatMap(List::stream)
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .flatMap(annotation -> annotation
            .getWeightsPerFieldValue()
            .entrySet()
            .stream()
            .map(entry -> buildWeightFactorFunction(annotation.getFieldName(), entry.getKey(),
                entry.getValue())))
        .forEach(finalScoreFunctions::add);

    entitySpecs.stream()
        .map(EntitySpec::getSearchScoreFieldSpecs)
        .flatMap(List::stream)
        .map(fieldSpec -> buildScoreFunctionFromSearchScoreAnnotation(fieldSpec.getSearchScoreAnnotation()))
        .forEach(finalScoreFunctions::add);

    return finalScoreFunctions.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[0]);
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder buildWeightFactorFunction(@Nonnull String fieldName,
      @Nonnull Object fieldValue, double weight) {
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery(fieldName, fieldValue),
        ScoreFunctionBuilders.weightFactorFunction((float) weight));
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder buildScoreFunctionFromSearchScoreAnnotation(
      @Nonnull SearchScoreAnnotation annotation) {
    FieldValueFactorFunctionBuilder scoreFunction =
        ScoreFunctionBuilders.fieldValueFactorFunction(annotation.getFieldName());
    scoreFunction.factor((float) annotation.getWeight());
    scoreFunction.missing(annotation.getDefaultValue());
    annotation.getModifier().ifPresent(modifier -> scoreFunction.modifier(mapModifier(modifier)));
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(scoreFunction);
  }

  private static FieldValueFactorFunction.Modifier mapModifier(SearchScoreAnnotation.Modifier modifier) {
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
}
