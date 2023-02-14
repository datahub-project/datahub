package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

import com.linkedin.util.Pair;
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
  private static final Set<FieldType> TYPES_WITH_DELIMITED_SUBFIELD =
      new HashSet<>(Arrays.asList(FieldType.TEXT, FieldType.TEXT_PARTIAL));

  private SearchQueryBuilder() {
  }

  public static QueryBuilder buildQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query, boolean fulltext) {
    final QueryBuilder queryBuilder = buildInternalQuery(entitySpec, query, fulltext);

    return QueryBuilders.functionScoreQuery(queryBuilder, buildScoreFunctions(entitySpec))
        .scoreMode(FunctionScoreQuery.ScoreMode.AVG) // Average score functions
        .boostMode(CombineFunction.MULTIPLY); // Multiply score function with the score from query
  }

  /**
   * Constructs the search query.
   * @param entitySpec entity being searched
   * @param query search string
   * @param fulltext use fulltext queries
   * @return query builder
   */
  private static QueryBuilder buildInternalQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query, boolean fulltext) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    if (fulltext) {
      SimpleQueryStringBuilder simpleBuilder = QueryBuilders.simpleQueryStringQuery(query.replaceFirst("^:+", ""));
      simpleBuilder.defaultOperator(Operator.AND);
      getStandardFields(entitySpec).forEach(fieldBoost -> simpleBuilder.field(fieldBoost.getFirst(), fieldBoost.getSecond()));
      finalQuery.should(simpleBuilder);
    } else {
      QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(query);
      queryBuilder.defaultOperator(Operator.AND);
      getStandardFields(entitySpec).forEach(fieldBoost -> queryBuilder.field(fieldBoost.getFirst(), fieldBoost.getSecond()));
      finalQuery.should(queryBuilder);
    }

    // common prefix query
    getPrefixQuery(entitySpec, query).ifPresent(finalQuery::should);

    return finalQuery;
  }

  private static Set<Pair<String, Float>> getStandardFields(@Nonnull EntitySpec entitySpec) {
    Set<Pair<String, Float>> fields = new HashSet<>();

    // Always present
    final float urnBoost = Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"));
    List.of("urn", "urn.delimited").forEach(urnField -> fields.add(Pair.of(urnField, urnBoost)));

    List<SearchableFieldSpec> searchableFieldSpecs = entitySpec.getSearchableFieldSpecs();
    for (SearchableFieldSpec fieldSpec : searchableFieldSpecs) {
      if (!fieldSpec.getSearchableAnnotation().isQueryByDefault()) {
        continue;
      }

      String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
      double boostScore = fieldSpec.getSearchableAnnotation().getBoostScore();
      fields.add(Pair.of(fieldName, (float) (boostScore)));

      FieldType fieldType = fieldSpec.getSearchableAnnotation().getFieldType();
      if (TYPES_WITH_DELIMITED_SUBFIELD.contains(fieldType)) {
        fields.add(Pair.of(fieldName + ".delimited", (float) (boostScore * 0.4)));
      }
      if (FieldType.URN_PARTIAL.equals(fieldType)) {
        fields.add(Pair.of(fieldName + ".delimited", (float) (boostScore * 0.4)));
      }
    }

    return fields;
  }

  private static Optional<QueryBuilder> getPrefixQuery(@Nonnull EntitySpec entitySpec, String query) {
    BoolQueryBuilder finalQuery =  QueryBuilders.boolQuery();
    entitySpec.getSearchableFieldSpecs().stream()
            .map(SearchableFieldSpec::getSearchableAnnotation)
            .filter(SearchableAnnotation::isQueryByDefault)
            .filter(SearchableAnnotation::isEnableAutocomplete)
            .filter(e -> TYPES_WITH_DELIMITED_SUBFIELD.contains(e.getFieldType()))
            .forEach(fieldSpec -> finalQuery.should(
                    QueryBuilders.matchPhrasePrefixQuery(fieldSpec.getFieldName() + ".delimited", query)
                            .boost((float) fieldSpec.getBoostScore())));
    return finalQuery.should().size() > 0 ? Optional.of(finalQuery) : Optional.empty();
  }

  private static QueryBuilder getPhraseQuery(@Nonnull EntitySpec entitySpec, String query) {
    BoolQueryBuilder finalQuery =  QueryBuilders.boolQuery();
    getStandardFields(entitySpec).stream()
            .filter(p -> p.getFirst().endsWith(".delimited"))
            .forEach(p -> finalQuery.should(QueryBuilders.matchPhraseQuery(p.getFirst(), query).boost(p.getSecond())));
    return finalQuery;
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder[] buildScoreFunctions(@Nonnull EntitySpec entitySpec) {
    List<FunctionScoreQueryBuilder.FilterFunctionBuilder> finalScoreFunctions = new ArrayList<>();
    // Add a default weight of 1.0 to make sure the score function is larger than 1
    finalScoreFunctions.add(
        new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.weightFactorFunction(1.0f)));
    entitySpec.getSearchableFieldSpecs()
        .stream()
        .flatMap(fieldSpec -> fieldSpec.getSearchableAnnotation()
            .getWeightsPerFieldValue()
            .entrySet()
            .stream()
            .map(entry -> buildWeightFactorFunction(fieldSpec.getSearchableAnnotation().getFieldName(), entry.getKey(),
                entry.getValue())))
        .forEach(finalScoreFunctions::add);

    entitySpec.getSearchScoreFieldSpecs()
        .stream()
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
