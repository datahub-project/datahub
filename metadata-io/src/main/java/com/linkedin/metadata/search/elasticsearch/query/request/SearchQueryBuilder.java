package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;


public class SearchQueryBuilder {

  private static final String KEYWORD_LOWERCASE_ANALYZER = "custom_keyword";
  private static final String TEXT_ANALYZER = "word_delimited";

  private static final Set<FieldType> TYPES_WITH_DELIMITED_SUBFIELD =
      new HashSet<>(Arrays.asList(FieldType.TEXT, FieldType.TEXT_PARTIAL));
  private static final Set<FieldType> TYPES_WITH_NGRAM_SUBFIELD =
      new HashSet<>(Arrays.asList(FieldType.TEXT_PARTIAL, FieldType.URN_PARTIAL));

  private SearchQueryBuilder() {
  }

  public static QueryBuilder buildQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query) {
    return QueryBuilders.functionScoreQuery(buildInternalQuery(entitySpec, query), buildScoreFunctions(entitySpec))
        .scoreMode(FunctionScoreQuery.ScoreMode.AVG) // Average score functions
        .boostMode(CombineFunction.MULTIPLY); // Multiply score function with the score from query
  }

  private static QueryBuilder buildInternalQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    // Key word lowercase queries do case agnostic exact matching between document value and query
    QueryStringQueryBuilder keywordLowercaseQuery = QueryBuilders.queryStringQuery(query);
    keywordLowercaseQuery.analyzer(KEYWORD_LOWERCASE_ANALYZER);
    keywordLowercaseQuery.defaultOperator(Operator.AND);
    // Text queries tokenize input query and document value into words before checking for matches
    QueryStringQueryBuilder textQuery = QueryBuilders.queryStringQuery(query);
    textQuery.analyzer(TEXT_ANALYZER);
    textQuery.defaultOperator(Operator.AND);

    for (SearchableFieldSpec fieldSpec : entitySpec.getSearchableFieldSpecs()) {
      if (!fieldSpec.getSearchableAnnotation().isQueryByDefault()) {
        continue;
      }

      String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
      double boostScore = fieldSpec.getSearchableAnnotation().getBoostScore();
      keywordLowercaseQuery.field(fieldName, (float) (boostScore));

      FieldType fieldType = fieldSpec.getSearchableAnnotation().getFieldType();
      if (TYPES_WITH_DELIMITED_SUBFIELD.contains(fieldType)) {
        textQuery.field(fieldName + ".delimited", (float) (boostScore * 0.4));
      }
      if (TYPES_WITH_NGRAM_SUBFIELD.contains(fieldType)) {
        textQuery.field(fieldName + ".ngram", (float) (boostScore * 0.1));
      }
    }

    // Only add the queries in if corresponding fields exist
    if (!keywordLowercaseQuery.fields().isEmpty()) {
      finalQuery.should(keywordLowercaseQuery);
    }
    if (!textQuery.fields().isEmpty()) {
      finalQuery.should(textQuery);
    }
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
