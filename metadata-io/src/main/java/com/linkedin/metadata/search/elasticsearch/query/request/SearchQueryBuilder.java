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
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;

import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.KEYWORD_LOWERCASE_ANALYZER;


public class SearchQueryBuilder {
  private static final Set<FieldType> TYPES_WITH_DELIMITED_SUBFIELD =
      new HashSet<>(Arrays.asList(FieldType.TEXT, FieldType.TEXT_PARTIAL));
  private static final Set<FieldType> TYPES_WITH_NGRAM_SUBFIELD =
      new HashSet<>(List.of(FieldType.TEXT_PARTIAL));

  private SearchQueryBuilder() {
  }

  public static QueryBuilder buildQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query, boolean structured) {
    final QueryBuilder queryBuilder;
    if (structured) {
      queryBuilder = buildInternalQuery(entitySpec, query, false, true);
    } else {
      queryBuilder = buildInternalQuery(entitySpec, query, true, false);
    }

    return QueryBuilders.functionScoreQuery(queryBuilder, buildScoreFunctions(entitySpec))
        .scoreMode(FunctionScoreQuery.ScoreMode.AVG) // Average score functions
        .boostMode(CombineFunction.MULTIPLY); // Multiply score function with the score from query
  }

  /**
   * Constructs the search query.
   * @param entitySpec entity being searched
   * @param query search string
   * @param safeMode should be used for user provided search strings. Either escapes them or executes them
   *                 with simple_query_search which will suppress syntax errors.
   * @param exactMatch override all analyzer with `custom_keyword` matching. Case-insensitive!
   * @return query builder
   */
  private static QueryBuilder buildInternalQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query, boolean safeMode,
                                                 boolean exactMatch) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    SimpleQueryStringBuilder simpleBuilder = QueryBuilders.simpleQueryStringQuery(query);
    simpleBuilder.defaultOperator(Operator.AND);

    QueryStringQueryBuilder escapedBuilder = QueryBuilders.queryStringQuery(query);
    escapedBuilder.defaultOperator(Operator.AND);
    escapedBuilder.escape(safeMode);

    if (exactMatch) {
      simpleBuilder.analyzer(KEYWORD_LOWERCASE_ANALYZER);
      escapedBuilder.analyzer(KEYWORD_LOWERCASE_ANALYZER);
    }

    // Always present
    List.of("urn", "urn.delimited", "urn.ngram").forEach(urnField -> {
      simpleBuilder.field(urnField, Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore")));
      escapedBuilder.field(urnField, Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore")));
    });

    List<SearchableFieldSpec> searchableFieldSpecs = entitySpec.getSearchableFieldSpecs();
    for (SearchableFieldSpec fieldSpec : searchableFieldSpecs) {
      if (!fieldSpec.getSearchableAnnotation().isQueryByDefault()) {
        continue;
      }

      String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
      double boostScore = fieldSpec.getSearchableAnnotation().getBoostScore();
      simpleBuilder.field(fieldName, (float) (boostScore));
      escapedBuilder.field(fieldName, (float) (boostScore));

      FieldType fieldType = fieldSpec.getSearchableAnnotation().getFieldType();
      if (TYPES_WITH_DELIMITED_SUBFIELD.contains(fieldType)) {
        simpleBuilder.field(fieldName + ".delimited", (float) (boostScore * 0.4));
        escapedBuilder.field(fieldName + ".delimited", (float) (boostScore * 0.4));
      }
      if (FieldType.URN_PARTIAL.equals(fieldType)) {
        simpleBuilder.field(fieldName + ".delimited", (float) (boostScore * 0.4));
        escapedBuilder.field(fieldName + ".delimited", (float) (boostScore * 0.4));
      } else if (TYPES_WITH_NGRAM_SUBFIELD.contains(fieldType) || fieldSpec.getSearchableAnnotation().isEnableAutocomplete()) {
        simpleBuilder.field(fieldName + ".ngram", (float) (boostScore * 0.1));
        escapedBuilder.field(fieldName + ".ngram", (float) (boostScore * 0.1));
      }
    }

    if (safeMode) {
      finalQuery.should(simpleBuilder);
    }
    finalQuery.should(escapedBuilder);

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
