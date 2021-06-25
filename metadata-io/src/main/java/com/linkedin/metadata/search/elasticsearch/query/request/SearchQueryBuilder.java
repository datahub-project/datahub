package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;


public class SearchQueryBuilder {

  private static final String KEYWORD_LOWERCASE_ANALYZER = "custom_keyword";
  private static final String TEXT_ANALYZER = "word_delimited";

  private static final Set<FieldType> TYPES_WITH_DELIMITED_SUBFIELD =
      new HashSet<>(Arrays.asList(FieldType.TEXT, FieldType.TEXT_PARTIAL, FieldType.URN,
          FieldType.URN_PARTIAL));
  private static final Set<FieldType> TYPES_WITH_NGRAM_SUBFIELD =
      new HashSet<>(Arrays.asList(FieldType.TEXT_PARTIAL, FieldType.URN_PARTIAL));

  private SearchQueryBuilder() {
  }

  public static QueryBuilder buildQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query) {
    return QueryBuilders.functionScoreQuery(buildInternalQuery(entitySpec, query), buildFilterFunctions(entitySpec));
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

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder[] buildFilterFunctions(
      @Nonnull EntitySpec entitySpec) {
    return entitySpec.getSearchableFieldSpecs()
        .stream()
        .flatMap(fieldSpec -> fieldSpec.getSearchableAnnotation()
            .getWeightsPerFieldValue()
            .entrySet()
            .stream()
            .map(entry -> buildFilterFunction(fieldSpec.getSearchableAnnotation().getFieldName(), entry.getKey(),
                entry.getValue())))
        .toArray(FunctionScoreQueryBuilder.FilterFunctionBuilder[]::new);
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder buildFilterFunction(@Nonnull String fieldName,
      @Nonnull Object fieldValue, double weight) {
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery(fieldName, fieldValue),
        ScoreFunctionBuilders.weightFactorFunction((float) weight));
  }
}
