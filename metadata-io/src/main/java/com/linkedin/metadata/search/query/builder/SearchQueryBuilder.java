package com.linkedin.metadata.search.query.builder;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexSetting;
import java.util.Optional;
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

  private SearchQueryBuilder() {
  }

  public static QueryBuilder buildQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query) {
    return QueryBuilders.functionScoreQuery(buildInternalQuery(entitySpec, query), buildFilterFunctions(entitySpec));
  }

  private static QueryBuilder buildInternalQuery(@Nonnull EntitySpec entitySpec, @Nonnull String query) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    // Key word queries do exact matching between document value and query
    QueryStringQueryBuilder keywordQuery = QueryBuilders.queryStringQuery(query);
    keywordQuery.defaultOperator(Operator.AND);
    // Key word lowercase queries do case agnostic exact matching between document value and query
    QueryStringQueryBuilder keywordLowercaseQuery = QueryBuilders.queryStringQuery(query);
    keywordLowercaseQuery.analyzer(KEYWORD_LOWERCASE_ANALYZER);
    keywordLowercaseQuery.defaultOperator(Operator.AND);
    // Text queries tokenize input query and document value into words before checking for matches
    QueryStringQueryBuilder textQuery = QueryBuilders.queryStringQuery(query);
    textQuery.analyzer(TEXT_ANALYZER);
    textQuery.defaultOperator(Operator.AND);

    for (SearchableFieldSpec fieldSpec : entitySpec.getSearchableFieldSpecs()) {
      boolean isFirst = true;
      for (IndexSetting setting : fieldSpec.getIndexSettings()) {
        if (setting.isAddToDefaultQuery()) {
          // First setting uses the original field name unless overridden
          String fieldName = isFirst ? fieldSpec.getFieldName()
              : fieldSpec.getFieldName() + "." + SearchableAnnotation.SUBFIELD_BY_TYPE.get(setting.getIndexType());
          // Override name if the overrideFieldName is set
          String finalFieldName = setting.getOverrideFieldName().orElse(fieldName);
          switch (setting.getIndexType()) {
            case BOOLEAN:
            case COUNT:
            case KEYWORD:
              addField(keywordQuery, finalFieldName, setting.getBoostScore());
              break;
            case KEYWORD_LOWERCASE:
              addField(keywordLowercaseQuery, finalFieldName, setting.getBoostScore());
              break;
            default:
              addField(textQuery, finalFieldName, setting.getBoostScore());
              break;
          }
        }
        isFirst = false;
      }
    }

    // Only add the queries in if corresponding fields exist
    if (!keywordQuery.fields().isEmpty()) {
      finalQuery.should(keywordQuery);
    }
    if (!keywordLowercaseQuery.fields().isEmpty()) {
      finalQuery.should(keywordLowercaseQuery);
    }
    if (!textQuery.fields().isEmpty()) {
      finalQuery.should(textQuery);
    }
    return finalQuery;
  }

  private static void addField(@Nonnull QueryStringQueryBuilder queryBuilder, @Nonnull String fieldName,
      Optional<Double> boostScore) {
    if (boostScore.isPresent()) {
      queryBuilder.field(fieldName, boostScore.get().floatValue());
    } else {
      queryBuilder.field(fieldName);
    }
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder[] buildFilterFunctions(
      @Nonnull EntitySpec entitySpec) {
    return entitySpec.getSearchableFieldSpecs()
        .stream()
        .flatMap(fieldSpec -> fieldSpec.getWeightsPerFieldValue()
            .entrySet()
            .stream()
            .map(entry -> buildFilterFunction(fieldSpec.getFieldName(), entry.getKey(), entry.getValue())))
        .toArray(FunctionScoreQueryBuilder.FilterFunctionBuilder[]::new);
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder buildFilterFunction(@Nonnull String fieldName,
      @Nonnull Object fieldValue, double weight) {
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery(fieldName, fieldValue),
        ScoreFunctionBuilders.weightFactorFunction((float) weight));
  }
}
