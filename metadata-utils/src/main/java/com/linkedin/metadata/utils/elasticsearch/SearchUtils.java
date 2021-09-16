package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


@Slf4j
public class SearchUtils {

  private SearchUtils() {

  }

  /**
   * Builds search query given a {@link Criterion}, containing field, value and association/condition between the two.
   *
   * <p>If the condition between a field and value (specified in {@link Criterion}) is EQUAL, we construct a Terms query.
   * In this case, a field can take multiple values, specified using comma as a delimiter - this method will split
   * tokens accordingly. This is done because currently there is no support of associating two different {@link Criterion}
   * in a {@link Filter} with an OR operator - default operator is AND.
   *
   * <p>This approach of supporting multiple values using comma as delimiter, prevents us from specifying a value that has comma
   * as one of it's characters. This is particularly true when one of the values is an urn e.g. "urn:li:example:(1,2,3)".
   * Hence we do not split the value (using comma as delimiter) if the value starts with "urn:li:".
   * TODO(https://github.com/linkedin/datahub-gma/issues/51): support multiple values a field can take without using delimiters like comma.
   *
   * <p>If the condition between a field and value is not the same as EQUAL, a Range query is constructed. This
   * condition does not support multiple values for the same field.
   *
   * <p>When CONTAIN, START_WITH and END_WITH conditions are used, the underlying logic is using wildcard query which is
   * not performant according to ES. For details, please refer to:
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html#wildcard-query-field-params
   *
   * @param criterion {@link Criterion} single criterion which contains field, value and a comparison operator
   */
  @Nonnull
  public static QueryBuilder getQueryBuilderFromCriterion(@Nonnull Criterion criterion) {
    final Condition condition = criterion.getCondition();
    if (condition == Condition.EQUAL) {
      if (criterion.getValue().startsWith("urn:li:")) {
        return QueryBuilders.termsQuery(criterion.getField(), criterion.getValue().trim());
      }
      return QueryBuilders.termsQuery(criterion.getField(), criterion.getValue().trim().split("\\s*,\\s*"));
    } else if (condition == Condition.GREATER_THAN) {
      return QueryBuilders.rangeQuery(criterion.getField()).gt(criterion.getValue().trim());
    } else if (condition == Condition.GREATER_THAN_OR_EQUAL_TO) {
      return QueryBuilders.rangeQuery(criterion.getField()).gte(criterion.getValue().trim());
    } else if (condition == Condition.LESS_THAN) {
      return QueryBuilders.rangeQuery(criterion.getField()).lt(criterion.getValue().trim());
    } else if (condition == Condition.LESS_THAN_OR_EQUAL_TO) {
      return QueryBuilders.rangeQuery(criterion.getField()).lte(criterion.getValue().trim());
    } else if (condition == Condition.CONTAIN) {
      return QueryBuilders.wildcardQuery(criterion.getField(),
          "*" + ESUtils.escapeReservedCharacters(criterion.getValue().trim()) + "*");
    } else if (condition == Condition.START_WITH) {
      return QueryBuilders.wildcardQuery(criterion.getField(),
          ESUtils.escapeReservedCharacters(criterion.getValue().trim()) + "*");
    } else if (condition == Condition.END_WITH) {
      return QueryBuilders.wildcardQuery(criterion.getField(),
          "*" + ESUtils.escapeReservedCharacters(criterion.getValue().trim()));
    }

    throw new UnsupportedOperationException("Unsupported condition: " + condition);
  }
}