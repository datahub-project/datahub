package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


@Slf4j
public class SearchUtils {

  private SearchUtils() {

  }

  /**
   * Validates the request params and create a request map out of it.
   *
   * @param requestParams the search request with fields and values
   * @return a request map
   */
  @Nonnull
  public static Map<String, String> getRequestMap(@Nullable Filter requestParams) {
    if (requestParams == null) {
      return Collections.emptyMap();
    }

    ConjunctiveCriterionArray disjunction = requestParams.getOr();

    if (disjunction.size() > 1) {
      throw new UnsupportedOperationException("To build request map, there must be only one conjunction group.");
    }

    CriterionArray criterionArray = disjunction.size() > 0 ? disjunction.get(0).getAnd() : new CriterionArray();

    criterionArray.forEach(criterion -> {
      if (!com.linkedin.metadata.query.filter.Condition.EQUAL.equals(criterion.getCondition())) {
        throw new UnsupportedOperationException("Unsupported condition: " + criterion.getCondition());
      }
    });

    return criterionArray.stream().collect(Collectors.toMap(Criterion::getField, Criterion::getValue));
  }

  static boolean isUrn(@Nonnull String value) {
    // TODO(https://github.com/linkedin/datahub-gma/issues/51): This method is a bit of a hack to support searching for
    // URNs that have commas in them, while also using commas a delimiter for search. We should stop supporting commas
    // as delimiter, and then we can stop using this hack.
    return value.startsWith("urn:li:");
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
   * TODO(https://github.com/linkedin/datahub-gma/issues/51): support multiple values a field can take without using
   * delimiters like comma.
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
      // TODO(https://github.com/linkedin/datahub-gma/issues/51): support multiple values a field can take without using
      // delimiters like comma. This is a hack to support equals with URN that has a comma in it.
      if (isUrn(criterion.getValue())) {
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

  @Nonnull
  public static String toEntityType(@Nonnull Class c) {
    String result = c.getSimpleName().toLowerCase();
    if (result.endsWith("entity")) {
      result = result.substring(0, result.length() - 6);
    }
    return result;
  }

  @Nonnull
  public static String readResourceFile(@Nonnull Class clazz, @Nonnull String filePath) {
    try (InputStream inputStream = clazz.getClassLoader().getResourceAsStream(filePath)) {
      return IOUtils.toString(inputStream);
    } catch (IOException e) {
      log.error("Can't read file: " + filePath);
      throw new RuntimeException("Can't read file: " + filePath);
    }
  }
}