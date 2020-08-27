package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
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
   * Validates the request params and create a request map out of it
   *
   * @param requestParams the search request with fields and values
   * @return a request map
   */
  @Nonnull
  public static Map<String, String> getRequestMap(@Nullable Filter requestParams) {
    if (requestParams == null) {
      return Collections.emptyMap();
    }

    requestParams.getCriteria().forEach(criterion -> {
      if (criterion.getCondition() != com.linkedin.metadata.query.Condition.EQUAL) {
        throw new UnsupportedOperationException("Unsupported condition: " + criterion.getCondition());
      }
    });

    return requestParams.getCriteria().stream().collect(Collectors.toMap(Criterion::getField, Criterion::getValue));
  }

  /**
   * Builds search query using criterion
   *
   * @param criterion {@link Criterion} single criterion which contains field, value and a comparison operator
   * @return QueryBuilder
   */
  @Nonnull
  public static QueryBuilder getQueryBuilderFromCriterion(@Nonnull Criterion criterion) {
    final Condition condition = criterion.getCondition();
    if (condition == Condition.EQUAL) {
      return QueryBuilders.termsQuery(criterion.getField(), criterion.getValue().trim().split("\\s*,\\s*"));
    } else if (condition == Condition.GREATER_THAN) {
      return QueryBuilders.rangeQuery(criterion.getField()).gt(criterion.getValue().trim());
    } else if (condition == Condition.GREATER_THAN_OR_EQUAL_TO) {
      return QueryBuilders.rangeQuery(criterion.getField()).gte(criterion.getValue().trim());
    } else if (condition == Condition.LESS_THAN) {
      return QueryBuilders.rangeQuery(criterion.getField()).lt(criterion.getValue().trim());
    } else if (condition == Condition.LESS_THAN_OR_EQUAL_TO) {
      return QueryBuilders.rangeQuery(criterion.getField()).lte(criterion.getValue().trim());
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