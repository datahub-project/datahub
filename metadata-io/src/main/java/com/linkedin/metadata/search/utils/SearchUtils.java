package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.query.filter.CriterionArray;
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