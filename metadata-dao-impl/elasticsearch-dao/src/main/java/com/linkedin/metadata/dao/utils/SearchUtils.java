package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;


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
  public static Map<String, String> getRequestMap(@Nonnull Filter requestParams) {

    if (requestParams.getCriteria()
        .stream()
        .anyMatch(criterion -> criterion.getCondition() != com.linkedin.metadata.query.Condition.EQUAL)) {
      throw new IllegalArgumentException("Invalid List criteria - condition must be EQUAL: ");
    }

    return requestParams.getCriteria().stream().collect(Collectors.toMap(Criterion::getField, Criterion::getValue));
  }

  /**
   * Convert a requestMap to a filter
   *
   * @param requestMap a map of fields and values
   * @return the search filter
   */
  @Nonnull
  public static Filter getFilter(@Nonnull Map<String, String> requestMap) {
    List<Criterion> criterionList = requestMap.entrySet().stream()
            .map(entry -> new Criterion().setField(entry.getKey()).setValue(entry.getValue()))
            .collect(Collectors.toList());
    return new Filter().setCriteria(new CriterionArray(criterionList));
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
  public static String readResourceFile(@Nonnull String filePath) {
    String query;
    try (InputStream inputStream = SearchUtils.class.getClassLoader().getResourceAsStream(filePath)) {
      query = IOUtils.toString(inputStream);
    } catch (IOException e) {
      log.error("Can't read file: " + filePath);
      throw new RuntimeException("Can't read file: " + filePath);
    }
    return query;
  }
}
