package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.DisjunctiveCriterion;
import com.linkedin.metadata.query.filter.DisjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
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

  public static void validateFilter(@Nonnull Filter filter) {
    if (filter.getOr() != null && filter.getAnd() != null) {
      log.error("Filter cannot have both or and and fields set {}", filter);
      throw new IllegalArgumentException(
          String.format("Filter cannot have both or and and fields set %s", filter.toString()));
    }
  }

  @Nonnull
  public static Filter removeCriteria(@Nonnull Filter originalFilter, Predicate<Criterion> shouldRemove) {
    if (originalFilter.getOr() != null) {
      return new Filter().setOr(new ConjunctiveCriterionArray(originalFilter.getOr()
          .stream()
          .map(criteria -> removeCriteria(criteria, shouldRemove))
          .collect(Collectors.toList())));
    } else if (originalFilter.getAnd() != null) {
      return new Filter().setAnd(new DisjunctiveCriterionArray(originalFilter.getAnd()
          .stream()
          .map(criteria -> removeCriteria(criteria, shouldRemove))
          .collect(Collectors.toList())));
    }
    return originalFilter;
  }

  private static DisjunctiveCriterion removeCriteria(@Nonnull DisjunctiveCriterion disjunctiveCriterion,
      Predicate<Criterion> shouldRemove) {
    return new DisjunctiveCriterion().setOr(new ConjunctiveCriterionArray(disjunctiveCriterion.getOr()
        .stream()
        .map(criteria -> removeCriteria(criteria, shouldRemove))
        .collect(Collectors.toList())));
  }

  private static ConjunctiveCriterion removeCriteria(@Nonnull ConjunctiveCriterion conjunctiveCriterion,
      Predicate<Criterion> shouldRemove) {
    return new ConjunctiveCriterion().setAnd(new CriterionArray(conjunctiveCriterion.getAnd()
        .stream()
        .filter(criterion -> !shouldRemove.test(criterion))
        .collect(Collectors.toList())));
  }
}