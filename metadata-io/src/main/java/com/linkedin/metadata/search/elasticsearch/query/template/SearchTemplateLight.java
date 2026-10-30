package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/** Gates for trying the light query before the full query. */
public final class SearchTemplateLight {
  private SearchTemplateLight() {}

  public static boolean relevanceSort(@Nullable List<SortCriterion> sortCriteria) {
    if (sortCriteria == null || sortCriteria.isEmpty()) {
      return true;
    }
    for (SortCriterion criterion : sortCriteria) {
      String field = criterion.getField();
      if (field != null && !"_score".equals(field) && !"score".equals(field)) {
        return false;
      }
      if (criterion.getOrder() == SortOrder.ASCENDING) {
        return false;
      }
    }
    return true;
  }

  /**
   * True when a positive criterion filters a column. Covers the UI {@code fieldPaths} filter, the
   * legacy criteria array, and degree-1 name and description fields.
   */
  public static boolean hasColumnNameFilter(@Nullable Filter filter) {
    if (filter == null) {
      return false;
    }
    Stream<Criterion> criteria =
        Stream.concat(
            filter.hasOr() && filter.getOr() != null
                ? filter.getOr().stream()
                    .flatMap(
                        conjunction ->
                            conjunction.getAnd() == null
                                ? Stream.empty()
                                : conjunction.getAnd().stream())
                : Stream.empty(),
            filter.hasCriteria() && filter.getCriteria() != null
                ? filter.getCriteria().stream()
                : Stream.empty());
    return criteria.anyMatch(
        criterion ->
            criterion.getField() != null
                && !Boolean.TRUE.equals(criterion.isNegated())
                && isColumnField(criterion.getField()));
  }

  /** A column is {@code fieldPaths}, a legacy column label, or a degree-1 name or description. */
  public static boolean isColumnField(@Nullable String field) {
    if (field == null || field.isEmpty()) {
      return false;
    }
    if ("fieldPaths".equals(field) || field.startsWith("fieldPaths.")) {
      return true;
    }
    String name = leaf(field);
    if ("fieldPaths".equals(name)
        || "columnName".equals(name)
        || "columnDescription".equals(name)
        || "editedFieldDescriptions".equals(name)) {
      return true;
    }
    return name.endsWith("_1") && (name.startsWith("name") || name.startsWith("description"));
  }

  private static String leaf(String field) {
    for (String suffix : List.of(".keyword", ".lowercase")) {
      if (field.endsWith(suffix)) {
        field = field.substring(0, field.length() - suffix.length());
        break;
      }
    }
    int dot = field.lastIndexOf('.');
    return dot < 0 ? field : field.substring(dot + 1);
  }
}
