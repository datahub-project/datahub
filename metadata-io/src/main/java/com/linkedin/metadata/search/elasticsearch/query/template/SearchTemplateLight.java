package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/** Gates for trying the light query before the full query. */
public final class SearchTemplateLight {
  private static final Set<String> COLUMN_FIELDS =
      Set.of("columnName", "columns", "fieldPaths", "columnDescription", "editedFieldDescriptions");

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

  /** True when any criterion filters a column-name field. */
  public static boolean hasColumnNameFilter(@Nullable Filter filter) {
    if (filter == null || filter.getOr() == null || filter.getOr().isEmpty()) {
      return false;
    }
    for (ConjunctiveCriterion conjunctive : filter.getOr()) {
      if (conjunctive.getAnd() == null) {
        continue;
      }
      for (Criterion criterion : conjunctive.getAnd()) {
        if (criterion.getField() == null) {
          continue;
        }
        if (COLUMN_FIELDS.contains(leaf(criterion.getField()))) {
          return true;
        }
      }
    }
    return false;
  }

  private static String leaf(String field) {
    int dot = field.lastIndexOf('.');
    return dot < 0 ? field : field.substring(dot + 1);
  }
}
