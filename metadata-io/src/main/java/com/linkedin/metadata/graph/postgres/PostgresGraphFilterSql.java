package com.linkedin.metadata.graph.postgres;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Builds PostgreSQL fragments for {@link Filter} over vertex alias columns ({@code urn}, {@code
 * entityType}), matching Elasticsearch graph query constraints ({@link
 * com.linkedin.metadata.graph.elastic.utils.GraphQueryUtils} — EQUAL only).
 */
public final class PostgresGraphFilterSql {

  private PostgresGraphFilterSql() {}

  /**
   * SQL boolean expression for matching rows, with positional parameters appended to {@code out}.
   * Empty filter matches all ({@code true}).
   */
  @Nonnull
  public static String vertexFilterSql(
      @Nullable Filter filter, @Nonnull String urnColumnAlias, @Nonnull List<Object> out) {
    if (filter == null || filter.getOr() == null || filter.getOr().isEmpty()) {
      return "TRUE";
    }
    List<String> orParts = new ArrayList<>();
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      List<String> andParts = new ArrayList<>();
      for (Criterion criterion : conjunction.getAnd()) {
        if (!Condition.EQUAL.equals(criterion.getCondition())) {
          throw new UnsupportedOperationException(
              "Postgres graph filter only supports EQUAL; got " + criterion.getCondition());
        }
        andParts.add(buildCriterionSql(criterion, urnColumnAlias, out));
      }
      if (!andParts.isEmpty()) {
        orParts.add("(" + String.join(" AND ", andParts) + ")");
      }
    }
    if (orParts.isEmpty()) {
      return "TRUE";
    }
    return "(" + String.join(" OR ", orParts) + ")";
  }

  private static String buildCriterionSql(
      @Nonnull Criterion criterion, @Nonnull String urnColumnAlias, @Nonnull List<Object> out) {
    String field = criterion.getField();
    if ("urn".equals(field)) {
      if (criterion.getValues().size() == 1) {
        out.add(criterion.getValues().get(0));
        return urnColumnAlias + " = ?";
      }
      StringBuilder sb = new StringBuilder();
      sb.append(urnColumnAlias).append(" IN (");
      for (int i = 0; i < criterion.getValues().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append("?");
        out.add(criterion.getValues().get(i));
      }
      sb.append(")");
      return sb.toString();
    }
    if ("entityType".equals(field)) {
      String entityExpr = entityTypeExpr(urnColumnAlias);
      if (criterion.getValues().size() == 1) {
        out.add(criterion.getValues().get(0).toString().toLowerCase());
        return entityExpr + " = ?";
      }
      StringBuilder sb = new StringBuilder();
      sb.append(entityExpr).append(" IN (");
      for (int i = 0; i < criterion.getValues().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append("?");
        out.add(criterion.getValues().get(i).toString().toLowerCase());
      }
      sb.append(")");
      return sb.toString();
    }
    throw new UnsupportedOperationException(
        "Postgres graph vertex filter field not supported: " + field);
  }

  /**
   * Entity type segment for standard DataHub URNs {@code urn:li:<type>:...}, lowercased for
   * case-insensitive comparison (URNs use camelCase e.g. {@code dataJob}).
   */
  @Nonnull
  static String entityTypeExpr(@Nonnull String urnColumnAlias) {
    return "lower(split_part(" + urnColumnAlias + ", ':', 3))";
  }
}
