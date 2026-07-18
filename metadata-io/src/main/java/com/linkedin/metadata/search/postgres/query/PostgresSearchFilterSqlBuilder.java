package com.linkedin.metadata.search.postgres.query;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Compiles {@link Filter} trees into PostgreSQL {@code WHERE} fragments for pgSearch. Does not
 * include entity-name / search-group scoping (see {@link PostgresEntitySearchService}).
 */
@Slf4j
public final class PostgresSearchFilterSqlBuilder {

  private PostgresSearchFilterSqlBuilder() {}

  @Value
  public static class SqlFragment {
    @Nonnull String sql;
    @Nonnull List<Object> args;
  }

  @Nonnull
  public static SqlFragment build(
      @Nullable Filter filter, @Nonnull OperationContext opContext, @Nonnull String tableAlias) {
    return build(filter, opContext, tableAlias, null);
  }

  @Nonnull
  public static SqlFragment build(
      @Nullable Filter filter,
      @Nonnull OperationContext opContext,
      @Nonnull String tableAlias,
      @Nullable List<String> entityTypesForFieldResolution) {
    List<Object> args = new ArrayList<>();
    if (filter == null || filter.getOr() == null || filter.getOr().isEmpty()) {
      return new SqlFragment("TRUE", args);
    }
    List<String> orSql = new ArrayList<>();
    for (ConjunctiveCriterion cc : filter.getOr()) {
      orSql.add(
          buildConjunctiveClause(cc, opContext, tableAlias, args, entityTypesForFieldResolution));
    }
    return new SqlFragment("(" + String.join(" OR ", orSql) + ")", args);
  }

  private static String buildConjunctiveClause(
      ConjunctiveCriterion cc,
      OperationContext opContext,
      String tableAlias,
      List<Object> args,
      List<String> entityTypesForFieldResolution) {
    if (cc.getAnd() == null || cc.getAnd().isEmpty()) {
      return "TRUE";
    }
    List<String> andParts = new ArrayList<>();
    for (Criterion c : cc.getAnd()) {
      andParts.add(buildCriterion(c, opContext, tableAlias, args, entityTypesForFieldResolution));
    }
    return "(" + String.join(" AND ", andParts) + ")";
  }

  private static String buildCriterion(
      Criterion c,
      OperationContext opContext,
      String a,
      List<Object> args,
      List<String> entityTypesForFieldResolution) {
    if (c.isNegated()
        && c.getCondition() == Condition.EQUAL
        && !isReservedFilterField(c.getField())) {
      // NOT((document #>> path) = v) is UNKNOWN when the path is missing, so the row is excluded.
      // OpenSearch-style "field absent" matches negated term filters; use IS DISTINCT FROM for
      // JSON document paths (e.g. DefaultEntityFiltersUtil document defaults on cross-entity
      // search).
      return buildNegatedDocumentEqualIsDistinctFrom(
          c, opContext, a, args, entityTypesForFieldResolution);
    }
    String inner = buildCriterionInner(c, opContext, a, args, entityTypesForFieldResolution);
    if (!c.isNegated()) {
      return inner;
    }
    return "NOT (" + inner + ")";
  }

  private static boolean isReservedFilterField(String field) {
    return SearchUtil.INDEX_VIRTUAL_FIELD.equalsIgnoreCase(field)
        || "_entityType".equalsIgnoreCase(field)
        || SearchUtil.ES_INDEX_FIELD.equalsIgnoreCase(field)
        || "urn".equalsIgnoreCase(field);
  }

  private static String buildNegatedDocumentEqualIsDistinctFrom(
      Criterion c,
      OperationContext opContext,
      String a,
      List<Object> args,
      List<String> entityTypesForFieldResolution) {
    String textPath =
        PostgresSearchDocumentFieldPathUtil.toDocumentTextSqlExpr(
            opContext.getEntityRegistry(), a, c.getField(), entityTypesForFieldResolution);
    args.add(singleValue(c));
    return "(" + textPath + ") IS DISTINCT FROM ?";
  }

  private static String buildCriterionInner(
      Criterion c,
      OperationContext opContext,
      String a,
      List<Object> args,
      List<String> entityTypesForFieldResolution) {
    String field = c.getField();
    Condition cond = c.getCondition();

    if (SearchUtil.INDEX_VIRTUAL_FIELD.equalsIgnoreCase(field)
        || "_entityType".equalsIgnoreCase(field)) {
      return buildEntityTypeCriterion(cond, c, a, args);
    }
    if (SearchUtil.ES_INDEX_FIELD.equalsIgnoreCase(field)) {
      return buildIndexCriterion(c, opContext, a, args);
    }
    if ("urn".equalsIgnoreCase(field)) {
      return buildUrCriterion(cond, c, a, args);
    }

    String textPath =
        PostgresSearchDocumentFieldPathUtil.toDocumentTextSqlExpr(
            opContext.getEntityRegistry(), a, field, entityTypesForFieldResolution);

    switch (cond) {
      case EQUAL:
        args.add(singleValue(c));
        return "(" + textPath + ") = ?";
      case IEQUAL:
        args.add(singleValue(c).toLowerCase());
        return "LOWER(COALESCE(" + textPath + ", '')) = ?";
      case IN:
        return buildIn(textPath, valuesList(c), args);
      case CONTAIN:
        args.add("%" + singleValue(c) + "%");
        return "COALESCE(" + textPath + ", '') ILIKE ? ESCAPE '\\'";
      case START_WITH:
        args.add(singleValue(c) + "%");
        return "COALESCE(" + textPath + ", '') ILIKE ? ESCAPE '\\'";
      case END_WITH:
        args.add("%" + singleValue(c));
        return "COALESCE(" + textPath + ", '') ILIKE ? ESCAPE '\\'";
      case IS_NULL:
        return "(" + textPath + ") IS NULL";
      case EXISTS:
        return "(" + textPath + ") IS NOT NULL AND (" + textPath + ") <> ''";
      case GREATER_THAN:
        args.add(Double.parseDouble(singleValue(c)));
        return "(" + textPath + ")::numeric > ?";
      case GREATER_THAN_OR_EQUAL_TO:
        args.add(Double.parseDouble(singleValue(c)));
        return "(" + textPath + ")::numeric >= ?";
      case LESS_THAN:
        args.add(Double.parseDouble(singleValue(c)));
        return "(" + textPath + ")::numeric < ?";
      case LESS_THAN_OR_EQUAL_TO:
        args.add(Double.parseDouble(singleValue(c)));
        return "(" + textPath + ")::numeric <= ?";
      case DESCENDANTS_INCL:
      case ANCESTORS_INCL:
      case RELATED_INCL:
        throw new UnsupportedOperationException(
            "PostgreSQL entity search does not yet support URN hierarchy condition " + cond);
      default:
        throw new UnsupportedOperationException(
            "Unsupported filter condition for PostgreSQL entity search: " + cond);
    }
  }

  private static List<String> valuesList(Criterion c) {
    if (c.getValues() != null && !c.getValues().isEmpty()) {
      return c.getValues();
    }
    String single = singleValue(c);
    return single != null ? List.of(single) : List.of();
  }

  private static String buildEntityTypeCriterion(
      Condition cond, Criterion c, String a, List<Object> args) {
    if (cond != Condition.EQUAL && cond != Condition.IN) {
      throw new UnsupportedOperationException(
          "Unsupported condition for _entityType in PostgreSQL search: " + cond);
    }
    if (cond == Condition.IN || (c.getValues() != null && c.getValues().size() > 1)) {
      return buildIn(a + ".entity_type", valuesList(c), args);
    }
    args.add(singleValue(c));
    return a + ".entity_type = ?";
  }

  private static String buildIndexCriterion(
      Criterion c, OperationContext opContext, String a, List<Object> args) {
    List<String> indexNames = valuesList(c);
    List<String> groups = new ArrayList<>();
    for (String indexName : indexNames) {
      Optional<String> sg = resolveSearchGroup(opContext, indexName);
      if (sg.isPresent()) {
        groups.add(sg.get());
      } else {
        log.warn(
            "Could not map OpenSearch index name {} to a pgSearch search_group; clause will match nothing for this value",
            indexName);
        groups.add("__no_such_search_group__");
      }
    }
    return buildIn(a + ".search_group", groups, args);
  }

  private static String buildUrCriterion(Condition cond, Criterion c, String a, List<Object> args) {
    if (cond != Condition.EQUAL && cond != Condition.IN) {
      throw new UnsupportedOperationException(
          "Unsupported condition for urn in PostgreSQL search: " + cond);
    }
    if (cond == Condition.IN || (c.getValues() != null && c.getValues().size() > 1)) {
      return buildIn(a + ".urn", valuesList(c), args);
    }
    args.add(singleValue(c));
    return a + ".urn = ?";
  }

  private static String buildIn(String expr, List<String> values, List<Object> args) {
    if (values == null || values.isEmpty()) {
      return "FALSE";
    }
    List<String> placeholders =
        values.stream()
            .map(
                v -> {
                  args.add(v);
                  return "?";
                })
            .collect(Collectors.toList());
    return expr + " IN (" + String.join(", ", placeholders) + ")";
  }

  private static String singleValue(Criterion c) {
    if (c.getValues() != null && !c.getValues().isEmpty()) {
      return c.getValues().get(0);
    }
    return null;
  }

  /** Strip Elasticsearch ".keyword" suffixes for JSON path navigation. */
  @Nonnull
  public static String normalizeFieldForJson(@Nonnull String field) {
    return field.replace(".keyword", "");
  }

  /**
   * SQL expression returning text at a path in {@code document}, e.g. {@code alias.document #>>
   * '{a,b}'}.
   */
  @Nonnull
  public static String documentTextPathExpr(
      @Nonnull String tableAlias, @Nonnull String normalizedField) {
    String[] segs = normalizedField.split("\\.");
    String array =
        "ARRAY['"
            + Arrays.stream(segs).map(s -> s.replace("'", "''")).collect(Collectors.joining("','"))
            + "']::text[]";
    return tableAlias + ".document #>> " + array;
  }

  /**
   * SQL expression returning {@code jsonb} at a path in {@code document}, e.g. {@code
   * alias.document #> '{a,b}'}.
   */
  @Nonnull
  public static String documentJsonbPathExpr(
      @Nonnull String tableAlias, @Nonnull String normalizedField) {
    String[] segs = normalizedField.split("\\.");
    String array =
        "ARRAY['"
            + Arrays.stream(segs).map(s -> s.replace("'", "''")).collect(Collectors.joining("','"))
            + "']::text[]";
    return tableAlias + ".document #> " + array;
  }

  /**
   * Map an Elasticsearch/OpenSearch physical index name to the pgSearch {@code search_group} string
   * (v3 per-group indices first, then v2 per-entity indices).
   */
  @Nonnull
  public static Optional<String> resolveSearchGroup(
      @Nonnull OperationContext opContext, @Nonnull String entityIndexName) {
    var ic = opContext.getSearchContext().getIndexConvention();
    var reg = opContext.getEntityRegistry();
    for (String sg : reg.getSearchGroups()) {
      if (entityIndexName.equals(ic.getEntityIndexNameV3(sg))) {
        return Optional.of(sg);
      }
    }
    for (EntitySpec spec : reg.getEntitySpecs().values()) {
      if (spec != null && entityIndexName.equals(ic.getIndexName(spec))) {
        return Optional.ofNullable(spec.getSearchGroup());
      }
    }
    return Optional.empty();
  }
}
