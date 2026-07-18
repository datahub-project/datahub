package com.linkedin.metadata.timeseries.postgres;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;
import static com.linkedin.metadata.query.filter.Condition.ANCESTORS_INCL;
import static com.linkedin.metadata.query.filter.Condition.DESCENDANTS_INCL;
import static com.linkedin.metadata.query.filter.Condition.RELATED_INCL;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds PostgreSQL {@code WHERE} fragments against the {@code document jsonb} column for the same
 * {@link Filter} model Elasticsearch uses. Trailing {@code .keyword} is stripped — PG jsonb has no
 * text/keyword split.
 */
@Slf4j
public final class TimeseriesFilterSqlBuilder {

  private TimeseriesFilterSqlBuilder() {}

  @Value
  public static class BuiltSql {
    /** SQL boolean expression, or {@code "TRUE"} when empty. Never null. */
    @Nonnull String expression;

    /** Positional parameters for {@code PreparedStatement} in order. */
    @Nonnull List<Object> params;
  }

  @Nonnull
  public static BuiltSql buildDocumentFilter(
      @Nullable Filter filter,
      boolean isTimeseries,
      @Nonnull Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {

    if (filter == null) {
      return new BuiltSql("TRUE", Collections.emptyList());
    }

    StructuredPropertyUtils.validateFilter(opContext, filter, opContext.getAspectRetriever());

    List<Object> params = new ArrayList<>();
    String expr =
        buildTopLevelFilter(filter, isTimeseries, searchableFieldTypes, opContext, params);

    if (Boolean.TRUE.equals(
        opContext.getSearchContext().getSearchFlags().isFilterNonLatestVersions())) {
      log.debug(
          "TimeseriesFilterSqlBuilder: filterNonLatestVersions is set; ES applies extra must clauses — not mirrored in PG builder yet");
    }

    return new BuiltSql(expr, params);
  }

  private static String buildTopLevelFilter(
      @Nonnull Filter filter,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      OperationContext opContext,
      List<Object> params) {

    if (filter.getOr() != null && !filter.getOr().isEmpty()) {
      List<String> disjuncts = new ArrayList<>();
      for (ConjunctiveCriterion cc : filter.getOr()) {
        disjuncts.add(
            "("
                + buildConjunctive(cc, isTimeseries, searchableFieldTypes, opContext, params)
                + ")");
      }
      return String.join(" OR ", disjuncts);
    }

    if (filter.getCriteria() != null && !filter.getCriteria().isEmpty()) {
      List<String> parts = new ArrayList<>();
      for (Criterion c : filter.getCriteria()) {
        if (c.hasValues()
            || c.getCondition() == Condition.IS_NULL
            || c.getCondition() == Condition.EXISTS) {
          parts.add(
              "("
                  + buildCriterionSql(
                      c, isTimeseries, searchableFieldTypes, opContext, params, false)
                  + ")");
        }
      }
      return parts.isEmpty() ? "TRUE" : String.join(" AND ", parts);
    }

    return "TRUE";
  }

  private static String buildConjunctive(
      ConjunctiveCriterion cc,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      OperationContext opContext,
      List<Object> params) {
    List<String> parts = new ArrayList<>();
    for (Criterion c : cc.getAnd()) {
      if (Set.of(Condition.EXISTS, Condition.IS_NULL).contains(c.getCondition()) || c.hasValues()) {
        boolean neg = c.isNegated();
        String inner =
            buildCriterionSql(c, isTimeseries, searchableFieldTypes, opContext, params, neg);
        parts.add("(" + inner + ")");
      }
    }
    return parts.isEmpty() ? "TRUE" : String.join(" AND ", parts);
  }

  private static String buildCriterionSql(
      @Nonnull Criterion criterion,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      OperationContext opContext,
      List<Object> params,
      boolean alreadyNegated) {

    boolean negated = criterion.isNegated() ^ alreadyNegated;
    String expandKey =
        ESUtils.toParentField(opContext, criterion.getField(), opContext.getAspectRetriever());
    Optional<List<String>> expand =
        Optional.ofNullable(ESUtils.FIELDS_TO_EXPANDED_FIELDS_LIST.get(expandKey));

    if (expand.isPresent()) {
      List<String> ors = new ArrayList<>();
      for (String alt : expand.get()) {
        Criterion copy =
            new Criterion()
                .setField(alt)
                .setCondition(criterion.getCondition())
                .setNegated(criterion.isNegated())
                .setValues(criterion.getValues());
        ors.add(
            buildCriterionSql(
                copy, isTimeseries, searchableFieldTypes, opContext, params, alreadyNegated));
      }
      String combined =
          String.join(" OR ", ors.stream().map(s -> "(" + s + ")").collect(Collectors.toList()));
      return negated ? "NOT (" + combined + ")" : combined;
    }

    String fieldName =
        TimeseriesPgJsonPaths.stripKeywordSuffix(
            ESUtils.toParentField(opContext, criterion.getField(), opContext.getAspectRetriever()));

    Condition condition = criterion.getCondition();
    if (condition == Condition.IS_NULL) {
      String path = jsonTextPathExprWithParams(fieldName, params);
      String exists = "(" + path + " IS NULL OR " + path + " = '')";
      return negated ? "NOT " + exists : exists;
    }
    if (condition == Condition.EXISTS) {
      String path = jsonTextPathExprWithParams(fieldName, params);
      String ex = "(" + path + " IS NOT NULL AND " + path + " <> '')";
      return negated ? "NOT (" + ex + ")" : ex;
    }

    if (!criterion.hasValues()
        || criterion.getValues() == null
        || criterion.getValues().isEmpty()) {
      return "TRUE";
    }

    String core;
    if (condition == ANCESTORS_INCL || condition == DESCENDANTS_INCL || condition == RELATED_INCL) {
      core = buildLineageAny(fieldName, criterion, condition, opContext, params);
    } else if (condition == Condition.EQUAL || condition == Condition.IEQUAL) {
      core =
          buildEqual(fieldName, criterion, isTimeseries, searchableFieldTypes, opContext, params);
    } else if (ESUtils.RANGE_QUERY_CONDITIONS.contains(condition)) {
      core =
          buildRange(
              fieldName,
              criterion,
              condition,
              isTimeseries,
              searchableFieldTypes,
              opContext,
              params);
    } else if (condition == Condition.CONTAIN) {
      core = buildLike(fieldName, criterion, params, "%", "%");
    } else if (condition == Condition.START_WITH) {
      core = buildLike(fieldName, criterion, params, "", "%");
    } else if (condition == Condition.END_WITH) {
      core = buildLike(fieldName, criterion, params, "%", "");
    } else {
      throw new UnsupportedOperationException(
          "Timeseries PostgreSQL filter: unsupported condition " + condition);
    }

    return negated ? "NOT (" + core + ")" : core;
  }

  /** Produces expression with ? placeholders for path segments when multi-segment. */
  private static void appendJsonPathParams(@Nonnull String dottedField, List<Object> params) {
    for (String seg : TimeseriesPgJsonPaths.pathSegments(dottedField)) {
      params.add(seg);
    }
  }

  private static String jsonTextPathExprWithParams(
      @Nonnull String dottedField, List<Object> params) {
    String[] segs = TimeseriesPgJsonPaths.pathSegments(dottedField);
    if (segs.length == 1) {
      return "document->>'" + escapeSqlIdent(segs[0]) + "'";
    }
    appendJsonPathParams(dottedField, params);
    return "document #>> ARRAY["
        + String.join(",", Collections.nCopies(segs.length, "?"))
        + "]::text[]";
  }

  private static String escapeSqlIdent(String s) {
    return s.replace("'", "''");
  }

  /**
   * Lineage filters: expand seed URNs via graph ({@link TimeseriesFilterGraphExpansion}), then
   * match {@code document} field with {@code = ANY(?::text[])}.
   */
  private static String buildLineageAny(
      String fieldName,
      Criterion criterion,
      Condition lineageCondition,
      OperationContext opContext,
      List<Object> params) {
    List<String> seeds = criterion.getValues();
    java.util.Set<String> expanded =
        TimeseriesFilterGraphExpansion.expandForLineageCondition(
            opContext, lineageCondition, seeds);
    String[] arr = expanded.toArray(new String[0]);
    String pathExpr = jsonTextPathExprWithParams(fieldName, params);
    params.add(arr);
    return "(" + pathExpr + " = ANY(?))";
  }

  private static String buildEqual(
      String fieldName,
      Criterion criterion,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      OperationContext opContext,
      List<Object> params) {

    List<String> values = criterion.getValues();

    Set<String> elasticTypes =
        resolveElasticFieldTypes(
            opContext, fieldName, criterion, searchableFieldTypes, opContext.getAspectRetriever());

    List<String> ors = new ArrayList<>();
    for (String raw : values) {
      String v = raw.trim();
      if (v.startsWith("urn:li:")) {
        ors.add(jsonTextPathExprWithParams(fieldName, params) + " = ?");
        params.add(v);
      } else if (elasticTypes.contains(ESUtils.BOOLEAN_FIELD_TYPE) && values.size() == 1) {
        ors.add(jsonTextPathExprWithParams(fieldName, params) + " = ?");
        params.add(Boolean.parseBoolean(v));
      } else if (elasticTypes.contains(ESUtils.LONG_FIELD_TYPE)
          || elasticTypes.contains(ESUtils.DATE_FIELD_TYPE)) {
        ors.add(jsonTextPathExprWithParams(fieldName, params) + " = ?");
        params.add(Long.parseLong(v));
      } else if (elasticTypes.contains(ESUtils.DOUBLE_FIELD_TYPE)) {
        ors.add(jsonTextPathExprWithParams(fieldName, params) + " = ?");
        params.add(Double.parseDouble(v));
      } else {
        ors.add(jsonTextPathExprWithParams(fieldName, params) + " = ?");
        params.add(v);
      }
    }
    if (ors.size() == 1) {
      return ors.get(0);
    }
    return "("
        + String.join(" OR ", ors.stream().map(s -> "(" + s + ")").collect(Collectors.toList()))
        + ")";
  }

  private static String buildRange(
      String fieldName,
      Criterion criterion,
      Condition condition,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      OperationContext opContext,
      List<Object> params) {

    String v0 = criterion.getValues().get(0).trim();
    Set<String> elasticTypes =
        resolveElasticFieldTypes(
            opContext, fieldName, criterion, searchableFieldTypes, opContext.getAspectRetriever());
    String path = jsonTextPathExprWithParams(fieldName, params);
    String cast =
        elasticTypes.contains(ESUtils.DOUBLE_FIELD_TYPE)
            ? "double precision"
            : elasticTypes.contains(ESUtils.LONG_FIELD_TYPE)
                    || elasticTypes.contains(ESUtils.DATE_FIELD_TYPE)
                ? "bigint"
                : elasticTypes.contains(ESUtils.BOOLEAN_FIELD_TYPE) ? "boolean" : "text";

    String castExpr = "(" + path + ")::" + cast;
    params.add(
        elasticTypes.contains(ESUtils.DOUBLE_FIELD_TYPE)
            ? Double.parseDouble(v0)
            : elasticTypes.contains(ESUtils.LONG_FIELD_TYPE)
                    || elasticTypes.contains(ESUtils.DATE_FIELD_TYPE)
                ? Long.parseLong(v0)
                : v0);

    String op;
    switch (condition) {
      case GREATER_THAN:
        op = ">";
        break;
      case GREATER_THAN_OR_EQUAL_TO:
        op = ">=";
        break;
      case LESS_THAN:
        op = "<";
        break;
      case LESS_THAN_OR_EQUAL_TO:
        op = "<=";
        break;
      default:
        throw new IllegalStateException(condition.toString());
    }
    return castExpr + " " + op + " ?";
  }

  private static String buildLike(
      String fieldName, Criterion criterion, List<Object> params, String pre, String post) {
    List<String> ors = new ArrayList<>();
    for (String raw : criterion.getValues()) {
      String v = pre + ESUtils.escapeReservedCharacters(raw.trim()) + post;
      ors.add(jsonTextPathExprWithParams(fieldName, params) + " LIKE ? ESCAPE '\\'");
      params.add(v);
    }
    return "("
        + String.join(" OR ", ors.stream().map(s -> "(" + s + ")").collect(Collectors.toList()))
        + ")";
  }

  private static Set<String> resolveElasticFieldTypes(
      OperationContext opContext,
      String fieldName,
      Criterion criterion,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFields,
      AspectRetriever aspectRetriever) {
    final Set<String> finalFieldTypes;
    if (fieldName.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX)) {
      finalFieldTypes =
          StructuredPropertyUtils.toElasticsearchFieldType(
              opContext, ESUtils.replaceSuffix(criterion.getField()), aspectRetriever);
    } else {
      Set<SearchableAnnotation.FieldType> fieldTypes =
          searchableFields.getOrDefault(fieldName.split("\\.")[0], Collections.emptySet());
      finalFieldTypes =
          fieldTypes.stream().map(ESUtils::getElasticTypeForFieldType).collect(Collectors.toSet());
    }
    return finalFieldTypes;
  }
}
