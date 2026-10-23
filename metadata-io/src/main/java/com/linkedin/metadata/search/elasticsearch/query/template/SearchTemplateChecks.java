package com.linkedin.metadata.search.elasticsearch.query.template;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/**
 * Rendered DSL checks. Structural limits run first in {@link SearchTemplatePolicyChecks}.
 * Elasticsearch bodies are parsed. Postgres bodies are rendered only.
 */
public final class SearchTemplateChecks {

  private SearchTemplateChecks() {}

  /** Checks the parent-merged template, which is the body that will render. */
  public static void checkMerged(@Nonnull MergedSearchTemplate merged, int maxLeafClauses) {
    checkMerged(merged, maxLeafClauses, Integer.MAX_VALUE, Integer.MAX_VALUE, null, null, null);
  }

  public static void checkMerged(
      @Nonnull MergedSearchTemplate merged,
      int maxLeafClauses,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    checkMerged(
        merged,
        maxLeafClauses,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        context,
        retriever,
        entitySpecs);
  }

  public static void checkMerged(
      @Nonnull MergedSearchTemplate merged,
      int maxLeafClauses,
      int maxRoutes,
      int maxRoutePatternLength,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    SearchTemplateProperties composed = new SearchTemplateProperties();
    composed.setName("merged");
    composed.setDialect(SearchTemplateDialect.valueOf(merged.getDialect()));
    composed.setApiVersion(merged.getApiVersion());
    if (!merged.getCapabilities().isEmpty()) {
      composed.setCapabilities(new StringArray(merged.getCapabilities()));
    }
    if (merged.getQuery() != null) {
      composed.setQuery(merged.getQuery());
    }
    if (merged.getClauses() != null) {
      composed.setClauses(merged.getClauses());
    }
    if (merged.getFunctionScore() != null) {
      composed.setFunctionScore(merged.getFunctionScore());
    }
    if (merged.getRescore() != null) {
      composed.setRescore(merged.getRescore());
    }
    if (merged.getKnn() != null) {
      composed.setKnn(merged.getKnn());
    }
    if (merged.getRanking() != null) {
      composed.setRanking(merged.getRanking());
    }
    if (merged.getRetrieval() != null) {
      composed.setRetrieval(merged.getRetrieval());
    }
    if (merged.getLadder() != null) {
      composed.setLadder(merged.getLadder());
    }
    if (merged.getAggregations() != null) {
      composed.setAggregations(merged.getAggregations());
    }
    if (merged.getSort() != null) {
      composed.setSort(merged.getSort());
    }
    if (merged.getFields() != null) {
      composed.setFields(merged.getFields());
    }
    if (merged.getBoosts() != null) {
      composed.setBoosts(merged.getBoosts());
    }
    if (merged.getStructuredPropertyQuery() != null) {
      composed.setStructuredPropertyQuery(merged.getStructuredPropertyQuery());
    }
    if (merged.getNormalization() != null) {
      composed.setNormalization(merged.getNormalization());
    }
    if (!merged.getQueryFragments().isEmpty()) {
      composed.setQueryFragments(new StringMap(merged.getQueryFragments()));
    }
    if (merged.getDefaultRoute() != null) {
      SearchTemplateDefaultRoute route = new SearchTemplateDefaultRoute();
      route.setTemplate(UrnUtils.getUrn(merged.getDefaultRoute()));
      composed.setDefaultRoute(route);
    }
    if (merged.getHighlightFields() != null
        && !SearchTemplateLoader.emptyHighlights(merged.getHighlightFields())) {
      composed.setHighlightFields(merged.getHighlightFields());
    }
    if (!merged.getParameters().isEmpty()) {
      composed.setParameters(new StringMap(merged.getParameters()));
    }
    check(
        composed,
        maxLeafClauses,
        maxRoutes,
        maxRoutePatternLength,
        context,
        retriever,
        entitySpecs);
  }

  public static void check(@Nonnull SearchTemplateProperties properties, int maxLeafClauses) {
    check(properties, maxLeafClauses, Integer.MAX_VALUE, Integer.MAX_VALUE, null, null, null);
  }

  public static void check(
      @Nonnull SearchTemplateProperties properties,
      int maxLeafClauses,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    check(
        properties,
        maxLeafClauses,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        context,
        retriever,
        entitySpecs);
  }

  public static void check(
      @Nonnull SearchTemplateProperties properties,
      int maxLeafClauses,
      int maxRoutes,
      int maxRoutePatternLength,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    try {
      checkProperties(
          properties,
          maxLeafClauses,
          maxRoutes,
          maxRoutePatternLength,
          context,
          retriever,
          entitySpecs);
    } catch (SearchTemplateException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new SearchTemplateException("Search template is not valid", e);
    }
  }

  private static void checkProperties(
      @Nonnull SearchTemplateProperties properties,
      int maxLeafClauses,
      int maxRoutes,
      int maxRoutePatternLength,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    SearchTemplatePolicyChecks.check(
        properties, maxRoutes, maxRoutePatternLength, context, retriever, entitySpecs);
    Map<String, String> fragments = rewriteFragments(properties, context, retriever);
    String query =
        rewrite(section(properties.hasQuery() ? properties.getQuery() : null), context, retriever);
    if (query != null) {
      SearchTemplateHoles.requirePresent(query, fragments.keySet());
    }
    String clauses =
        rewrite(
            section(properties.hasClauses() ? properties.getClauses() : null), context, retriever);
    String functionScore =
        rewrite(
            section(properties.hasFunctionScore() ? properties.getFunctionScore() : null),
            context,
            retriever);
    String rescore =
        rewrite(
            section(properties.hasRescore() ? properties.getRescore() : null), context, retriever);
    String knn = section(properties.hasKnn() ? properties.getKnn() : null);
    String aggregations =
        rewrite(
            section(properties.hasAggregations() ? properties.getAggregations() : null),
            context,
            retriever);
    String sort =
        rewrite(section(properties.hasSort() ? properties.getSort() : null), context, retriever);
    rewriteHighlights(properties, context, retriever);
    String source =
        String.join(
            "\n",
            nullToEmpty(query),
            nullToEmpty(clauses),
            nullToEmpty(functionScore),
            nullToEmpty(rescore),
            nullToEmpty(knn),
            nullToEmpty(aggregations),
            nullToEmpty(sort),
            String.join("\n", fragments.values()));
    rejectUnescaped(query, clauses, functionScore, rescore, knn, aggregations, sort, fragments);
    Map<String, Object> mustache = SearchTemplateMustache.validationContext(source);
    Map<String, Object> spliced = new LinkedHashMap<>(mustache);
    for (Map.Entry<String, String> entry : fragments.entrySet()) {
      String rendered = render(entry.getValue(), mustache);
      if (rendered != null) {
        spliced.put(entry.getKey(), rendered);
      }
    }
    String renderedQuery = render(query, spliced);
    String renderedClauses = render(clauses, mustache);
    String renderedFunctionScore = render(functionScore, mustache);
    String renderedRescore = render(rescore, mustache);
    String renderedKnn = render(knn, mustache);
    String renderedAggregations = render(aggregations, mustache);
    String renderedSort = render(sort, mustache);
    if (properties.getDialect() != SearchTemplateDialect.ELASTICSEARCH) {
      return;
    }
    rejectForbidden(renderedQuery, "query");
    rejectForbidden(renderedClauses, "clauses");
    rejectForbidden(renderedFunctionScore, "functionScore");
    rejectForbidden(renderedRescore, "rescore");
    rejectForbidden(renderedKnn, "knn");
    rejectForbidden(renderedAggregations, "aggregations");
    rejectForbidden(renderedSort, "sort");
    if (SearchTemplateJson.isTopLevelFunctionScore(renderedQuery)) {
      throw new SearchTemplateException(
          "Search template query cannot be a top-level function_score");
    }
    if (renderedRescore != null && SearchTemplateJson.boostModeReplace(renderedFunctionScore)) {
      throw new SearchTemplateException(
          "Search template rescore cannot be combined with functionScore.boost_mode replace");
    }
    if (renderedRescore != null && SearchTemplateJson.sortClauseCount(renderedSort) > 0) {
      throw new SearchTemplateException("Search template rescore cannot be combined with sort");
    }
    if (renderedKnn != null) {
      SearchTemplateJson.parseKnn(renderedKnn);
    }
    if (renderedKnn != null && SearchTemplateJson.sortClauseCount(renderedSort) > 0) {
      throw new SearchTemplateException("Search template knn cannot be combined with sort");
    }
    if (renderedQuery != null) {
      SearchTemplateJson.parseQuery(renderedQuery);
    }
    if (renderedAggregations != null) {
      SearchTemplateJson.parseAggregations(renderedAggregations);
    }
    if (renderedSort != null) {
      SearchTemplateJson.parseSort(renderedSort);
    }
    int leaves = composedLeaves(renderedQuery, renderedClauses, renderedFunctionScore);
    leaves += SearchTemplateJson.rescoreLeaves(renderedRescore);
    if (leaves > maxLeafClauses) {
      throw new SearchTemplateException(
          "Search template has " + leaves + " leaf clauses and the limit is " + maxLeafClauses);
    }
  }

  private static int composedLeaves(
      @Nullable String query, @Nullable String clauses, @Nullable String functionScore) {
    if (query == null && clauses == null && functionScore == null) {
      return 0;
    }
    QueryBuilder built =
        query == null ? QueryBuilders.matchAllQuery() : SearchTemplateJson.parseQuery(query);
    if (clauses != null) {
      built = SearchTemplateJson.mergeClauses(built, clauses);
    }
    if (functionScore != null) {
      built = SearchTemplateJson.wrapFunctionScore(built, functionScore);
    }
    return SearchTemplateJson.countLeaves(built.toString());
  }

  private static void rejectUnescaped(
      @Nullable String query,
      @Nullable String clauses,
      @Nullable String functionScore,
      @Nullable String rescore,
      @Nullable String knn,
      @Nullable String aggregations,
      @Nullable String sort,
      @Nonnull Map<String, String> fragments) {
    Set<String> holes = fragments.keySet();
    SearchTemplateHoles.rejectUnescaped("query", query, holes);
    SearchTemplateHoles.rejectUnescaped("clauses", clauses, holes);
    SearchTemplateHoles.rejectUnescaped("functionScore", functionScore, holes);
    SearchTemplateHoles.rejectUnescaped("rescore", rescore, holes);
    SearchTemplateHoles.rejectUnescaped("knn", knn, holes);
    SearchTemplateHoles.rejectUnescaped("aggregations", aggregations, holes);
    SearchTemplateHoles.rejectUnescaped("sort", sort, holes);
    fragments.forEach(
        (name, value) -> SearchTemplateHoles.rejectUnescaped("queryFragments", value, holes));
  }

  private static void rejectForbidden(@Nullable String json, @Nonnull String section) {
    SearchTemplateJson.rejectForbidden(json, section);
  }

  private static void rewriteHighlights(
      @Nonnull SearchTemplateProperties properties,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (!properties.hasHighlightFields() || properties.getHighlightFields() == null) {
      return;
    }
    var highlights = properties.getHighlightFields();
    rewriteAll(highlights.hasAdd() ? highlights.getAdd() : null, context, retriever);
    rewriteAll(highlights.hasRemove() ? highlights.getRemove() : null, context, retriever);
    rewriteAll(highlights.hasReplace() ? highlights.getReplace() : null, context, retriever);
  }

  private static void rewriteAll(
      @Nullable Iterable<String> names,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (names == null) {
      return;
    }
    for (String name : names) {
      StructuredPropertyReferences.rewrite(name, context, retriever);
    }
  }

  @Nonnull
  private static Map<String, String> rewriteFragments(
      @Nonnull SearchTemplateProperties properties,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    Map<String, String> fragments = new LinkedHashMap<>();
    if (!properties.hasQueryFragments() || properties.getQueryFragments() == null) {
      return fragments;
    }
    properties
        .getQueryFragments()
        .forEach(
            (name, value) -> {
              if (value != null && !value.isEmpty()) {
                fragments.put(name, rewrite(value, context, retriever));
              }
            });
    return fragments;
  }

  @Nullable
  private static String rewrite(
      @Nullable String text,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    return StructuredPropertyReferences.rewrite(text, context, retriever);
  }

  @Nullable
  private static String render(@Nullable String template, @Nonnull Map<String, Object> context) {
    if (template == null) {
      return null;
    }
    String rendered = SearchTemplateMustache.render(template, context).trim();
    return rendered.isEmpty() ? null : rendered;
  }

  @Nullable
  private static String section(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }

  @Nonnull
  private static String nullToEmpty(@Nullable String value) {
    return value == null ? "" : value;
  }
}
