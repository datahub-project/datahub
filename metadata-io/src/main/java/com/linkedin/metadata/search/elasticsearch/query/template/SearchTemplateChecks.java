package com.linkedin.metadata.search.elasticsearch.query.template;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateMustache;
import com.linkedin.metadata.search.elasticsearch.query.v3.StructuredPropertyQueryFields;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.StructuredPropertyQuery;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

/** Write-time render. Elasticsearch bodies are parsed. Postgres bodies are rendered only. */
public final class SearchTemplateChecks {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SearchTemplateChecks() {}

  /** Checks the parent-merged template, which is the body that will render. */
  public static void checkMerged(@Nonnull MergedSearchTemplate merged, int maxLeafClauses) {
    checkMerged(merged, maxLeafClauses, null, null, null);
  }

  public static void checkMerged(
      @Nonnull MergedSearchTemplate merged,
      int maxLeafClauses,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    SearchTemplateProperties composed = new SearchTemplateProperties();
    composed.setName("merged");
    composed.setDialect(SearchTemplateDialect.valueOf(merged.getDialect()));
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
    check(composed, maxLeafClauses, context, retriever, entitySpecs);
  }

  public static void check(@Nonnull SearchTemplateProperties properties, int maxLeafClauses) {
    check(properties, maxLeafClauses, null, null, null);
  }

  public static void check(
      @Nonnull SearchTemplateProperties properties,
      int maxLeafClauses,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    try {
      checkProperties(properties, maxLeafClauses, context, retriever, entitySpecs);
    } catch (SearchTemplateException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new SearchTemplateException("Search template is not valid", e);
    }
  }

  private static void checkProperties(
      @Nonnull SearchTemplateProperties properties,
      int maxLeafClauses,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable Collection<EntitySpec> entitySpecs) {
    rejectCombined(properties);
    rejectRouterBody(properties);
    if (properties.hasRoutes()) {
      SearchTemplateRoutes.compile(properties.getRoutes());
    }
    rejectListMembershipSnippet(
        properties.hasFunctionScore() ? properties.getFunctionScore() : null);
    checkFieldReferences(properties, context, retriever);
    checkBoosts(properties, context, retriever);
    if (entitySpecs != null) {
      checkOverrideMembership(
          properties.hasFields() ? properties.getFields() : null, entitySpecs, context, retriever);
    }
    if (properties.hasStructuredPropertyQuery()
        && properties.getStructuredPropertyQuery() != null) {
      checkStructuredPropertyQuery(properties.getStructuredPropertyQuery());
    }
    if (properties.hasRanking() && properties.getRanking() != null) {
      checkRanking(properties.getRanking());
    }
    if (properties.hasRetrieval() && properties.getRetrieval() != null) {
      checkRetrieval(properties.getRetrieval());
    }
    if (properties.hasLadder() && properties.getLadder() != null) {
      checkLadder(
          properties.getLadder(), properties.hasRetrieval() ? properties.getRetrieval() : null);
    }
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
    if (renderedRescore != null && sortClauseCount(renderedSort) > 0) {
      throw new SearchTemplateException("Search template rescore cannot be combined with sort");
    }
    if (renderedKnn != null) {
      SearchTemplateJson.parseKnn(renderedKnn);
    }
    if (renderedKnn != null && sortClauseCount(renderedSort) > 0) {
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
    leaves += rescoreLeaves(renderedRescore);
    if (leaves > maxLeafClauses) {
      throw new SearchTemplateException(
          "Search template has " + leaves + " leaf clauses and the limit is " + maxLeafClauses);
    }
  }

  private static void checkStructuredPropertyQuery(@Nonnull StructuredPropertyQuery section) {
    if (SearchTemplateLoader.emptyStructuredPropertyQuery(section)) {
      return;
    }
    Float weight = section.getNameAndValueWeight();
    if (weight == null || !(weight > 0f)) {
      throw new SearchTemplateException("Search template nameAndValueWeight must be positive");
    }
  }

  private static void checkRanking(@Nonnull SearchTemplateRanking ranking) {
    if (SearchTemplateLoader.emptyRanking(ranking)) {
      return;
    }
    if (ranking.hasPreset() && ranking.getPreset() != null && !ranking.getPreset().isBlank()) {
      requireClosed("ranking preset", ranking.getPreset(), SearchTemplateCapabilities.PRESETS);
    }
    if (ranking.hasWindowSizer()
        && ranking.getWindowSizer() != null
        && !ranking.getWindowSizer().isBlank()) {
      requireClosed(
          "ranking windowSizer", ranking.getWindowSizer(), SearchTemplateCapabilities.SIZERS);
    }
  }

  private static void checkRetrieval(@Nonnull SearchTemplateRetrieval retrieval) {
    if (SearchTemplateLoader.emptyRetrieval(retrieval)) {
      return;
    }
    if (retrieval.hasMode() && retrieval.getMode() != null && !retrieval.getMode().isBlank()) {
      requireClosed("retrieval mode", retrieval.getMode(), SearchTemplateCapabilities.MODES);
    }
  }

  private static void requireClosed(
      @Nonnull String field, @Nonnull String value, @Nonnull Set<String> allowed) {
    if (!allowed.contains(value)) {
      throw new SearchTemplateException("Search template " + field + " is not valid: " + value);
    }
  }

  static void checkLadder(
      @Nonnull SearchTemplateLadder ladder, @Nullable SearchTemplateRetrieval retrieval) {
    if (SearchTemplateLoader.emptyLadder(ladder)) {
      return;
    }
    boolean parallel =
        ladder.hasParallel() && ladder.getParallel() != null && !ladder.getParallel().isEmpty();
    boolean stop = hasStop(ladder);
    boolean next = ladder.hasNext() && ladder.getNext() != null;
    if (next && !parallel && !stop) {
      throw new SearchTemplateException("Search template next requires a stop condition");
    }
    if (parallel && (!ladder.hasJoin() || ladder.getJoin() == null || ladder.getJoin().isBlank())) {
      throw new SearchTemplateException("Search template parallel requires a join");
    }
    SearchTemplateLadderJoins join = null;
    if (ladder.hasJoin() && ladder.getJoin() != null && !ladder.getJoin().isBlank()) {
      join = SearchTemplateLadderJoins.parse(ladder.getJoin());
    }
    if (join == SearchTemplateLadderJoins.MERGE && next && !stop) {
      throw new SearchTemplateException("Search template merge next requires a stop condition");
    }
    if (ladder.hasFusion() && ladder.getFusion() != null && !ladder.getFusion().isBlank()) {
      requireClosed("fusion", ladder.getFusion(), SearchTemplateCapabilities.FUSIONS);
    }
    if (ladder.hasFacets() && ladder.getFacets() != null && !ladder.getFacets().isBlank()) {
      requireClosed("facets", ladder.getFacets(), SearchTemplateCapabilities.FACETS);
      boolean vector =
          retrieval != null && retrieval.hasMode() && "vector".equals(retrieval.getMode());
      if ("candidates".equals(ladder.getFacets()) && !vector) {
        throw new SearchTemplateException(
            "Search template facets candidates requires vector retrieval");
      }
    }
    if (ladder.hasSelect() && ladder.getSelect() != null) {
      for (String name : ladder.getSelect()) {
        if (!SearchTemplateLadderPredicates.SELECT.contains(name)) {
          throw new SearchTemplateException(
              "Search template select predicate is not valid: " + name);
        }
      }
    }
    if (ladder.hasStop() && ladder.getStop() != null) {
      for (SearchTemplateLadderStop condition : ladder.getStop()) {
        boolean minHits = condition.hasMinHits() && condition.getMinHits() != null;
        boolean emptyWhen =
            condition.hasEmptyWhen()
                && condition.getEmptyWhen() != null
                && !condition.getEmptyWhen().isBlank();
        if (!minHits && !emptyWhen) {
          throw new SearchTemplateException("Search template stop condition is empty");
        }
        if (emptyWhen
            && !SearchTemplateLadderPredicates.SHAPES.contains(condition.getEmptyWhen())) {
          throw new SearchTemplateException(
              "Search template emptyWhen predicate is not valid: " + condition.getEmptyWhen());
        }
      }
    }
  }

  private static boolean hasStop(@Nonnull SearchTemplateLadder ladder) {
    if (!ladder.hasStop() || ladder.getStop() == null || ladder.getStop().isEmpty()) {
      return false;
    }
    for (SearchTemplateLadderStop condition : ladder.getStop()) {
      if (condition.hasMinHits() && condition.getMinHits() != null) {
        return true;
      }
      if (condition.hasEmptyWhen()
          && condition.getEmptyWhen() != null
          && !condition.getEmptyWhen().isBlank()) {
        return true;
      }
    }
    return false;
  }

  private static void rejectRouterBody(@Nonnull SearchTemplateProperties properties) {
    if (!hasDefaultTarget(properties)) {
      return;
    }
    if (present(properties.hasQuery() ? properties.getQuery() : null)
        || presentFragment(properties)
        || present(properties.hasClauses() ? properties.getClauses() : null)
        || present(properties.hasFunctionScore() ? properties.getFunctionScore() : null)
        || present(properties.hasRescore() ? properties.getRescore() : null)
        || present(properties.hasKnn() ? properties.getKnn() : null)
        || (properties.hasRanking() && !SearchTemplateLoader.emptyRanking(properties.getRanking()))
        || (properties.hasRetrieval()
            && !SearchTemplateLoader.emptyRetrieval(properties.getRetrieval()))
        || (properties.hasLadder() && !SearchTemplateLoader.emptyLadder(properties.getLadder()))
        || present(properties.hasAggregations() ? properties.getAggregations() : null)
        || present(properties.hasSort() ? properties.getSort() : null)
        || (properties.hasFields() && !SearchTemplateLoader.emptyFields(properties.getFields()))
        || (properties.hasHighlightFields()
            && !SearchTemplateLoader.emptyHighlights(properties.getHighlightFields()))
        || (properties.hasStructuredPropertyQuery()
            && !SearchTemplateLoader.emptyStructuredPropertyQuery(
                properties.getStructuredPropertyQuery()))
        || (properties.hasBoosts()
            && properties.getBoosts() != null
            && !properties.getBoosts().isEmpty())
        || (properties.hasParameters()
            && properties.getParameters() != null
            && !properties.getParameters().isEmpty())) {
      throw new SearchTemplateException(
          "Search template default route cannot be combined with a body");
    }
  }

  private static boolean hasDefaultTarget(@Nonnull SearchTemplateProperties properties) {
    return properties.hasDefaultRoute()
        && properties.getDefaultRoute() != null
        && properties.getDefaultRoute().hasTemplate()
        && properties.getDefaultRoute().getTemplate() != null;
  }

  private static boolean presentFragment(@Nonnull SearchTemplateProperties properties) {
    if (!properties.hasQueryFragments() || properties.getQueryFragments() == null) {
      return false;
    }
    for (String value : properties.getQueryFragments().values()) {
      if (value != null && !value.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  private static boolean present(@Nullable String value) {
    return value != null && !value.isBlank();
  }

  private static void rejectCombined(@Nonnull SearchTemplateProperties properties) {
    if (properties.hasFields()) {
      SearchFieldOverrides fields = properties.getFields();
      boolean replace = fields.hasReplace() && !fields.getReplace().isEmpty();
      boolean add = fields.hasAdd() && !fields.getAdd().isEmpty();
      boolean remove = fields.hasRemove() && !fields.getRemove().isEmpty();
      boolean override = fields.hasOverride() && !fields.getOverride().isEmpty();
      if (replace && (add || remove || override)) {
        throw new SearchTemplateException(
            "Search template fields.replace cannot be combined with add, remove, or override");
      }
    }
    if (properties.hasHighlightFields()) {
      var highlights = properties.getHighlightFields();
      boolean replace = highlights.hasReplace() && !highlights.getReplace().isEmpty();
      boolean add = highlights.hasAdd() && !highlights.getAdd().isEmpty();
      boolean remove = highlights.hasRemove() && !highlights.getRemove().isEmpty();
      if (replace && (add || remove)) {
        throw new SearchTemplateException(
            "Search template highlightFields.replace cannot be combined with add or remove");
      }
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

  private static int sortClauseCount(@Nullable String sort) {
    if (sort == null) {
      return 0;
    }
    try {
      JsonNode node = MAPPER.readTree(sort);
      if (node.isArray()) {
        return node.size();
      }
      return node.isObject() && !node.isEmpty() ? 1 : 0;
    } catch (Exception e) {
      throw new SearchTemplateException("Search template sort is not valid JSON", e);
    }
  }

  private static int rescoreLeaves(@Nullable String rescore) {
    if (rescore == null) {
      return 0;
    }
    try {
      JsonNode node = MAPPER.readTree(rescore);
      JsonNode rescoreQuery = node.path("query").path("rescore_query");
      if (rescoreQuery.isMissingNode() || rescoreQuery.isNull()) {
        return SearchTemplateJson.countLeaves(rescore);
      }
      return SearchTemplateJson.countLeaves(rescoreQuery.toString());
    } catch (Exception e) {
      throw new SearchTemplateException("Search template rescore is not valid JSON", e);
    }
  }

  private static void rejectForbidden(@Nullable String json, @Nonnull String section) {
    if (json == null) {
      return;
    }
    List<String> forbidden = SearchTemplateJson.forbidden(json);
    if (!forbidden.isEmpty()) {
      throw new SearchTemplateException(
          "Search template " + section + " contains " + forbidden.get(0));
    }
  }

  private static void rejectListMembershipSnippet(@Nullable String functionScore) {
    if (functionScore == null) {
      return;
    }
    for (String injectable : SearchTemplateViewer.INJECTABLES) {
      if (functionScore.contains(injectable)) {
        throw new SearchTemplateException(
            "Search template functionScore cannot reference " + injectable);
      }
    }
  }

  private static void checkFieldReferences(
      @Nonnull SearchTemplateProperties properties,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (!properties.hasFields() || properties.getFields() == null) {
      return;
    }
    SearchFieldOverrides fields = properties.getFields();
    StructuredPropertyReferences.Resolver resolver =
        StructuredPropertyReferences.resolver(context, retriever);
    resolveBoosts(fields.hasAdd() ? fields.getAdd() : null, resolver, context, retriever);
    resolveBoosts(fields.hasOverride() ? fields.getOverride() : null, resolver, context, retriever);
    resolveBoosts(fields.hasReplace() ? fields.getReplace() : null, resolver, context, retriever);
    if (fields.hasRemove() && fields.getRemove() != null) {
      for (String name : fields.getRemove()) {
        resolver.resolveRemove(name);
      }
    }
  }

  private static void resolveBoosts(
      @Nullable SearchFieldBoostArray boosts,
      @Nonnull StructuredPropertyReferences.Resolver resolver,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (boosts == null) {
      return;
    }
    for (SearchFieldBoost boost : boosts) {
      String subfield = boost.hasSubfield() ? boost.getSubfield() : null;
      if (subfield != null
          && !subfield.isBlank()
          && !StructuredPropertyReferences.KEYWORD.equals(subfield)) {
        throw new SearchTemplateException("Search template subfield is not valid: " + subfield);
      }
      resolver.resolve(boost.getFieldName(), subfield);
    }
  }

  private static void checkBoosts(
      @Nonnull SearchTemplateProperties properties,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (!properties.hasBoosts() || properties.getBoosts() == null) {
      return;
    }
    StructuredPropertyReferences.Resolver resolver =
        StructuredPropertyReferences.resolver(context, retriever);
    for (SearchTemplateBoost boost : properties.getBoosts()) {
      if (!SearchTemplateViewer.INJECTABLES.contains(boost.getValues())) {
        throw new SearchTemplateException(
            "Search template boost values are not valid: " + boost.getValues());
      }
      resolver.resolve(boost.getField(), boost.hasSubfield() ? boost.getSubfield() : null);
    }
  }

  private static void checkOverrideMembership(
      @Nullable SearchFieldOverrides fields,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (fields == null || !fields.hasOverride() || fields.getOverride().isEmpty()) {
      return;
    }
    if (fields.hasReplace() && !fields.getReplace().isEmpty()) {
      return;
    }
    StructuredPropertyReferences.Resolver resolver =
        StructuredPropertyReferences.resolver(context, retriever);
    Set<String> queried = new LinkedHashSet<>();
    for (var field : SearchTemplateFields.pdlQueryFields(entitySpecs)) {
      queried.add(field.fieldName());
    }
    for (var field : StructuredPropertyQueryFields.fields()) {
      queried.add(field.fieldName());
    }
    if (fields.hasRemove() && fields.getRemove() != null) {
      for (String name : fields.getRemove()) {
        queried.remove(resolver.resolveRemove(name));
      }
    }
    if (fields.hasAdd() && fields.getAdd() != null) {
      for (SearchFieldBoost boost : fields.getAdd()) {
        queried.add(
            resolver.resolve(
                boost.getFieldName(), boost.hasSubfield() ? boost.getSubfield() : null));
      }
    }
    for (SearchFieldBoost boost : fields.getOverride()) {
      String name =
          resolver.resolve(boost.getFieldName(), boost.hasSubfield() ? boost.getSubfield() : null);
      if (!queried.contains(name)) {
        throw new SearchTemplateException(
            "Search template fields.override is not in the queried set: " + boost.getFieldName());
      }
    }
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
