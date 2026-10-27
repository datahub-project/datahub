package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.search.template.SearchTemplateComponentMode;
import com.linkedin.search.template.SearchTemplateProperties;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Shared component rules: empty records, skip conditions, the subset invariant, and cache identity.
 */
public final class SearchTemplateComponents {
  private SearchTemplateComponents() {}

  public static boolean empty(
      @Nullable com.linkedin.search.template.SearchTemplateComponent component) {
    if (component == null) {
      return true;
    }
    boolean name = !component.hasName() || blank(component.getName());
    boolean version = !component.hasVersion() || component.getVersion() == null;
    boolean mode = !component.hasMode() || component.getMode() == null;
    boolean config =
        !component.hasConfig() || component.getConfig() == null || component.getConfig().isEmpty();
    return name && version && mode && config;
  }

  /**
   * Write-time check. An OWN_RUNG component cannot be stored beside query or ranking sections. An
   * unknown name fails closed.
   */
  public static void checkWrite(
      @Nonnull SearchTemplateProperties properties,
      @Nonnull SearchTemplateComponentRegistry registry) {
    if (!properties.hasComponent() || empty(properties.getComponent())) {
      return;
    }
    com.linkedin.search.template.SearchTemplateComponent component = properties.getComponent();
    String name = component.hasName() ? component.getName() : null;
    Integer version = component.hasVersion() ? component.getVersion() : null;
    SearchTemplateComponentMode mode = component.hasMode() ? component.getMode() : null;
    if (name == null || name.isBlank() || version == null || mode == null) {
      throw new SearchTemplateException(
          "Search template component requires name, version, and mode");
    }
    Map<String, String> config = config(component);
    SearchTemplateComponent implementation = registry.require(name, version);
    implementation.validate(mode, config);
    if (mode == SearchTemplateComponentMode.OWN_RUNG && mixesQuerySections(properties)) {
      throw new SearchTemplateException(
          "Search template OWN_RUNG component cannot be combined with query or ranking sections");
    }
  }

  @Nonnull
  public static Map<String, String> config(
      @Nonnull com.linkedin.search.template.SearchTemplateComponent component) {
    if (!component.hasConfig() || component.getConfig() == null) {
      return Map.of();
    }
    StringMap stored = component.getConfig();
    Map<String, String> copy = new TreeMap<>();
    stored.forEach((key, value) -> copy.put(key, value == null ? "" : value));
    return Map.copyOf(copy);
  }

  @Nonnull
  public static String cacheSuffix(
      @Nullable com.linkedin.search.template.SearchTemplateComponent component) {
    if (empty(component)) {
      return "";
    }
    return "|component="
        + component.getName()
        + ":"
        + component.getVersion()
        + ":"
        + component.getMode()
        + ":"
        + canonical(config(component));
  }

  public static boolean skip(
      boolean scroll,
      @Nullable Integer size,
      @Nullable List<SortCriterion> sort,
      int from,
      int window) {
    return scroll || (size != null && size == 0) || !relevanceSort(sort) || from >= window;
  }

  /**
   * POST_PROCESS reorder. The engine total and facets stay on {@code page}. Output hits must be a
   * duplicate-free subset of the fetched hits. Pages past the window are not passed here.
   */
  public static void applyPostProcess(
      @Nonnull SearchTemplateComponentRegistry registry,
      @Nonnull MergedSearchTemplate template,
      @Nonnull SearchTemplateComponentRequest request,
      @Nonnull SearchResult page) {
    com.linkedin.search.template.SearchTemplateComponent spec = template.getComponent();
    if (empty(spec) || spec.getMode() != SearchTemplateComponentMode.POST_PROCESS) {
      return;
    }
    SearchTemplateComponent implementation = registry.require(spec.getName(), spec.getVersion());
    int window = implementation.plan(request, request.getConfig());
    if (skip(request.isScroll(), request.getSize(), request.getSort(), request.getFrom(), window)) {
      return;
    }
    SearchTemplateComponentPage ranked = implementation.execute(request);
    List<SearchEntity> input = request.getHits();
    List<SearchEntity> ordered = enforceSubset(input, ranked.hits());
    int start = Math.min(Math.max(request.getFrom(), 0), ordered.size());
    int requested = request.getSize() == null ? ordered.size() - start : request.getSize();
    int end = Math.min(start + Math.max(requested, 0), ordered.size());
    page.setEntities(new SearchEntityArray(ordered.subList(start, end)));
    page.setFrom(request.getFrom());
    if (request.getSize() != null) {
      page.setPageSize(request.getSize());
    }
  }

  @Nonnull
  public static SearchTemplateWalk.AttemptResult<SearchResult> reorder(
      @Nonnull SearchTemplateComponentRegistry registry,
      @Nonnull MergedSearchTemplate template,
      @Nonnull SearchTemplateComponentRequest request,
      @Nonnull SearchTemplateWalk.AttemptResult<SearchResult> raw) {
    SearchResult page = raw.page();
    if (page == null || SearchTemplateComponents.empty(template.getComponent())) {
      return raw;
    }
    applyPostProcess(registry, template, request, page);
    List<SearchEntity> entities =
        page.getEntities() == null ? List.of() : new ArrayList<>(page.getEntities());
    return SearchTemplateWalk.AttemptResult.ok(
        page,
        page.getNumEntities(),
        entities,
        raw.highlights(),
        raw.shardFailed,
        raw.renderedQuery());
  }

  @Nonnull
  public static List<SearchEntity> enforceSubset(
      @Nonnull List<SearchEntity> input, @Nonnull List<SearchEntity> output) {
    Set<String> allowed = new LinkedHashSet<>();
    for (SearchEntity hit : input) {
      allowed.add(urn(hit));
    }
    Set<String> seen = new LinkedHashSet<>();
    List<SearchEntity> ordered = new ArrayList<>();
    for (SearchEntity hit : output) {
      String urn = urn(hit);
      if (!allowed.contains(urn)) {
        throw new SearchTemplateException(
            "Search template component returned a hit outside the fetched window: " + urn);
      }
      if (!seen.add(urn)) {
        throw new SearchTemplateException(
            "Search template component returned a duplicate hit: " + urn);
      }
      ordered.add(hit);
    }
    return List.copyOf(ordered);
  }

  @Nonnull
  private static String urn(@Nonnull SearchEntity hit) {
    if (hit.getEntity() == null) {
      throw new SearchTemplateException("Search template component returned a hit without an URN");
    }
    return hit.getEntity().toString();
  }

  private static boolean mixesQuerySections(@Nonnull SearchTemplateProperties properties) {
    if (properties.hasQuery() && !blank(properties.getQuery())) {
      return true;
    }
    if (properties.hasClauses() && !blank(properties.getClauses())) {
      return true;
    }
    if (properties.hasFunctionScore() && !blank(properties.getFunctionScore())) {
      return true;
    }
    if (properties.hasRescore() && !blank(properties.getRescore())) {
      return true;
    }
    if (properties.hasKnn() && !blank(properties.getKnn())) {
      return true;
    }
    if (properties.hasAggregations() && !blank(properties.getAggregations())) {
      return true;
    }
    if (properties.hasSort() && !blank(properties.getSort())) {
      return true;
    }
    if (properties.hasRanking() && !SearchTemplateLoader.emptyRanking(properties.getRanking())) {
      return true;
    }
    if (properties.hasRetrieval()
        && !SearchTemplateLoader.emptyRetrieval(properties.getRetrieval())) {
      return true;
    }
    if (properties.hasBoosts()
        && properties.getBoosts() != null
        && !properties.getBoosts().isEmpty()) {
      return true;
    }
    if (properties.hasFields() && !SearchTemplateLoader.emptyFields(properties.getFields())) {
      return true;
    }
    if (properties.hasFieldValueScores()
        && properties.getFieldValueScores() != null
        && !properties.getFieldValueScores().isEmpty()) {
      return true;
    }
    if (properties.hasFieldFactors()
        && properties.getFieldFactors() != null
        && !properties.getFieldFactors().isEmpty()) {
      return true;
    }
    if (properties.hasTermWeights()
        && properties.getTermWeights() != null
        && !properties.getTermWeights().isEmpty()) {
      return true;
    }
    if (properties.hasScoreMode() || properties.hasBoostMode()) {
      return true;
    }
    if (properties.hasEntityTypeBoosts()
        && properties.getEntityTypeBoosts() != null
        && !properties.getEntityTypeBoosts().isEmpty()) {
      return true;
    }
    return properties.hasStructuredPropertyQuery()
        && !SearchTemplateLoader.emptyStructuredPropertyQuery(
            properties.getStructuredPropertyQuery());
  }

  private static boolean relevanceSort(@Nullable List<SortCriterion> sort) {
    if (sort == null || sort.isEmpty()) {
      return true;
    }
    for (SortCriterion criterion : sort) {
      String field = criterion.getField();
      boolean score = "_score".equals(field) || "score".equals(field);
      if (!score || criterion.getOrder() == SortOrder.ASCENDING) {
        return false;
      }
    }
    return true;
  }

  @Nonnull
  private static String canonical(@Nonnull Map<String, String> config) {
    StringBuilder builder = new StringBuilder();
    config.forEach(
        (key, value) -> {
          if (builder.length() > 0) {
            builder.append(',');
          }
          builder.append(key).append('=').append(value);
        });
    return builder.toString();
  }

  private static boolean blank(@Nullable String value) {
    return value == null || value.isBlank();
  }
}
