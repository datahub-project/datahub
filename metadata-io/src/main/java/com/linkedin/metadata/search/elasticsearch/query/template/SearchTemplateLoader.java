package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalBridge;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.StructuredPropertyQuery;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Loads a template and merges its parent chain. Writes drop this URN and every child that parents
 * it.
 */
public class SearchTemplateLoader {
  public static final int MAX_PARENT_DEPTH = 5;

  private final Cache<String, MergedSearchTemplate> cache;
  private final SearchTemplateSource source;

  /**
   * Templates supplied by a test in place of the shipped file. Null reads {@link
   * SearchTemplateBootstrap}.
   */
  @Nullable private final Map<String, SearchTemplateProperties> catalog;

  public SearchTemplateLoader(long cacheTtlSeconds) {
    this(cacheTtlSeconds, SearchTemplateSource.STORAGE, null);
  }

  public SearchTemplateLoader(long cacheTtlSeconds, @Nonnull SearchTemplateSource source) {
    this(cacheTtlSeconds, source, null);
  }

  public SearchTemplateLoader(
      long cacheTtlSeconds,
      @Nonnull SearchTemplateSource source,
      @Nullable Map<String, SearchTemplateProperties> catalog) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder().maximumSize(500);
    if (cacheTtlSeconds > 0) {
      builder.expireAfterWrite(Duration.ofSeconds(cacheTtlSeconds));
    }
    this.cache = builder.build();
    this.source = source;
    this.catalog = catalog;
  }

  @Nonnull
  public MergedSearchTemplate load(@Nonnull OperationContext opContext, @Nonnull String urn) {
    return cache.get(urn, key -> merge(opContext, key));
  }

  /**
   * A seed that is neither stored nor shipped. A template that exists still merges and can fail.
   * Classpath mode does not look at storage.
   */
  @Nullable
  public MergedSearchTemplate find(@Nonnull OperationContext opContext, @Nonnull String urn) {
    if (source == SearchTemplateSource.CLASSPATH) {
      if (shipped(urn) == null) {
        return null;
      }
      SearchTemplateBootstrap.note(urn);
      return load(opContext, urn);
    }
    if (stored(opContext, urn)) {
      return load(opContext, urn);
    }
    if (shipped(urn) == null) {
      return null;
    }
    SearchTemplateBootstrap.note(urn);
    return load(opContext, urn);
  }

  /**
   * Merges {@code leaf} with parents loaded from storage. The leaf is not read from storage, so a
   * proposed write can be checked before it is committed. The result is not cached.
   */
  @Nonnull
  public MergedSearchTemplate preview(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn,
      @Nonnull SearchTemplateProperties leaf) {
    return compose(
        operationContext, retriever, urn, leaf, Map.of(), source == SearchTemplateSource.CLASSPATH);
  }

  /**
   * Merges {@code leaf} with parents loaded from storage, using {@code overrides} in place of
   * storage for those URNs. A proposed parent write can be checked against children before the
   * write is visible in storage.
   */
  @Nonnull
  public MergedSearchTemplate preview(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn,
      @Nonnull SearchTemplateProperties leaf,
      @Nonnull Map<String, SearchTemplateProperties> overrides) {
    return compose(
        operationContext,
        retriever,
        urn,
        leaf,
        overrides,
        source == SearchTemplateSource.CLASSPATH);
  }

  /** Drops the written URN and every cached template whose parent chain includes it. */
  public void invalidate(@Nonnull String urn) {
    List<String> drop = new ArrayList<>();
    cache
        .asMap()
        .forEach(
            (key, merged) -> {
              if (urn.equals(key) || merged.getChain().contains(urn)) {
                drop.add(key);
              }
            });
    cache.invalidateAll(drop);
  }

  @Nonnull
  private MergedSearchTemplate merge(@Nonnull OperationContext opContext, @Nonnull String urn) {
    AspectRetriever retriever = opContext.getAspectRetriever();
    boolean bridge = source == SearchTemplateSource.CLASSPATH || !stored(opContext, urn);
    return compose(opContext, retriever, urn, fetch(opContext, retriever, urn), Map.of(), bridge);
  }

  @Nonnull
  private MergedSearchTemplate compose(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn,
      @Nonnull SearchTemplateProperties leaf,
      @Nonnull Map<String, SearchTemplateProperties> overrides,
      boolean classpathBridge) {
    List<SearchTemplateProperties> chain = new ArrayList<>();
    List<String> urns = new ArrayList<>();
    chain.add(leaf);
    urns.add(urn);
    String current =
        leaf.hasParent() && leaf.getParent() != null ? leaf.getParent().toString() : null;
    for (int depth = 1; current != null; depth++) {
      if (urns.contains(current)) {
        throw new SearchTemplateException("Search template parent cycle at " + current);
      }
      if (depth > MAX_PARENT_DEPTH) {
        throw new SearchTemplateException(
            "Search template parent chain exceeds " + MAX_PARENT_DEPTH + " at " + urn);
      }
      urns.add(current);
      SearchTemplateProperties properties =
          resolve(operationContext, retriever, current, overrides);
      chain.add(properties);
      current =
          properties.hasParent() && properties.getParent() != null
              ? properties.getParent().toString()
              : null;
    }

    Map<String, String> parameters = new LinkedHashMap<>();
    Map<String, List<String>> queryFragmentParts = new LinkedHashMap<>();
    long lastModified = 0L;
    MergedSearchTemplate.MergedSearchTemplateBuilder builder =
        MergedSearchTemplate.builder().urn(urn).chain(List.copyOf(urns));
    StructuredPropertyReferences.Resolver resolver =
        classpathBridge
            ? StructuredPropertyReferences.omitting()
            : StructuredPropertyReferences.resolver(operationContext, retriever);
    SearchFieldOverrides fields = null;
    HighlightFieldOverrides highlights = null;
    SearchTemplateBoostArray boosts = null;
    StructuredPropertyQuery structuredPropertyQuery = null;
    List<SearchTemplateRoute> routes = null;
    String defaultRoute = null;
    boolean defaultRouteSet = false;
    // Root first so the child replaces sections it sets.
    for (int i = chain.size() - 1; i >= 0; i--) {
      SearchTemplateProperties props = chain.get(i);
      overlay(builder, props);
      if (props.hasFields()) {
        fields = SearchTemplateFields.merge(fields, props.getFields(), resolver);
      }
      if (props.hasHighlightFields()) {
        highlights =
            SearchTemplateFields.mergeHighlights(highlights, props.getHighlightFields(), resolver);
      }
      if (props.hasBoosts()) {
        boosts =
            props.getBoosts() == null || props.getBoosts().isEmpty()
                ? null
                : new SearchTemplateBoostArray(props.getBoosts());
      }
      if (props.hasStructuredPropertyQuery()) {
        StructuredPropertyQuery section = props.getStructuredPropertyQuery();
        structuredPropertyQuery = emptyStructuredPropertyQuery(section) ? null : section;
      }
      if (props.hasParameters() && props.getParameters() != null) {
        parameters.putAll(props.getParameters());
      }
      if (props.hasQueryFragments() && props.getQueryFragments() != null) {
        props
            .getQueryFragments()
            .forEach(
                (name, value) -> {
                  if (value == null || value.isEmpty()) {
                    queryFragmentParts.remove(name);
                  } else {
                    queryFragmentParts
                        .computeIfAbsent(name, ignored -> new ArrayList<>())
                        .add(value);
                  }
                });
      }
      if (props.hasRoutes()) {
        routes =
            props.getRoutes() == null || props.getRoutes().isEmpty()
                ? List.of()
                : List.copyOf(props.getRoutes());
      }
      if (props.hasDefaultRoute()) {
        defaultRouteSet = true;
        SearchTemplateDefaultRoute section = props.getDefaultRoute();
        defaultRoute =
            section != null && section.hasTemplate() && section.getTemplate() != null
                ? section.getTemplate().toString()
                : null;
      }
      if (props.hasLastModified() && props.getLastModified() != null) {
        lastModified = Math.max(lastModified, props.getLastModified().getTime());
      }
    }
    Map<String, String> queryFragments = new LinkedHashMap<>();
    queryFragmentParts.forEach((name, parts) -> queryFragments.put(name, String.join(", ", parts)));
    builder.dialect(chain.get(0).getDialect().name());
    builder.parameters(parameters);
    builder.queryFragments(queryFragments);
    builder.lastModified(lastModified);
    builder.fields(fields);
    builder.highlightFields(highlights);
    builder.boosts(boosts);
    builder.structuredPropertyQuery(structuredPropertyQuery);
    builder.routes(SearchTemplateRoutes.compile(routes));
    builder.defaultRoute(defaultRouteSet ? defaultRoute : null);
    builder.classpathBridge(classpathBridge);
    return builder.build();
  }

  private static void overlay(
      MergedSearchTemplate.MergedSearchTemplateBuilder builder, SearchTemplateProperties props) {
    if (props.hasQuery()) {
      builder.query(blankToNull(props.getQuery()));
    }
    if (props.hasClauses()) {
      builder.clauses(blankToNull(props.getClauses()));
    }
    if (props.hasFunctionScore()) {
      builder.functionScore(blankToNull(props.getFunctionScore()));
    }
    if (props.hasRescore()) {
      builder.rescore(blankToNull(props.getRescore()));
    }
    if (props.hasKnn()) {
      builder.knn(blankToNull(props.getKnn()));
    }
    if (props.hasRanking()) {
      builder.ranking(emptyRanking(props.getRanking()) ? null : props.getRanking());
    }
    if (props.hasRetrieval()) {
      builder.retrieval(emptyRetrieval(props.getRetrieval()) ? null : props.getRetrieval());
    }
    if (props.hasLadder()) {
      builder.ladder(emptyLadder(props.getLadder()) ? null : props.getLadder());
    }
    if (props.hasAggregations()) {
      builder.aggregations(blankToNull(props.getAggregations()));
    }
    if (props.hasSort()) {
      builder.sort(blankToNull(props.getSort()));
    }
  }

  @Nonnull
  private SearchTemplateProperties resolve(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn,
      @Nonnull Map<String, SearchTemplateProperties> overrides) {
    SearchTemplateProperties overridden = overrides.get(urn);
    if (overridden != null) {
      return overridden;
    }
    return fetch(operationContext, retriever, urn);
  }

  @Nonnull
  private SearchTemplateProperties fetch(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn) {
    if (source != SearchTemplateSource.CLASSPATH) {
      Urn parsed = UrnUtils.getUrn(urn);
      Aspect aspect =
          retriever.getLatestAspectObject(
              operationContext, parsed, SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
      if (aspect != null && aspect.data() != null) {
        return new SearchTemplateProperties(aspect.data());
      }
    }
    SearchTemplateProperties shipped = shipped(urn);
    if (shipped != null) {
      SearchTemplateBootstrap.note(urn);
      return shipped;
    }
    throw new SearchTemplateException("Search template not found: " + urn);
  }

  private boolean stored(@Nonnull OperationContext opContext, @Nonnull String urn) {
    if (source == SearchTemplateSource.CLASSPATH) {
      return false;
    }
    Aspect aspect =
        opContext
            .getAspectRetriever()
            .getLatestAspectObject(
                opContext, UrnUtils.getUrn(urn), SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    return aspect != null && aspect.data() != null;
  }

  @Nullable
  private SearchTemplateProperties shipped(@Nonnull String urn) {
    if (catalog != null) {
      SearchTemplateProperties properties = catalog.get(urn);
      if (properties == null) {
        return null;
      }
      try {
        return properties.copy();
      } catch (CloneNotSupportedException e) {
        throw new IllegalStateException(e);
      }
    }
    return SearchTemplateBootstrap.get(urn);
  }

  @Nullable
  private static String blankToNull(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }

  static boolean emptyFields(@Nullable SearchFieldOverrides overrides) {
    if (overrides == null) {
      return true;
    }
    boolean add = !overrides.hasAdd() || overrides.getAdd().isEmpty();
    boolean remove = !overrides.hasRemove() || overrides.getRemove().isEmpty();
    boolean replace = !overrides.hasReplace() || overrides.getReplace().isEmpty();
    boolean override = !overrides.hasOverride() || overrides.getOverride().isEmpty();
    return add && remove && replace && override;
  }

  public static boolean emptyLadder(@Nullable SearchTemplateLadder ladder) {
    if (ladder == null) {
      return true;
    }
    boolean select = !ladder.hasSelect() || ladder.getSelect().isEmpty();
    boolean stop = !ladder.hasStop() || ladder.getStop().isEmpty();
    boolean next = !ladder.hasNext() || ladder.getNext() == null;
    boolean parallel = !ladder.hasParallel() || ladder.getParallel().isEmpty();
    boolean fusion = !ladder.hasFusion() || blank(ladder.getFusion());
    boolean fusionCap = !ladder.hasFusionCap() || ladder.getFusionCap() == null;
    boolean facets = !ladder.hasFacets() || blank(ladder.getFacets());
    return select && stop && next && parallel && !ladder.hasJoin() && fusion && fusionCap && facets;
  }

  public static boolean emptyStructuredPropertyQuery(@Nullable StructuredPropertyQuery section) {
    if (section == null) {
      return true;
    }
    return !section.hasNameAndValueWeight() || section.getNameAndValueWeight() == null;
  }

  public static boolean emptyRanking(@Nullable SearchTemplateRanking ranking) {
    if (ranking == null) {
      return true;
    }
    boolean preset = !ranking.hasPreset() || blank(ranking.getPreset());
    boolean window = !ranking.hasWindow() || ranking.getWindow() == null;
    boolean sizer = !ranking.hasWindowSizer() || blank(ranking.getWindowSizer());
    boolean parameters =
        !ranking.hasParameters()
            || ranking.getParameters() == null
            || ranking.getParameters().isEmpty();
    return preset && window && sizer && parameters;
  }

  public static boolean emptyRetrieval(@Nullable SearchTemplateRetrieval retrieval) {
    if (retrieval == null) {
      return true;
    }
    boolean mode = !retrieval.hasMode() || blank(retrieval.getMode());
    boolean model = !retrieval.hasModel() || blank(retrieval.getModel());
    boolean field = !retrieval.hasField() || blank(retrieval.getField());
    boolean cap = !retrieval.hasCandidateCap() || retrieval.getCandidateCap() == null;
    boolean total = !retrieval.hasTotal() || blank(retrieval.getTotal());
    boolean facets = !retrieval.hasFacets() || blank(retrieval.getFacets());
    boolean page = !retrieval.hasPage() || blank(retrieval.getPage());
    boolean authorization = !retrieval.hasAuthorization() || blank(retrieval.getAuthorization());
    boolean onFailure = !retrieval.hasOnFailure() || blank(retrieval.getOnFailure());
    boolean bridge = !retrieval.hasBridge() || emptyBridge(retrieval.getBridge());
    return mode
        && model
        && field
        && cap
        && total
        && facets
        && page
        && authorization
        && onFailure
        && bridge;
  }

  private static boolean emptyBridge(@Nullable SearchTemplateRetrievalBridge bridge) {
    if (bridge == null) {
      return true;
    }
    boolean pivot = !bridge.hasPivotField() || blank(bridge.getPivotField());
    boolean onFailure = !bridge.hasOnFailure() || blank(bridge.getOnFailure());
    return pivot && onFailure;
  }

  private static boolean blank(@Nullable String value) {
    return value == null || value.isBlank();
  }

  static boolean emptyHighlights(@Nullable HighlightFieldOverrides overrides) {
    if (overrides == null) {
      return true;
    }
    boolean add = !overrides.hasAdd() || overrides.getAdd().isEmpty();
    boolean remove = !overrides.hasRemove() || overrides.getRemove().isEmpty();
    boolean replace = !overrides.hasReplace() || overrides.getReplace().isEmpty();
    return add && remove && replace;
  }

  /** Exposed for a parent walk. Missing templates return null. */
  @Nullable
  public SearchTemplateProperties read(@Nonnull OperationContext opContext, @Nonnull String urn) {
    try {
      return fetch(opContext, opContext.getAspectRetriever(), urn);
    } catch (SearchTemplateException e) {
      return null;
    }
  }
}
