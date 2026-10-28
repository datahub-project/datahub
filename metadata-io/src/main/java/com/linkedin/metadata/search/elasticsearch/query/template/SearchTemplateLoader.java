package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.search.SearchTemplateSource;
import com.linkedin.search.template.EntityTypeBoostArray;
import com.linkedin.search.template.FieldFactorArray;
import com.linkedin.search.template.FieldValueScoreArray;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.RankingSignal;
import com.linkedin.search.template.RankingSignalArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateNormalization;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalBridge;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.StructuredPropertyQuery;
import com.linkedin.search.template.TermWeightArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Loads a template and merges its parent chain. Every merged template this process has loaded is
 * replaced together. A snapshot stays fresh for {@code cache.primary.ttlSeconds}, measured from the
 * moment its reads started. {@code ttlSeconds <= 0} does not expire.
 */
@Slf4j
public class SearchTemplateLoader {
  private final Object reloadLock = new Object();
  private volatile Snapshot published;
  @Nullable private volatile SearchTemplateDocuments documents;
  private final long cacheTtlMillis;
  private final int cacheMaximumSize;
  private final LongSupplier clock;
  private final SearchTemplateSource source;
  private final int maxParentDepth;
  private final int maxRoutes;
  private final int maxRoutePatternLength;

  /**
   * Parent-chain and ladder ceiling from {@code elasticsearch.search.v3.templates.maxParentDepth}.
   */
  public int maxParentDepth() {
    return maxParentDepth;
  }

  /**
   * Templates supplied by a test in place of the shipped file. Null reads {@link
   * SearchTemplateBootstrap}.
   */
  @Nullable private final Map<String, SearchTemplateProperties> catalog;

  public SearchTemplateLoader(long cacheTtlSeconds) {
    this(cacheTtlSeconds, SearchTemplateSource.STORAGE, null);
  }

  SearchTemplateLoader(long cacheTtlSeconds, @Nonnull LongSupplier clock) {
    this(
        cacheTtlSeconds,
        SearchTemplateSource.STORAGE,
        null,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        clock);
  }

  public SearchTemplateLoader(long cacheTtlSeconds, @Nonnull SearchTemplateSource source) {
    this(cacheTtlSeconds, source, null);
  }

  SearchTemplateLoader(
      long cacheTtlSeconds,
      @Nonnull SearchTemplateSource source,
      @Nullable Map<String, SearchTemplateProperties> catalog,
      @Nonnull LongSupplier clock) {
    this(
        cacheTtlSeconds,
        source,
        catalog,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        clock);
  }

  public SearchTemplateLoader(
      long cacheTtlSeconds,
      @Nonnull SearchTemplateSource source,
      @Nullable Map<String, SearchTemplateProperties> catalog) {
    this(
        cacheTtlSeconds,
        source,
        catalog,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE);
  }

  public SearchTemplateLoader(
      long cacheTtlSeconds,
      @Nonnull SearchTemplateSource source,
      @Nullable Map<String, SearchTemplateProperties> catalog,
      int maxParentDepth,
      int cacheMaximumSize,
      int maxRoutes,
      int maxRoutePatternLength) {
    this(
        cacheTtlSeconds,
        source,
        catalog,
        maxParentDepth,
        cacheMaximumSize,
        maxRoutes,
        maxRoutePatternLength,
        System::currentTimeMillis);
  }

  SearchTemplateLoader(
      long cacheTtlSeconds,
      @Nonnull SearchTemplateSource source,
      @Nullable Map<String, SearchTemplateProperties> catalog,
      int maxParentDepth,
      int cacheMaximumSize,
      int maxRoutes,
      int maxRoutePatternLength,
      @Nonnull LongSupplier clock) {
    if (maxParentDepth <= 0) {
      throw new IllegalArgumentException(
          "elasticsearch.search.v3.templates.maxParentDepth must be positive");
    }
    if (cacheMaximumSize <= 0) {
      throw new IllegalArgumentException(
          "elasticsearch.search.v3.templates.cacheMaximumSize must be positive");
    }
    if (maxRoutes <= 0 || maxRoutePatternLength <= 0) {
      throw new IllegalArgumentException(
          "elasticsearch.search.v3.templates maxRoutes and maxRoutePatternLength must be positive");
    }
    this.maxParentDepth = maxParentDepth;
    this.maxRoutes = maxRoutes;
    this.maxRoutePatternLength = maxRoutePatternLength;
    this.cacheMaximumSize = cacheMaximumSize;
    this.cacheTtlMillis = cacheTtlSeconds <= 0 ? 0 : cacheTtlSeconds * 1000L;
    this.clock = clock;
    this.source = source;
    this.catalog = catalog;
  }

  /** Search documents for a storage refresh. Installed after the search client exists. */
  public void installDocuments(@Nullable SearchTemplateDocuments documents) {
    this.documents = documents;
  }

  @Nonnull
  public MergedSearchTemplate load(@Nonnull OperationContext opContext, @Nonnull String urn) {
    Snapshot current = published;
    MergedSearchTemplate hit = fresh(current, urn);
    if (hit != null) {
      SearchTemplateMetrics.cache(opContext, age(current));
      return hit;
    }
    synchronized (reloadLock) {
      current = published;
      hit = fresh(current, urn);
      if (hit != null) {
        SearchTemplateMetrics.cache(opContext, age(current));
        return hit;
      }
      long readStart = clock.getAsLong();
      try {
        Snapshot next = refresh(opContext, urn, current, readStart);
        Snapshot latest = published;
        // The reload lock serializes refreshes, so an equal read-start is this refresh: the
        // clock did not move while a new template was added. An older read-start is a refresh
        // that observed storage before the snapshot already published.
        if (latest == null || readStart >= latest.readStartedAtMillis) {
          published = latest = next;
        }
        MergedSearchTemplate served = latest.templates.get(urn);
        if (served == null) {
          SearchTemplateException rejected = latest.rejected.get(urn);
          if (rejected != null) {
            throw rejected;
          }
          throw new SearchTemplateException("Search template not found: " + urn);
        }
        return served;
      } catch (RuntimeException e) {
        Snapshot kept = published;
        if (kept != null && kept.templates.containsKey(urn)) {
          log.warn(
              "Search template cache reload failed for {}; keeping the previous cache", urn, e);
          return kept.templates.get(urn);
        }
        throw e;
      }
    }
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
        operationContext,
        retriever,
        urn,
        new TemplateCopy(leaf, 0L),
        Map.of(),
        source == SearchTemplateSource.CLASSPATH,
        true);
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
        new TemplateCopy(leaf, 0L),
        copies(overrides),
        source == SearchTemplateSource.CLASSPATH,
        true);
  }

  @Nonnull
  private static Map<String, TemplateCopy> copies(
      @Nonnull Map<String, SearchTemplateProperties> overrides) {
    Map<String, TemplateCopy> copies = new LinkedHashMap<>();
    overrides.forEach((urn, properties) -> copies.put(urn, new TemplateCopy(properties, 0L)));
    return copies;
  }

  @Nonnull
  private MergedSearchTemplate mergeComposed(
      @Nonnull OperationContext opContext, @Nonnull String urn) {
    AspectRetriever retriever = opContext.getAspectRetriever();
    boolean bridge = source == SearchTemplateSource.CLASSPATH || !stored(opContext, urn);
    return compose(
        opContext, retriever, urn, fetch(opContext, retriever, urn), Map.of(), bridge, true);
  }

  @Nonnull
  private MergedSearchTemplate compose(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn,
      @Nonnull TemplateCopy leaf,
      @Nonnull Map<String, TemplateCopy> overrides,
      boolean classpathBridge,
      boolean resolveReferences) {
    List<TemplateCopy> chain = new ArrayList<>();
    List<String> urns = new ArrayList<>();
    chain.add(leaf);
    urns.add(urn);
    String current =
        leaf.properties().hasParent() && leaf.properties().getParent() != null
            ? leaf.properties().getParent().toString()
            : null;
    for (int depth = 1; current != null; depth++) {
      if (urns.contains(current)) {
        throw new SearchTemplateException("Search template parent cycle at " + current);
      }
      if (depth > maxParentDepth) {
        throw new SearchTemplateException(
            "Search template parent chain exceeds " + maxParentDepth + " at " + urn);
      }
      urns.add(current);
      TemplateCopy resolved = resolve(operationContext, retriever, current, overrides);
      chain.add(resolved);
      current =
          resolved.properties().hasParent() && resolved.properties().getParent() != null
              ? resolved.properties().getParent().toString()
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
    FieldValueScoreArray fieldValueScores = null;
    FieldFactorArray fieldFactors = null;
    TermWeightArray termWeights = null;
    EntityTypeBoostArray entityTypeBoosts = null;
    SearchTemplateNormalization normalization = null;
    List<SearchTemplateRoute> routes = null;
    String defaultRoute = null;
    boolean defaultRouteSet = false;
    Integer apiVersion = null;
    List<String> capabilities = null;
    SearchTemplateRanking ranking = null;
    Set<String> addSatisfiedRemoves = new LinkedHashSet<>();
    // Root first so the child replaces sections it sets.
    for (int i = chain.size() - 1; i >= 0; i--) {
      TemplateCopy copy = chain.get(i);
      SearchTemplateProperties props = copy.properties();
      SearchTemplateCapabilities.requireDeclared(props);
      if (props.hasApiVersion() && props.getApiVersion() != null) {
        apiVersion = props.getApiVersion();
      }
      if (props.hasCapabilities()) {
        capabilities = capabilityList(props.getCapabilities());
      }
      overlay(builder, props);
      if (props.hasRanking()) {
        ranking = mergeRanking(ranking, props.getRanking(), addSatisfiedRemoves);
      }
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
        structuredPropertyQuery =
            StructuredPropertyClauses.merge(
                structuredPropertyQuery, props.getStructuredPropertyQuery());
      }
      if (props.hasFieldValueScores()) {
        fieldValueScores =
            props.getFieldValueScores() == null || props.getFieldValueScores().isEmpty()
                ? null
                : new FieldValueScoreArray(props.getFieldValueScores());
      }
      if (props.hasFieldFactors()) {
        fieldFactors =
            props.getFieldFactors() == null || props.getFieldFactors().isEmpty()
                ? null
                : new FieldFactorArray(props.getFieldFactors());
      }
      if (props.hasTermWeights()) {
        termWeights =
            props.getTermWeights() == null || props.getTermWeights().isEmpty()
                ? null
                : new TermWeightArray(props.getTermWeights());
      }
      if (props.hasEntityTypeBoosts()) {
        entityTypeBoosts =
            props.getEntityTypeBoosts() == null || props.getEntityTypeBoosts().isEmpty()
                ? null
                : new EntityTypeBoostArray(props.getEntityTypeBoosts());
      }
      if (props.hasNormalization()) {
        normalization =
            SearchTemplateNormalizationPolicy.merge(normalization, props.getNormalization());
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
      lastModified = Math.max(lastModified, copy.modifiedAt());
    }
    Map<String, String> queryFragments = new LinkedHashMap<>();
    queryFragmentParts.forEach((name, parts) -> queryFragments.put(name, String.join(", ", parts)));
    builder.dialect(chain.get(0).properties().getDialect().name());
    builder.apiVersion(apiVersion == null ? SearchTemplateCapabilities.API_VERSION : apiVersion);
    builder.capabilities(capabilities == null ? List.of() : capabilities);
    builder.parameters(parameters);
    builder.queryFragments(queryFragments);
    builder.lastModified(lastModified);
    builder.fields(fields);
    builder.highlightFields(highlights);
    builder.boosts(boosts);
    builder.structuredPropertyQuery(structuredPropertyQuery);
    builder.fieldValueScores(fieldValueScores);
    builder.fieldFactors(fieldFactors);
    builder.termWeights(termWeights);
    builder.entityTypeBoosts(entityTypeBoosts);
    builder.normalization(normalization);
    builder.routes(SearchTemplateRoutes.compile(routes, maxRoutes, maxRoutePatternLength));
    builder.defaultRoute(defaultRouteSet ? defaultRoute : null);
    builder.classpathBridge(classpathBridge);
    List<RankingSignal> pendingAdds = rankingAdds(ranking);
    List<String> pendingRemoves = rankingRemoves(ranking);
    String rankingReference = rankingReferenceUrn(ranking);
    if (resolveReferences) {
      ResolvedRanking resolved =
          resolveRankingReference(
              operationContext, retriever, urn, ranking, overrides, classpathBridge, urns);
      ranking = resolved.ranking();
      if (rankingReference != null) {
        lastModified = Math.max(lastModified, resolved.lastModified());
        builder.chain(List.copyOf(urns));
        builder.lastModified(lastModified);
      }
    }
    if (resolveReferences || !referencesTemplate(ranking)) {
      ranking = applyRankingPatches(ranking, pendingAdds, pendingRemoves, addSatisfiedRemoves);
    }
    if (ranking != null) {
      boolean resolvedSignals =
          ranking.hasSignals() && ranking.getSignals() != null && !ranking.getSignals().isEmpty();
      boolean resolvedFormula =
          ranking.hasFormula() && ranking.getFormula() != null && !ranking.getFormula().isBlank();
      if (resolvedSignals && !hasCapability(capabilities, "rankingSignals")) {
        throw new SearchTemplateException("Search template capability is required: rankingSignals");
      }
      if (resolvedFormula && !hasCapability(capabilities, "rankingFormula")) {
        throw new SearchTemplateException("Search template capability is required: rankingFormula");
      }
    }
    builder.ranking(ranking);
    MergedSearchTemplate merged = builder.build();
    SearchTemplateCapabilities.requireContract(merged);
    return merged;
  }

  @Nonnull
  private static List<String> capabilityList(@Nullable StringArray declared) {
    if (declared == null || declared.isEmpty()) {
      return List.of();
    }
    return List.copyOf(declared);
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
    if (props.hasScoreMode() && props.getScoreMode() != null) {
      builder.scoreMode(props.getScoreMode());
    }
    if (props.hasBoostMode() && props.getBoostMode() != null) {
      builder.boostMode(props.getBoostMode());
    }
    if (props.hasRescore()) {
      builder.rescore(blankToNull(props.getRescore()));
    }
    if (props.hasKnn()) {
      builder.knn(blankToNull(props.getKnn()));
    }
    if (props.hasComponent()) {
      builder.component(
          SearchTemplateComponents.empty(props.getComponent()) ? null : props.getComponent());
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
  private TemplateCopy resolve(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn,
      @Nonnull Map<String, TemplateCopy> overrides) {
    TemplateCopy overridden = overrides.get(urn);
    if (overridden != null) {
      return overridden;
    }
    return fetch(operationContext, retriever, urn);
  }

  @Nonnull
  private TemplateCopy fetch(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn) {
    if (source != SearchTemplateSource.CLASSPATH) {
      Urn parsed = UrnUtils.getUrn(urn);
      SystemAspect aspect =
          retriever.getLatestSystemAspect(
              operationContext, parsed, SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
      SearchTemplateProperties properties =
          aspect == null ? null : aspect.getAspect(SearchTemplateProperties.class);
      if (properties != null) {
        return new TemplateCopy(properties, SearchTemplateModified.at(aspect));
      }
    }
    SearchTemplateProperties shipped = shipped(urn);
    if (shipped != null) {
      SearchTemplateBootstrap.note(urn);
      return new TemplateCopy(shipped, 0L);
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
    return StructuredPropertyClauses.empty(section);
  }

  private static boolean hasRankingField(@Nonnull SearchTemplateRanking ranking) {
    return ranking.hasPreset()
        || ranking.hasWindow()
        || ranking.hasWindowSizer()
        || ranking.hasParameters()
        || ranking.hasTemplate()
        || ranking.hasSignals()
        || ranking.hasAdd()
        || ranking.hasRemove()
        || ranking.hasFormula();
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
    boolean template = !ranking.hasTemplate() || blank(ranking.getTemplate());
    boolean signals =
        !ranking.hasSignals() || ranking.getSignals() == null || ranking.getSignals().isEmpty();
    boolean add = !ranking.hasAdd() || ranking.getAdd() == null || ranking.getAdd().isEmpty();
    boolean remove =
        !ranking.hasRemove() || ranking.getRemove() == null || ranking.getRemove().isEmpty();
    boolean formula = !ranking.hasFormula() || blank(ranking.getFormula());
    return preset
        && window
        && sizer
        && parameters
        && template
        && signals
        && add
        && remove
        && formula;
  }

  @Nullable
  private static SearchTemplateRanking mergeRanking(
      @Nullable SearchTemplateRanking base,
      @Nullable SearchTemplateRanking overlay,
      @Nonnull Set<String> addSatisfiedRemoves) {
    // A record with no fields drops the section. A set field, including an empty
    // list or string, drops that field and keeps the rest.
    if (overlay == null || !hasRankingField(overlay)) {
      addSatisfiedRemoves.clear();
      return null;
    }
    if (base == null || emptyRanking(base)) {
      base = new SearchTemplateRanking();
      addSatisfiedRemoves.clear();
    }
    // A ranking is a reference or it is inline. Setting one drops the inherited other.
    // A non-empty signal list leaves preset unset.
    boolean overlayReference = referencesTemplate(overlay);
    boolean overlayInline = inlineRanking(overlay);
    boolean overlaySignals =
        overlay.hasSignals() && overlay.getSignals() != null && !overlay.getSignals().isEmpty();
    SearchTemplateRanking merged = new SearchTemplateRanking();
    putString(
        merged::setPreset,
        overlay.hasPreset(),
        overlay.getPreset(),
        !overlayReference && !overlaySignals && base.hasPreset(),
        base.getPreset());
    if (overlay.hasWindow()) {
      if (overlay.getWindow() != null) {
        merged.setWindow(overlay.getWindow());
      }
    } else if (!overlayReference && base.hasWindow() && base.getWindow() != null) {
      merged.setWindow(base.getWindow());
    }
    putString(
        merged::setWindowSizer,
        overlay.hasWindowSizer(),
        overlay.getWindowSizer(),
        !overlayReference && base.hasWindowSizer(),
        base.getWindowSizer());
    if (overlay.hasParameters()) {
      if (overlay.getParameters() != null && !overlay.getParameters().isEmpty()) {
        merged.setParameters(overlay.getParameters());
      }
    } else if (!overlayReference
        && base.hasParameters()
        && base.getParameters() != null
        && !base.getParameters().isEmpty()) {
      merged.setParameters(base.getParameters());
    }
    putString(
        merged::setTemplate,
        overlay.hasTemplate(),
        overlay.getTemplate(),
        !overlayInline && base.hasTemplate(),
        base.getTemplate());
    // A non-empty list replaces signals and discards patches. An empty list drops signals only.
    if (overlaySignals) {
      merged.setSignals(new RankingSignalArray(overlay.getSignals()));
      addSatisfiedRemoves.clear();
    } else {
      if (!overlay.hasSignals()
          && !overlayReference
          && base.hasSignals()
          && base.getSignals() != null
          && !base.getSignals().isEmpty()) {
        merged.setSignals(new RankingSignalArray(base.getSignals()));
      }
      // Patches beside a reference apply to that target. Clearing the reference drops them.
      SearchTemplateRanking patchBase = base;
      if (clearsTemplate(overlay) && referencesTemplate(base)) {
        patchBase = new SearchTemplateRanking();
        addSatisfiedRemoves.clear();
      }
      mergeSignalPatches(merged, patchBase, overlay, addSatisfiedRemoves);
    }
    putString(
        merged::setFormula,
        overlay.hasFormula(),
        overlay.getFormula(),
        !overlayReference && base.hasFormula(),
        base.getFormula());
    return emptyRanking(merged) ? null : merged;
  }

  private static void mergeSignalPatches(
      @Nonnull SearchTemplateRanking merged,
      @Nonnull SearchTemplateRanking base,
      @Nonnull SearchTemplateRanking overlay,
      @Nonnull Set<String> addSatisfiedRemoves) {
    List<RankingSignal> adds = new ArrayList<>(rankingAdds(base));
    List<String> removes = new ArrayList<>(rankingRemoves(base));
    for (String name : rankingRemoves(overlay)) {
      if (name == null || name.isBlank()) {
        throw new SearchTemplateException("Search template ranking remove name is not valid");
      }
      boolean droppedAdd = false;
      Iterator<RankingSignal> inherited = adds.iterator();
      while (inherited.hasNext()) {
        if (name.equals(inherited.next().getName())) {
          inherited.remove();
          droppedAdd = true;
        }
      }
      if (droppedAdd) {
        addSatisfiedRemoves.add(name);
      }
      if (!removes.contains(name)) {
        removes.add(name);
      }
    }
    Set<String> seen = new HashSet<>();
    for (RankingSignal signal : rankingAdds(overlay)) {
      String name = signal.getName();
      if (name == null || !seen.add(name)) {
        throw new SearchTemplateException(
            "Search template ranking signal name is not valid: " + name);
      }
      removes.remove(name);
      addSatisfiedRemoves.remove(name);
      int index = indexOfSignal(adds, name);
      if (index >= 0) {
        adds.set(index, signal);
      } else {
        adds.add(signal);
      }
    }
    if (!adds.isEmpty()) {
      merged.setAdd(new RankingSignalArray(adds));
    }
    if (!removes.isEmpty()) {
      merged.setRemove(new StringArray(removes));
    }
  }

  @Nullable
  private static SearchTemplateRanking applyRankingPatches(
      @Nullable SearchTemplateRanking ranking,
      @Nonnull List<RankingSignal> adds,
      @Nonnull List<String> removes,
      @Nonnull Set<String> addSatisfiedRemoves) {
    if (adds.isEmpty() && removes.isEmpty()) {
      return ranking;
    }
    List<RankingSignal> signals = new ArrayList<>();
    if (ranking != null && ranking.hasSignals() && ranking.getSignals() != null) {
      signals.addAll(ranking.getSignals());
    }
    for (String name : removes) {
      boolean removed = false;
      Iterator<RankingSignal> present = signals.iterator();
      while (present.hasNext()) {
        if (name.equals(present.next().getName())) {
          present.remove();
          removed = true;
        }
      }
      if (!removed && !addSatisfiedRemoves.contains(name)) {
        throw new SearchTemplateException(
            "Search template ranking remove is not a signal: " + name);
      }
    }
    for (RankingSignal signal : adds) {
      int index = indexOfSignal(signals, signal.getName());
      if (index >= 0) {
        signals.set(index, signal);
      } else {
        signals.add(signal);
      }
    }
    SearchTemplateRanking applied =
        ranking == null ? new SearchTemplateRanking() : copyRankingScalars(ranking);
    if (!signals.isEmpty()) {
      applied.setSignals(new RankingSignalArray(signals));
    }
    return emptyRanking(applied) ? null : applied;
  }

  @Nonnull
  private static SearchTemplateRanking copyRankingScalars(@Nonnull SearchTemplateRanking ranking) {
    SearchTemplateRanking copy = new SearchTemplateRanking();
    if (ranking.hasPreset() && ranking.getPreset() != null && !ranking.getPreset().isBlank()) {
      copy.setPreset(ranking.getPreset());
    }
    if (ranking.hasWindow() && ranking.getWindow() != null) {
      copy.setWindow(ranking.getWindow());
    }
    if (ranking.hasWindowSizer()
        && ranking.getWindowSizer() != null
        && !ranking.getWindowSizer().isBlank()) {
      copy.setWindowSizer(ranking.getWindowSizer());
    }
    if (ranking.hasParameters()
        && ranking.getParameters() != null
        && !ranking.getParameters().isEmpty()) {
      copy.setParameters(ranking.getParameters());
    }
    if (ranking.hasFormula() && ranking.getFormula() != null && !ranking.getFormula().isBlank()) {
      copy.setFormula(ranking.getFormula());
    }
    return copy;
  }

  @Nonnull
  private static List<RankingSignal> rankingAdds(@Nullable SearchTemplateRanking ranking) {
    if (ranking == null || !ranking.hasAdd() || ranking.getAdd() == null) {
      return List.of();
    }
    return ranking.getAdd();
  }

  @Nonnull
  private static List<String> rankingRemoves(@Nullable SearchTemplateRanking ranking) {
    if (ranking == null || !ranking.hasRemove() || ranking.getRemove() == null) {
      return List.of();
    }
    return ranking.getRemove();
  }

  private static int indexOfSignal(@Nonnull List<RankingSignal> signals, @Nullable String name) {
    for (int i = 0; i < signals.size(); i++) {
      if (name != null && name.equals(signals.get(i).getName())) {
        return i;
      }
    }
    return -1;
  }

  @Nullable
  private static String rankingReferenceUrn(@Nullable SearchTemplateRanking ranking) {
    if (ranking == null
        || !ranking.hasTemplate()
        || ranking.getTemplate() == null
        || ranking.getTemplate().isBlank()) {
      return null;
    }
    return ranking.getTemplate();
  }

  private static boolean clearsTemplate(@Nonnull SearchTemplateRanking ranking) {
    return ranking.hasTemplate() && blank(ranking.getTemplate());
  }

  private static boolean referencesTemplate(@Nullable SearchTemplateRanking ranking) {
    return ranking != null
        && ranking.hasTemplate()
        && ranking.getTemplate() != null
        && !ranking.getTemplate().isBlank();
  }

  private static void putString(
      java.util.function.Consumer<String> setter,
      boolean overlaySet,
      @Nullable String overlayValue,
      boolean baseSet,
      @Nullable String baseValue) {
    if (overlaySet) {
      if (overlayValue != null && !overlayValue.isBlank()) {
        setter.accept(overlayValue);
      }
      return;
    }
    if (baseSet && baseValue != null && !baseValue.isBlank()) {
      setter.accept(baseValue);
    }
  }

  @Nonnull
  private ResolvedRanking resolveRankingReference(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull AspectRetriever retriever,
      @Nonnull String urn,
      @Nullable SearchTemplateRanking ranking,
      @Nonnull Map<String, TemplateCopy> overrides,
      boolean classpathBridge,
      @Nonnull List<String> urns) {
    if (ranking == null
        || !ranking.hasTemplate()
        || ranking.getTemplate() == null
        || ranking.getTemplate().isBlank()) {
      return new ResolvedRanking(ranking, 0L);
    }
    if (inlineRanking(ranking)) {
      throw new SearchTemplateException("Search template ranking is a reference or it is inline");
    }
    String target = ranking.getTemplate();
    if (urn.equals(target) || urns.contains(target)) {
      throw new SearchTemplateException("Search template ranking reference cycle at " + target);
    }
    TemplateCopy copy = resolve(operationContext, retriever, target, overrides);
    MergedSearchTemplate targetMerged =
        compose(operationContext, retriever, target, copy, overrides, classpathBridge, false);
    SearchTemplateRanking resolved = targetMerged.getRanking();
    if (resolved == null
        || (resolved.hasTemplate()
            && resolved.getTemplate() != null
            && !resolved.getTemplate().isBlank())) {
      throw new SearchTemplateException(
          "Search template ranking reference must be inline: " + target);
    }
    for (String link : targetMerged.getChain()) {
      if (urn.equals(link)) {
        throw new SearchTemplateException("Search template ranking reference cycle at " + link);
      }
      if (!urns.contains(link)) {
        urns.add(link);
      }
    }
    return new ResolvedRanking(resolved, targetMerged.getLastModified());
  }

  private static boolean inlineRanking(@Nonnull SearchTemplateRanking ranking) {
    boolean preset =
        ranking.hasPreset() && ranking.getPreset() != null && !ranking.getPreset().isBlank();
    boolean window = ranking.hasWindow() && ranking.getWindow() != null;
    boolean sizer =
        ranking.hasWindowSizer()
            && ranking.getWindowSizer() != null
            && !ranking.getWindowSizer().isBlank();
    boolean parameters =
        ranking.hasParameters()
            && ranking.getParameters() != null
            && !ranking.getParameters().isEmpty();
    boolean signals =
        ranking.hasSignals() && ranking.getSignals() != null && !ranking.getSignals().isEmpty();
    boolean formula =
        ranking.hasFormula() && ranking.getFormula() != null && !ranking.getFormula().isBlank();
    return preset || window || sizer || parameters || signals || formula;
  }

  private static boolean hasCapability(@Nullable List<String> capabilities, @Nonnull String name) {
    return capabilities != null && capabilities.contains(name);
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
      return fetch(opContext, opContext.getAspectRetriever(), urn).properties();
    } catch (SearchTemplateException e) {
      return null;
    }
  }

  @Nullable
  private MergedSearchTemplate fresh(@Nullable Snapshot snapshot, @Nonnull String urn) {
    if (snapshot == null || !snapshot.templates.containsKey(urn)) {
      return null;
    }
    if (cacheTtlMillis > 0 && clock.getAsLong() - snapshot.readStartedAtMillis >= cacheTtlMillis) {
      return null;
    }
    return snapshot.templates.get(urn);
  }

  private long age(@Nonnull Snapshot snapshot) {
    return Math.max(0, clock.getAsLong() - snapshot.readStartedAtMillis);
  }

  @Nonnull
  private Snapshot refresh(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nullable Snapshot previous,
      long readStart) {
    LinkedHashSet<String> serve = new LinkedHashSet<>();
    if (previous != null) {
      serve.addAll(previous.templates.keySet());
    }
    serve.add(urn);
    while (serve.size() > cacheMaximumSize) {
      String drop = null;
      for (String key : serve) {
        if (!urn.equals(key)) {
          drop = key;
          break;
        }
      }
      if (drop == null) {
        break;
      }
      serve.remove(drop);
    }
    // Ancestors are read for their stamps. They do not take a cache slot and are not merged
    // unless a caller already loaded that template.
    LinkedHashSet<String> read = new LinkedHashSet<>(serve);
    if (previous != null) {
      for (String key : serve) {
        MergedSearchTemplate cached = previous.templates.get(key);
        if (cached != null) {
          read.addAll(cached.getChain());
        }
      }
    }

    boolean fromSearch = documents != null && source != SearchTemplateSource.CLASSPATH;
    Map<String, SearchTemplateDocuments.Document> indexed =
        fromSearch ? documents.read(opContext, read) : Map.of();
    Map<String, Held> raw = new LinkedHashMap<>();
    for (String key : read) {
      Held held = hold(opContext, key, fromSearch, indexed);
      if (held != null) {
        raw.put(key, held);
      }
    }

    Map<String, MergedSearchTemplate> merged = new LinkedHashMap<>();
    Set<String> rebuild = new LinkedHashSet<>();
    for (String key : serve) {
      Held held = raw.get(key);
      if (held == null) {
        continue;
      }
      MergedSearchTemplate existing = previous == null ? null : previous.templates.get(key);
      if (fromSearch
          && existing != null
          && previous != null
          && stampsMatch(existing, raw, previous)) {
        merged.put(key, existing);
      } else {
        rebuild.add(key);
      }
    }
    if (fromSearch && previous != null) {
      Set<String> changed = Set.copyOf(rebuild);
      for (String key : List.copyOf(merged.keySet())) {
        if (previous.templates.get(key).getChain().stream().anyMatch(changed::contains)) {
          merged.remove(key);
          rebuild.add(key);
        }
      }
    }

    Map<String, TemplateCopy> overrides = new LinkedHashMap<>();
    raw.forEach((key, held) -> overrides.put(key, new TemplateCopy(held.properties, held.stamp)));
    AspectRetriever retriever = opContext.getAspectRetriever();
    Map<String, SearchTemplateException> rejected = new LinkedHashMap<>();
    for (String key : rebuild) {
      Held held = raw.get(key);
      try {
        merged.put(
            key,
            compose(
                opContext,
                retriever,
                key,
                new TemplateCopy(held.properties, held.stamp),
                overrides,
                held.classpathBridge,
                true));
      } catch (SearchTemplateException e) {
        log.warn("Search template {} is invalid and was left out of the cache", key, e);
        rejected.put(key, e);
      }
    }

    if (fromSearch
        && rebuild.isEmpty()
        && previous != null
        && previous.templates.keySet().equals(merged.keySet())) {
      return new Snapshot(previous.templates, previous.raw, previous.stamps, readStart, Map.of());
    }
    LinkedHashSet<String> missing = new LinkedHashSet<>();
    for (MergedSearchTemplate template : merged.values()) {
      for (String link : template.getChain()) {
        if (!raw.containsKey(link)) {
          missing.add(link);
        }
      }
    }
    if (!missing.isEmpty()) {
      Map<String, SearchTemplateDocuments.Document> extra =
          fromSearch ? documents.read(opContext, missing) : Map.of();
      for (String link : missing) {
        Held held = hold(opContext, link, fromSearch, extra);
        if (held != null) {
          raw.put(link, held);
        }
      }
    }
    Map<String, SearchTemplateProperties> bodies = new LinkedHashMap<>();
    Map<String, Long> stamps = new LinkedHashMap<>();
    Set<String> kept = new LinkedHashSet<>(merged.keySet());
    for (MergedSearchTemplate template : merged.values()) {
      kept.addAll(template.getChain());
    }
    raw.forEach(
        (key, held) -> {
          if (kept.contains(key)) {
            bodies.put(key, held.properties);
          }
        });
    for (MergedSearchTemplate template : merged.values()) {
      for (String link : template.getChain()) {
        Held held = raw.get(link);
        if (held != null) {
          stamps.put(link, held.stamp);
        }
      }
    }
    return new Snapshot(
        Map.copyOf(merged),
        Map.copyOf(bodies),
        Map.copyOf(stamps),
        readStart,
        Map.copyOf(rejected));
  }

  /**
   * A cached merge is current only when every link has a non-zero stamp, that stamp still matches,
   * and the fetched body equals the cached document. Stamp 0 means the aspect row had no system
   * write time.
   */
  private static boolean stampsMatch(
      @Nonnull MergedSearchTemplate existing,
      @Nonnull Map<String, Held> raw,
      @Nonnull Snapshot previous) {
    for (String link : existing.getChain()) {
      Held held = raw.get(link);
      SearchTemplateProperties previousBody = previous.raw.get(link);
      if (held == null
          || held.stamp == 0L
          || previous.stamps.getOrDefault(link, Long.MIN_VALUE) != held.stamp
          || previousBody == null
          || !previousBody.data().equals(held.properties.data())) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private Held hold(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      boolean fromSearch,
      @Nonnull Map<String, SearchTemplateDocuments.Document> indexed) {
    if (fromSearch && indexed.containsKey(urn)) {
      SearchTemplateDocuments.Document storedDocument = indexed.get(urn);
      if (storedDocument != null && storedDocument.properties() != null) {
        return new Held(storedDocument.properties(), storedDocument.modifiedAt(), false);
      }
    }
    try {
      boolean bridge = source == SearchTemplateSource.CLASSPATH || !stored(opContext, urn);
      TemplateCopy loaded = fetch(opContext, opContext.getAspectRetriever(), urn);
      return new Held(loaded.properties(), loaded.modifiedAt(), bridge);
    } catch (SearchTemplateException e) {
      return null;
    }
  }

  private record ResolvedRanking(@Nullable SearchTemplateRanking ranking, long lastModified) {}

  private record TemplateCopy(@Nonnull SearchTemplateProperties properties, long modifiedAt) {}

  private static final class Held {
    private final SearchTemplateProperties properties;
    private final long stamp;
    private final boolean classpathBridge;

    private Held(
        @Nonnull SearchTemplateProperties properties, long stamp, boolean classpathBridge) {
      this.properties = properties;
      this.stamp = stamp;
      this.classpathBridge = classpathBridge;
    }
  }

  private static final class Snapshot {
    private final Map<String, MergedSearchTemplate> templates;
    private final Map<String, SearchTemplateProperties> raw;
    private final Map<String, Long> stamps;
    private final long readStartedAtMillis;
    private final Map<String, SearchTemplateException> rejected;

    private Snapshot(
        @Nonnull Map<String, MergedSearchTemplate> templates,
        @Nonnull Map<String, SearchTemplateProperties> raw,
        @Nonnull Map<String, Long> stamps,
        long readStartedAtMillis,
        @Nonnull Map<String, SearchTemplateException> rejected) {
      this.templates = templates;
      this.raw = raw;
      this.stamps = stamps;
      this.readStartedAtMillis = readStartedAtMillis;
      this.rejected = rejected;
    }
  }
}
