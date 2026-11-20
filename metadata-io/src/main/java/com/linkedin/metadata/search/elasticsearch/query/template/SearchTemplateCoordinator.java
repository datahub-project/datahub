package com.linkedin.metadata.search.elasticsearch.query.template;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateRolloutConfiguration;
import com.linkedin.metadata.config.search.VectorStoreConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProfile;
import com.linkedin.metadata.search.embedding.EmbeddingProfileRegistry;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.search.template.SearchTemplateLadder;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import io.datahubproject.metadata.context.SearchTemplateRuntime;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

/** Selects and renders templates when keyword read and {@code templates.enabled} are both on. */
@Slf4j
public class SearchTemplateCoordinator implements SearchTemplateRuntime {
  // Runs a similarity anchor's check, reads, and kNN calls, so a stalled store call cannot hold a
  // search past the vector retrieval budget. Not the ladder executor: that one runs a task on the
  // caller when
  // it is full, where nothing bounds the wait. This queue is bounded, and when it is full the
  // search fails as it would on a failed check.
  private static final ExecutorService ANCHOR_EXECUTOR = newAnchorExecutor();

  private final ElasticSearchConfiguration elasticSearchConfiguration;
  private final SearchTemplateLoader loader;
  private final SearchTemplateSelector selector;
  private final SearchV3QueryBuilder queryBuilder;
  private final SearchTemplateRenderer renderer;
  private final ExecutorService ladderExecutor;
  private final SearchTemplateComponentRegistry components;
  private final SearchTemplateGates gates;
  private final AtomicInteger shadowRenders = new AtomicInteger();

  // The same budget a vector rung's embedding and kNN calls get
  private volatile long vectorTimeoutMillis = 2_000;
  @Nullable private volatile String lastShadowDsl;
  private volatile ExecutorService anchorExecutor = ANCHOR_EXECUTOR;

  public SearchTemplateCoordinator(
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull SearchTemplateLoader loader,
      @Nonnull ExecutorService ladderExecutor,
      @Nonnull SearchTemplateComponentRegistry components,
      @Nonnull SearchTemplateGates gates) {
    this.elasticSearchConfiguration = elasticSearchConfiguration;
    this.loader = loader;
    this.ladderExecutor = ladderExecutor;
    this.components = components;
    this.gates = gates;
    SearchTemplateConfiguration templates = templates(elasticSearchConfiguration);
    this.selector = new SearchTemplateSelector(templates, loader, gates);
    this.queryBuilder =
        new SearchV3QueryBuilder(
            loader, elasticSearchConfiguration.getSearch(), customSearchConfiguration);
    this.renderer = new SearchTemplateRenderer(queryBuilder);
  }

  @Nonnull
  public SearchV3QueryBuilder queryBuilder() {
    return queryBuilder;
  }

  /** Loads one template by URN. Autocomplete uses this without the search-surface rollout. */
  @Nonnull
  public MergedSearchTemplate load(@Nonnull OperationContext opContext, @Nonnull String urn) {
    return loader.load(opContext, urn);
  }

  public double hybridKeywordScale() {
    return templates(elasticSearchConfiguration).getHybridKeywordScale();
  }

  public int documentRerankTimeoutSeconds() {
    return templates(elasticSearchConfiguration).getDocumentRerankTimeoutSeconds();
  }

  public boolean active() {
    SearchTemplateConfiguration templates = templates(elasticSearchConfiguration);
    return templates.isEnabled()
        && EntitySearchIndexResolver.shouldReadV3(elasticSearchConfiguration.getEntityIndex());
  }

  /**
   * Request decision. {@link #active()} is the feature switch. Off and shadow do not execute a
   * template. Canary percent keeps the same actor on one side.
   */
  @Nonnull
  public SearchTemplateRollout.Effect effect(@Nonnull OperationContext opContext) {
    if (!active()) {
      return SearchTemplateRollout.Effect.OFF;
    }
    String surface = opContext.getSearchContext().getSearchSurface();
    return SearchTemplateRollout.effect(
        true, rollout(), surface, SearchTemplateRollout.bucketKey(actor(opContext), surface));
  }

  /**
   * This request serves the template query. Shadow, off, and filter lookups stay on the Java
   * builder.
   */
  public boolean executes(@Nonnull OperationContext opContext) {
    if (opContext.getSearchContext().isSkipTemplateExecution()) {
      return false;
    }
    return effect(opContext) == SearchTemplateRollout.Effect.ACTIVE;
  }

  /**
   * Resolves and renders the template the request would serve. The rendered body is discarded.
   * Nothing is sent to the search engine. Failures are counted and do not fail the Java query.
   *
   * @param walks the request walks a ladder, as a page search does. A filter, aggregation, or count
   *     does not, so, as when it executes, a scope that leaves out a requested type fails it. A
   *     scroll fails such a scope at bind, so its shadow fails it whether or not it walks.
   */
  public void shadow(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      boolean scroll,
      boolean walks) {
    shadowRenders.incrementAndGet();
    try {
      OperationContext rendered = opContext;
      SearchFlags existing = opContext.getSearchContext().getSearchFlags();
      boolean current = existing != null && Boolean.TRUE.equals(existing.isFulltext());
      if (existing != null && current != fulltext) {
        rendered = opContext.withSearchFlags(flags -> flags.setFulltext(fulltext));
      }
      List<String> names = new ArrayList<>();
      for (EntitySpec entitySpec : entitySpecs) {
        names.add(entitySpec.getName());
      }
      SearchTemplateDryRender preview = preview(rendered, query, names, scroll, null);
      if (!walks || scroll) {
        SearchTemplateCapabilities.requireWholeScope(
            loader.load(rendered, preview.resolvedUrn()), names);
      }
      lastShadowDsl = preview.queryDsl();
      SearchTemplateMetrics.shadow(
          opContext, preview.resolvedUrn(), opContext.getSearchContext().getSearchSurface());
    } catch (RuntimeException e) {
      lastShadowDsl = null;
      log.warn("Search template shadow render failed", e);
      SearchTemplateMetrics.renderFailure(opContext, "shadow");
    }
  }

  @VisibleForTesting
  public int shadowRenderCount() {
    return shadowRenders.get();
  }

  @VisibleForTesting
  @Nullable
  public String lastShadowDsl() {
    return lastShadowDsl;
  }

  @Nonnull
  public SearchTemplateLoader loader() {
    return loader;
  }

  @Nonnull
  public SearchTemplateComponentRegistry components() {
    return components;
  }

  @Nonnull
  public SearchTemplateGates gates() {
    return gates;
  }

  @Nonnull
  public ExecutorService ladderExecutor() {
    return ladderExecutor;
  }

  /**
   * Arms document rerank from the rung that was served. A fused page has no single rung, so rerank
   * stays off.
   */
  public void armServed(@Nonnull OperationContext opContext, @Nullable String servedUrn) {
    ResolvedSearchTemplate resolved = opContext.getSearchContext().getResolvedSearchTemplate();
    if (resolved == null) {
      return;
    }
    resolved.setTemplateRerank(null);
    if (servedUrn == null) {
      resolved.setKnn(false, 1f, 1f, 100);
      return;
    }
    MergedSearchTemplate template = loader.load(opContext, servedUrn);
    // A keywordRerank rung owns the rerank of its window, so the default hybrid reranker stays off.
    // The search reranks, or reports why it kept keyword order, when it runs.
    if (SearchTemplateCapabilities.keywordRerank(template)) {
      resolved.setTemplateRerank(servedUrn);
      resolved.setKnn(false, 1f, 1f, 100);
      return;
    }
    if (!semanticReadEnabled()
        || template.getKnn() == null
        || SearchTemplateCapabilities.combine(template.getRetrieval()) != null) {
      resolved.setKnn(false, 1f, 1f, 100);
      return;
    }
    SearchTemplateJson.rejectForbidden(template.getKnn(), "knn");
    SearchTemplateKnn knn = SearchTemplateJson.parseKnn(template.getKnn());
    resolved.setKnn(true, knn.keywordWeight(), knn.vectorWeight(), knn.k());
  }

  @Nonnull
  public OperationContext bind(@Nonnull OperationContext opContext, @Nonnull String query) {
    return bind(opContext, query, false);
  }

  @Nonnull
  public OperationContext bind(
      @Nonnull OperationContext opContext, @Nonnull String query, boolean scroll) {
    return bind(opContext, query, scroll, null);
  }

  @Nonnull
  public OperationContext bind(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      boolean scroll,
      @Nullable List<String> entityNames) {
    if (!executes(opContext)) {
      if (opContext.getSearchContext().getResolvedSearchTemplate() == null) {
        return opContext;
      }
      return opContext.withResolvedSearchTemplate(null);
    }
    boolean fulltext =
        opContext.getSearchContext().getSearchFlags() != null
            && Boolean.TRUE.equals(opContext.getSearchContext().getSearchFlags().isFulltext());
    ResolvedSearchTemplate resolved =
        selector.select(
            opContext,
            query,
            fulltext,
            engineType(opContext),
            entityNames == null ? List.of() : entityNames);
    boolean documentRerank =
        !scroll && semanticReadEnabled() && SearchTemplateHybrid.includesDocument(entityNames);
    resolved.setExecutionCapabilities(
        scroll,
        documentRerank,
        entityNames,
        EmbeddingProfileRegistry.system(engineType(opContext)));
    try {
      noteIdentity(opContext, resolved, query, scroll);
      SearchTemplateCapabilities.requireResolvable(
          loader.load(opContext, resolved.getUrn()),
          gates,
          resolved.getEntityTypes(),
          resolved.getSearchSystem(),
          documentRerank,
          scroll);
    } catch (SearchTemplateFailClosedException e) {
      // A similarity anchor this search cannot use, counted as a vector retrieval outcome
      throw e;
    } catch (RuntimeException e) {
      SearchTemplateMetrics.renderFailure(opContext, "select");
      throw e;
    }
    return opContext
        .withSearchFlags(flags -> flags.setFulltext(fulltext))
        .withResolvedSearchTemplate(resolved);
  }

  /**
   * Resolves and renders a template for {@code query} without executing search. {@code engineType}
   * null uses the engine on the operation context, the same choice a live search would make.
   */
  @Nonnull
  public SearchTemplateDryRender preview(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      @Nonnull List<String> entityNames,
      boolean scroll,
      @Nullable SearchClientShim.SearchEngineType engineType) {
    if (entityNames.isEmpty()) {
      throw new SearchTemplateException("Search template preview requires an entity type");
    }
    boolean fulltext =
        opContext.getSearchContext().getSearchFlags() != null
            && Boolean.TRUE.equals(opContext.getSearchContext().getSearchFlags().isFulltext());
    SearchClientShim.SearchEngineType engine =
        engineType == null ? engineType(opContext) : engineType;
    ResolvedSearchTemplate resolved =
        selector.select(opContext, query, fulltext, engine, entityNames);
    OperationContext bound = opContext.withResolvedSearchTemplate(resolved);
    List<EntitySpec> entitySpecs = new ArrayList<>();
    for (String entityName : entityNames) {
      entitySpecs.add(opContext.getEntityRegistry().getEntitySpec(entityName));
    }
    RenderedSearchTemplate rendered =
        queryBuilder.render(bound, entitySpecs, query, fulltext, resolved.getUrn(), scroll);
    MergedSearchTemplate template = loader.load(bound, resolved.getUrn());
    boolean documentRerank =
        !scroll && semanticReadEnabled() && SearchTemplateHybrid.includesDocument(entityNames);
    // As at bind, a root the request assigns no type is skipped, so its shape is not checked
    String rejection =
        SearchTemplateGates.assigns(template, entityNames)
            ? SearchTemplateCapabilities.rejectionCode(
                template, documentRerank, scroll, components, gates)
            : null;
    List<String> rungs = ladderRungs(bound, resolved.getUrn(), !scroll);
    if (rejection == null && vectorWithLegacyKnn(bound, resolved.getUrn(), entityNames, !scroll)) {
      rejection = SearchTemplateCapabilities.VECTOR_WITH_LEGACY_KNN;
    }
    if (rejection == null) {
      rejection =
          SearchTemplateCapabilities.resolvableRejection(
              template,
              gates,
              entityNames,
              EmbeddingProfileRegistry.system(engine),
              documentRerank,
              scroll);
    }
    List<SearchTemplateDryRender.CapabilityDecision> capabilities = new ArrayList<>();
    for (String name : template.getCapabilities()) {
      boolean accepted = SearchTemplateCapabilities.SUPPORTED.contains(name);
      capabilities.add(
          new SearchTemplateDryRender.CapabilityDecision(
              name, accepted, accepted ? "declared" : "unsupported"));
    }
    capabilities.add(
        new SearchTemplateDryRender.CapabilityDecision(
            "sections", rejection == null, rejection == null ? "runnable" : rejection));
    String dsl = rendered.query().toString();
    return new SearchTemplateDryRender(
        resolved.getUrn(),
        template.getChain(),
        resolved.getRouteChain(),
        resolved.getEngineSlot() == null ? "default" : resolved.getEngineSlot(),
        dsl,
        rendered.queriedFields(),
        SearchTemplateJson.countLeaves(dsl),
        rendered.estimatedClauses(),
        capabilities,
        rungs);
  }

  private boolean vectorWithLegacyKnn(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nullable List<String> entityTypes,
      boolean followLadder) {
    boolean[] vector = new boolean[1];
    boolean[] legacyKnn = new boolean[1];
    reach(opContext, urn, entityTypes, followLadder, new HashSet<>(), 0, vector, legacyKnn);
    return vector[0] && legacyKnn[0];
  }

  /**
   * Marks whether a rung this request can run retrieves by vector, and whether one declares a
   * legacy knn rerank. The walk skips a rung its scope assigns none of the requested types, and the
   * members of a group it skips, so those do not count. Bind and the dry render both use this, so
   * the vector and legacy knn check, and the search cache's once-per-page run, follow one rule.
   */
  private void reach(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nullable List<String> entityTypes,
      boolean followLadder,
      @Nonnull Set<String> seen,
      int depth,
      @Nonnull boolean[] vector,
      @Nonnull boolean[] legacyKnn) {
    if (!seen.add(urn) || depth > loader.maxParentDepth()) {
      return;
    }
    MergedSearchTemplate template = loader.load(opContext, urn);
    SearchTemplateLadder ladder = followLadder ? template.getLadder() : null;
    if (SearchTemplateGates.assigns(template, entityTypes)) {
      vector[0] |= SearchTemplateCapabilities.vectorRetrieval(template);
      legacyKnn[0] |= SearchTemplateCapabilities.legacyKnn(template);
      if (ladder != null && ladder.hasParallel() && ladder.getParallel() != null) {
        for (Urn member : ladder.getParallel()) {
          reach(
              opContext, member.toString(), entityTypes, true, seen, depth + 1, vector, legacyKnn);
        }
      }
    }
    if (ladder != null && ladder.hasNext() && ladder.getNext() != null) {
      reach(
          opContext,
          ladder.getNext().toString(),
          entityTypes,
          true,
          seen,
          depth + 1,
          vector,
          legacyKnn);
    }
  }

  @Nonnull
  private List<String> ladderRungs(
      @Nonnull OperationContext opContext, @Nonnull String urn, boolean followLadder) {
    List<String> rungs = new ArrayList<>();
    collectRungs(opContext, urn, rungs, new HashSet<>(), 0, followLadder);
    return rungs;
  }

  private void collectRungs(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull List<String> rungs,
      @Nonnull Set<String> seen,
      int depth,
      boolean followLadder) {
    if (!seen.add(urn) || depth > loader.maxParentDepth()) {
      return;
    }
    MergedSearchTemplate template = loader.load(opContext, urn);
    rungs.add(urn);
    if (!followLadder || template.getLadder() == null) {
      return;
    }
    SearchTemplateLadder ladder = template.getLadder();
    if (ladder.hasParallel() && ladder.getParallel() != null) {
      for (Urn member : ladder.getParallel()) {
        collectRungs(opContext, member.toString(), rungs, seen, depth + 1, true);
      }
    }
    if (ladder.hasNext() && ladder.getNext() != null) {
      collectRungs(opContext, ladder.getNext().toString(), rungs, seen, depth + 1, true);
    }
  }

  /**
   * Cache identity includes every reachable rung, the request inputs route conditions read when a
   * conditional route fired, the requested types' semantic eligibility when a reachable rung's
   * select reads it, and whether document rerank runs when a legacy knn rung is reachable.
   * Capability checks happen when a rung executes, so an unsupported next does not fail a request
   * that stops before reaching it. The exception is a graph that reaches a vector rung and a legacy
   * knn rerank: the search cache runs the first once per page, and the second reranks the first
   * cache batch, so bind fails.
   */
  private void noteIdentity(
      @Nonnull OperationContext opContext,
      @Nonnull ResolvedSearchTemplate resolved,
      @Nonnull String query,
      boolean scroll) {
    List<String> rungs = new ArrayList<>();
    StringBuilder bodies = new StringBuilder();
    boolean[] nameAndValue = new boolean[1];
    boolean[] classpathBridge = new boolean[1];
    boolean[] semanticSelect = new boolean[1];
    collect(
        opContext,
        resolved.getUrn(),
        rungs,
        bodies,
        new HashSet<>(),
        0,
        !scroll,
        resolved,
        query,
        scroll,
        nameAndValue,
        classpathBridge,
        semanticSelect);
    boolean[] vector = new boolean[1];
    boolean[] legacyKnn = new boolean[1];
    reach(
        opContext,
        resolved.getUrn(),
        resolved.getEntityTypes(),
        !scroll,
        new HashSet<>(),
        0,
        vector,
        legacyKnn);
    resolved.setVectorReachable(vector[0]);
    if (vector[0] && legacyKnn[0]) {
      throw new SearchTemplateException(
          "Search template section is not supported: vector retrieval with a legacy knn rerank");
    }
    String identity = String.join("+", rungs);
    if (!classpathBridge[0] && SearchTemplateViewer.references(bodies.toString())) {
      identity += SearchTemplateViewer.identity(opContext, bodies.toString());
    }
    if (!classpathBridge[0] && nameAndValue[0]) {
      identity += "|sp=" + queryBuilder.structuredPropertyCatalogStamp(opContext);
    }
    if (resolved.getRouteMatch() != null) {
      identity += "|route=" + resolved.getRouteMatch();
    }
    if (semanticSelect[0]) {
      identity += "|eligible=" + gates.eligibilityIdentity(resolved.getEntityTypes());
    }
    // A legacy knn rung runs only with document rerank, and a parallel member that cannot run hands
    // the group to its next, so the flag can change which rung serves.
    if (legacyKnn[0]) {
      identity += "|dr=" + resolved.isDocumentRerank();
    }
    resolved.setCacheIdentity(identity);
  }

  private void collect(
      @Nonnull OperationContext opContext,
      @Nonnull String urn,
      @Nonnull List<String> rungs,
      @Nonnull StringBuilder bodies,
      @Nonnull Set<String> seen,
      int depth,
      boolean followLadder,
      @Nonnull ResolvedSearchTemplate resolved,
      @Nonnull String query,
      boolean scroll,
      @Nonnull boolean[] nameAndValue,
      @Nonnull boolean[] classpathBridge,
      @Nonnull boolean[] semanticSelect) {
    if (!seen.add(urn) || depth > loader.maxParentDepth()) {
      return;
    }
    MergedSearchTemplate template = loader.load(opContext, urn);
    // The walk skips a root its scope assigns none of the requested types, so its shape is not
    // checked. On scroll, requireResolvable fails such a root instead.
    if (depth == 0 && SearchTemplateGates.assigns(template, resolved.getEntityTypes())) {
      SearchTemplateCapabilities.requireSupported(
          template, resolved.isDocumentRerank(), scroll, components, gates);
    }
    if (depth == 0) {
      classpathBridge[0] = template.isClasspathBridge();
    }
    if (!template.isClasspathBridge() && template.getStructuredPropertyQuery() != null) {
      nameAndValue[0] = true;
    }
    if (SearchTemplateLadderPredicates.selectsOnSemantic(template.getLadder())) {
      semanticSelect[0] = true;
    }
    rungs.add(
        template.getUrn()
            + "@"
            + template.getLastModified()
            + SearchTemplateComponents.cacheSuffix(template.getComponent())
            + rerankIdentity(template, resolved)
            + vectorIdentity(opContext, template, resolved, query, depth == 0));
    append(bodies, template.getQuery());
    template.getQueryFragments().values().forEach(value -> append(bodies, value));
    append(bodies, template.getClauses());
    append(bodies, template.getFunctionScore());
    if (template.getBoosts() != null) {
      template.getBoosts().forEach(boost -> append(bodies, boost.getValues()));
    }
    if (template.getFieldValueScores() != null) {
      template.getFieldValueScores().forEach(score -> append(bodies, score.data().toString()));
    }
    append(bodies, template.getScoreMode() == null ? null : template.getScoreMode().name());
    append(bodies, template.getBoostMode() == null ? null : template.getBoostMode().name());
    append(bodies, template.getSearchType() == null ? null : template.getSearchType().name());
    if (template.getFieldFactors() != null) {
      template.getFieldFactors().forEach(factor -> append(bodies, factor.data().toString()));
    }
    if (template.getTermWeights() != null) {
      template.getTermWeights().forEach(weight -> append(bodies, weight.data().toString()));
    }
    if (template.getEntityTypeBoosts() != null) {
      template.getEntityTypeBoosts().forEach(boost -> append(bodies, boost.data().toString()));
    }
    if (!followLadder) {
      return;
    }
    SearchTemplateLadder ladder = template.getLadder();
    if (ladder == null) {
      return;
    }
    if (ladder.hasParallel() && ladder.getParallel() != null) {
      for (Urn member : ladder.getParallel()) {
        collect(
            opContext,
            member.toString(),
            rungs,
            bodies,
            seen,
            depth + 1,
            true,
            resolved,
            query,
            false,
            nameAndValue,
            classpathBridge,
            semanticSelect);
      }
    }
    if (ladder.hasNext() && ladder.getNext() != null) {
      collect(
          opContext,
          ladder.getNext().toString(),
          rungs,
          bodies,
          seen,
          depth + 1,
          true,
          resolved,
          query,
          false,
          nameAndValue,
          classpathBridge,
          semanticSelect);
    }
  }

  // A keywordRerank rung's order also depends on this build's rerank, the profile it resolves, and
  // whether this request can rerank. The rung's lastModified covers its blend, and the search key
  // covers SearchFlags.minScore.
  @Nonnull
  private String rerankIdentity(
      @Nonnull MergedSearchTemplate template, @Nonnull ResolvedSearchTemplate resolved) {
    if (!SearchTemplateCapabilities.keywordRerank(template)) {
      return "";
    }
    Set<String> profiles =
        resolved.getEntityTypes() == null
            ? Set.of()
            : gates.joinedProfileIds(
                template, resolved.getEntityTypes(), resolved.getSearchSystem());
    return "~kr"
        + SearchTemplateKeywordRerank.EXECUTION_VERSION
        + ":"
        + String.join(",", profiles)
        + ":"
        + resolved.isDocumentRerank()
        + ":"
        + gates.hybridRead();
  }

  // A vector rung's hits also depend on this build's execution, the profile it resolves on the
  // system serving it, a floor inherited from configuration, and, on an external store, the window
  // of that store's result TTL. The rung's lastModified covers its own floor and cap, and the
  // search key covers the query, filter, flags, page, and caller. A similarity rung also depends
  // on the anchor's stored vectors.
  @Nonnull
  private String vectorIdentity(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull ResolvedSearchTemplate resolved,
      @Nonnull String query,
      boolean selected) {
    if (!SearchTemplateCapabilities.vectorRetrieval(template)) {
      return "";
    }
    final Map<String, VectorStoreConfiguration> stores =
        SearchTemplateVectorRetrieval.stores(
            elasticSearchConfiguration.getEntityIndex() == null
                ? null
                : elasticSearchConfiguration.getEntityIndex().getSemanticSearch());
    final String system =
        resolved.getEntityTypes() == null
            ? resolved.getSearchSystem()
            : SearchTemplateVectorRetrieval.system(
                gates, template, resolved.getEntityTypes(), resolved.getSearchSystem(), stores);
    Set<String> profiles =
        resolved.getEntityTypes() == null
            ? Set.of()
            : gates.joinedProfileIds(template, resolved.getEntityTypes(), system);
    return "~vr"
        + SearchTemplateVectorRetrieval.EXECUTION_VERSION
        + ":"
        + profiles.stream().map(this::profileIdentity).collect(Collectors.joining(","))
        + ":"
        + system
        + ":"
        + gates.minSimilarity(template)
        + SearchTemplateVectorRetrieval.resultWindow(system, stores)
        + (SearchTemplateCapabilities.QUERY_SOURCE_ANCHOR_ENTITY.equals(
                template.getRetrieval().getQuerySource())
            ? ":anchor=" + anchorStamp(opContext, template, resolved, system, query, selected)
            : "");
  }

  // The anchor URN with the time each of its stored embeddings was generated, so re-embedding the
  // anchor changes the key. Bind runs before the result cache, so an anchor this viewer may not use
  // is never read here and no cached page is served for it: the selected template fails the search
  // as noAnchor. A similarity rung only a ladder reaches does not fail a search that stops first,
  // so it is stamped unusable and fails if it runs. An anchor whose type has no profile on the
  // system serving the rung is not read either, and the rung fails it. The rung reuses the answer
  // recorded here.
  @Nonnull
  private String anchorStamp(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull ResolvedSearchTemplate resolved,
      @Nullable String system,
      @Nonnull String query,
      boolean selected) {
    final Urn anchor;
    try {
      anchor = Urn.createFromString(query.trim());
    } catch (URISyntaxException e) {
      return "none";
    }
    final Set<String> profiles = gates.profileIds(template, anchor.getEntityType(), system);
    if (profiles.size() != 1) {
      return anchor + "@none";
    }
    // The selected rung runs next, so its check and retrieval share one deadline
    final long deadlineNanos =
        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(vectorTimeoutMillis);
    if (selected) {
      resolved.setVectorDeadlineNanos(deadlineNanos);
    }
    final boolean usable;
    try {
      usable = anchorUsable(opContext, anchor, deadlineNanos);
    } catch (RuntimeException | IOException e) {
      return unknownAnchor(opContext, template, resolved, anchor, selected, e);
    }
    resolved.setAnchorCheck(anchor.toString(), usable);
    if (!usable) {
      if (selected) {
        throw SearchTemplateVectorRetrieval.noAnchor(
            opContext,
            template,
            anchor + " has no stored vectors for " + profiles.iterator().next());
      }
      return anchor + "@unusable";
    }
    try {
      return anchor + "@" + generatedAt(anchorAspect(opContext, anchor, deadlineNanos));
    } catch (RuntimeException | IOException e) {
      return unknownAnchor(opContext, template, resolved, anchor, selected, e);
    }
  }

  // A check or read that failed or ran out of time. The selected rung would run next, so the
  // search fails closed. A rung only a ladder reaches is decided if it runs, and the page is not
  // cached, since its key does not say what the anchor was.
  @Nonnull
  private static String unknownAnchor(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull ResolvedSearchTemplate resolved,
      @Nonnull Urn anchor,
      boolean selected,
      @Nonnull Exception failure) {
    if (selected) {
      SearchTemplateMetrics.vectorRetrieval(opContext, SearchTemplateVectorRetrieval.FAILED);
      throw new SearchTemplateFailClosedException(
          "Search template " + template.getUrn() + " failed to check the anchor " + anchor,
          failure);
    }
    resolved.setSkipResultCache(true);
    return anchor + "@unknown";
  }

  // When each of the anchor's stored embeddings was generated, by profile
  @Nonnull
  private static String generatedAt(@Nullable Aspect aspect) {
    if (aspect == null) {
      return "none";
    }
    final Map<String, Long> generated = new TreeMap<>();
    new SemanticContent(aspect.data())
        .getEmbeddings()
        .forEach((profile, embeddings) -> generated.put(profile, embeddings.getGeneratedAt()));
    return generated.toString();
  }

  // The metric decides whether an inherited floor applies and what an engine score means, so the
  // same profile id on another metric does not share a cached page. Provider tuning that changes
  // the query vector under the same profile id, such as a query instruction, is a configuration
  // rollout: as with any other configuration change, a page cached before it is served until it
  // expires. Changing how stored vectors are made needs a new profile id.
  @Nonnull
  private String profileIdentity(@Nonnull String id) {
    EmbeddingProfile profile = gates.profiles().profile(id);
    return profile == null ? id : id + "/" + profile.metric().trim().toLowerCase(Locale.ROOT);
  }

  /**
   * Whether this viewer may anchor a similarity search on {@code anchor}: view authorization lets
   * them see it, and the default search filters keep it, so it is not soft-deleted and not in a
   * hidden lifecycle stage unless the request opts in with {@code includeSoftDeleted} or {@code
   * includeHiddenLifecycleStages}. These are the filters every neighbor passes. The caller's filter
   * selects neighbors, not the anchor, so it does not apply. The store lookup must finish by the
   * deadline. Bind checks this before it reads the anchor and before the result cache, and the rung
   * reuses that answer.
   */
  public boolean anchorUsable(
      @Nonnull OperationContext opContext, @Nonnull Urn anchor, long deadlineNanos)
      throws IOException {
    if (ESAccessControlUtil.restrictUrn(opContext, anchor)) {
      return false;
    }
    final EntityIndexConfiguration entityIndex = elasticSearchConfiguration.getEntityIndex();
    final List<String> entityType = List.of(anchor.getEntityType());
    // The shards stop at the deadline too, and a timed-out response fails the check below
    final long remainingMillis =
        Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
    final SearchRequest request =
        new SearchRequest(EntitySearchIndexResolver.indexNames(opContext, entityType, entityIndex))
            .source(
                new SearchSourceBuilder()
                    .size(0)
                    .timeout(TimeValue.timeValueMillis(remainingMillis))
                    .query(
                        ESUtils.applyDefaultSearchFilters(
                            opContext,
                            entityType,
                            null,
                            QueryBuilders.boolQuery()
                                .filter(QueryBuilders.termQuery("urn", anchor.toString())),
                            entityIndex)));
    final SearchResponse response =
        within(
            () ->
                SearchClients.forEntityIndices(opContext, request, elasticSearchConfiguration)
                    .search(opContext, request, RequestOptions.DEFAULT),
            deadlineNanos);
    if (response.getFailedShards() > 0 || response.isTimedOut()) {
      throw new IOException("The anchor check reported a timed-out or failed shard");
    }
    return response.getHits().getTotalHits() != null && response.getHits().getTotalHits().value > 0;
  }

  /**
   * The anchor's {@code semanticContent}, or null when it has none, read by the deadline. Bind
   * reads it for the cache key.
   */
  @Nullable
  public Aspect anchorAspect(
      @Nonnull OperationContext opContext, @Nonnull Urn anchor, long deadlineNanos)
      throws IOException {
    return within(
        () ->
            opContext
                .getAspectRetriever()
                .getLatestAspectObject(
                    opContext, anchor, SearchTemplateVectorRetrieval.SEMANTIC_CONTENT_ASPECT_NAME),
        deadlineNanos);
  }

  // Runs a store call on the anchor executor and stops waiting at the deadline, so a stalled
  // lookup fails within the vector retrieval budget instead of holding the request
  private <T> T within(@Nonnull Callable<T> call, long deadlineNanos) throws IOException {
    if (deadlineNanos - System.nanoTime() <= 0) {
      throw new UncheckedTimeoutException("The vector retrieval deadline passed");
    }
    return awaitAnchorCall(startAnchorCall(call), deadlineNanos);
  }

  /**
   * Starts a similarity anchor's store call on the anchor executor. A full executor rejects it with
   * a {@link java.util.concurrent.RejectedExecutionException}.
   */
  @Nonnull
  public <T> Future<T> startAnchorCall(@Nonnull Callable<T> call) {
    return anchorExecutor.submit(call);
  }

  /**
   * Waits for a call {@link #startAnchorCall} started until the deadline. A call still running then
   * is cancelled, but a store call is not aborted and keeps its worker until it returns.
   */
  public <T> T awaitAnchorCall(@Nonnull Future<T> future, long deadlineNanos) throws IOException {
    try {
      return future.get(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new UncheckedTimeoutException("The anchor call ran past the vector retrieval deadline");
    } catch (InterruptedException e) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw new UncheckedTimeoutException("Interrupted while waiting for the anchor call");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException io) {
        throw io;
      }
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException("The anchor call failed", e.getCause());
    }
  }

  // A test that fills the pool builds its own from this, since the shared pool keeps the workers
  // that earlier tests started
  @VisibleForTesting
  public static ThreadPoolExecutor newAnchorExecutor() {
    // Sixteen workers, so one anchor's four kNN requests hold a quarter of the pool
    return new ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(32),
        new ThreadFactoryBuilder()
            .setNameFormat("search-template-anchor-%d")
            .setDaemon(true)
            .build());
  }

  @VisibleForTesting
  public void setAnchorExecutor(@Nonnull ExecutorService executor) {
    anchorExecutor = executor;
  }

  /** The vector retrieval budget bind gives a similarity anchor's check, read, and retrieval. */
  @VisibleForTesting
  public void setVectorTimeoutMillis(long millis) {
    vectorTimeoutMillis = millis;
  }

  private static void append(@Nonnull StringBuilder bodies, @Nullable String section) {
    if (section != null) {
      bodies.append('\n').append(section);
    }
  }

  @Nullable
  private SearchTemplateRolloutConfiguration rollout() {
    return templates(elasticSearchConfiguration).getRollout();
  }

  @Nullable
  private static String actor(@Nonnull OperationContext opContext) {
    try {
      return opContext.getActor().toString();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private boolean semanticReadEnabled() {
    return SemanticEntitySearchService.shouldReadSemanticV3(
        elasticSearchConfiguration.getEntityIndex());
  }

  @Nonnull
  public QueryBuilder render(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    return renderer.render(opContext, entitySpecs, query, fulltext);
  }

  @Nullable
  private static SearchClientShim.SearchEngineType engineType(@Nonnull OperationContext opContext) {
    try {
      return opContext
          .getSearchContext()
          .requireSearchClusterAccess()
          .engineTypeFor(SearchComponent.SEARCH_V3);
    } catch (RuntimeException e) {
      return null;
    }
  }

  @Nonnull
  private static SearchTemplateConfiguration templates(
      @Nonnull ElasticSearchConfiguration configuration) {
    if (configuration.getSearch() == null
        || configuration.getSearch().getV3() == null
        || configuration.getSearch().getV3().getTemplates() == null) {
      return new SearchTemplateConfiguration();
    }
    return configuration.getSearch().getV3().getTemplates();
  }
}
