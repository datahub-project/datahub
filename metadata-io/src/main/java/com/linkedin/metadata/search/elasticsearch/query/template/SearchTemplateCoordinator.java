package com.linkedin.metadata.search.elasticsearch.query.template;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateRolloutConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.search.template.SearchTemplateLadder;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.QueryBuilder;

/** Selects and renders templates when keyword read and {@code templates.enabled} are both on. */
@Slf4j
public class SearchTemplateCoordinator {
  private final ElasticSearchConfiguration elasticSearchConfiguration;
  private final SearchTemplateLoader loader;
  private final SearchTemplateSelector selector;
  private final SearchV3QueryBuilder queryBuilder;
  private final SearchTemplateRenderer renderer;
  private final ExecutorService ladderExecutor;
  private final AtomicInteger shadowRenders = new AtomicInteger();
  @Nullable private volatile String lastShadowDsl;

  public SearchTemplateCoordinator(
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull SearchTemplateLoader loader) {
    this(elasticSearchConfiguration, customSearchConfiguration, loader, ForkJoinPool.commonPool());
  }

  public SearchTemplateCoordinator(
      @Nonnull ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      @Nonnull SearchTemplateLoader loader,
      @Nonnull ExecutorService ladderExecutor) {
    this.elasticSearchConfiguration = elasticSearchConfiguration;
    this.loader = loader;
    this.ladderExecutor = ladderExecutor;
    SearchTemplateConfiguration templates = templates(elasticSearchConfiguration);
    this.selector = new SearchTemplateSelector(templates, loader);
    this.queryBuilder =
        new SearchV3QueryBuilder(
            loader, elasticSearchConfiguration.getSearch(), customSearchConfiguration);
    this.renderer = new SearchTemplateRenderer(queryBuilder);
  }

  @Nonnull
  public SearchV3QueryBuilder queryBuilder() {
    return queryBuilder;
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
   */
  public void shadow(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      boolean scroll) {
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
    if (servedUrn == null || !semanticReadEnabled()) {
      resolved.setKnn(false, 1f, 1f, 100);
      return;
    }
    MergedSearchTemplate template = loader.load(opContext, servedUrn);
    if (template.getKnn() == null) {
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
        selector.select(opContext, query, fulltext, engineType(opContext));
    boolean documentRerank =
        !scroll && semanticReadEnabled() && SearchTemplateHybrid.includesDocument(entityNames);
    resolved.setExecutionCapabilities(scroll, documentRerank);
    try {
      noteIdentity(opContext, resolved, scroll, documentRerank);
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
    ResolvedSearchTemplate resolved = selector.select(opContext, query, fulltext, engine);
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
    String rejection = SearchTemplateCapabilities.rejectionCode(template, documentRerank, scroll);
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
        ladderRungs(bound, resolved.getUrn(), !scroll));
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
   * Cache identity includes every reachable rung. Capability checks happen when a rung executes, so
   * an unsupported next does not fail a request that stops before reaching it.
   */
  private void noteIdentity(
      @Nonnull OperationContext opContext,
      @Nonnull ResolvedSearchTemplate resolved,
      boolean scroll,
      boolean documentRerank) {
    List<String> rungs = new ArrayList<>();
    StringBuilder bodies = new StringBuilder();
    boolean[] nameAndValue = new boolean[1];
    boolean[] classpathBridge = new boolean[1];
    collect(
        opContext,
        resolved.getUrn(),
        rungs,
        bodies,
        new HashSet<>(),
        0,
        !scroll,
        documentRerank,
        scroll,
        nameAndValue,
        classpathBridge);
    String identity = String.join("+", rungs);
    if (!classpathBridge[0] && SearchTemplateViewer.references(bodies.toString())) {
      identity += SearchTemplateViewer.identity(opContext, bodies.toString());
    }
    if (!classpathBridge[0] && nameAndValue[0]) {
      identity += "|sp=" + queryBuilder.structuredPropertyCatalogStamp(opContext);
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
      boolean documentRerank,
      boolean scroll,
      @Nonnull boolean[] nameAndValue,
      @Nonnull boolean[] classpathBridge) {
    if (!seen.add(urn) || depth > loader.maxParentDepth()) {
      return;
    }
    MergedSearchTemplate template = loader.load(opContext, urn);
    if (depth == 0) {
      SearchTemplateCapabilities.requireSupported(template, documentRerank, scroll);
    }
    if (depth == 0) {
      classpathBridge[0] = template.isClasspathBridge();
    }
    if (!template.isClasspathBridge() && template.getStructuredPropertyQuery() != null) {
      nameAndValue[0] = true;
    }
    rungs.add(template.getUrn() + "@" + template.getLastModified());
    append(bodies, template.getQuery());
    template.getQueryFragments().values().forEach(value -> append(bodies, value));
    append(bodies, template.getClauses());
    append(bodies, template.getFunctionScore());
    if (template.getBoosts() != null) {
      template.getBoosts().forEach(boost -> append(bodies, boost.getValues()));
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
            documentRerank,
            false,
            nameAndValue,
            classpathBridge);
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
          documentRerank,
          false,
          nameAndValue,
          classpathBridge);
    }
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
    if (elasticSearchConfiguration.getEntityIndex() == null
        || elasticSearchConfiguration.getEntityIndex().getV3() == null) {
      return false;
    }
    return elasticSearchConfiguration.getEntityIndex().getV3().isSemanticReadEnabled();
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
