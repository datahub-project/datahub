package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.v3.StructuredPropertyQueryFields;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

/** Selects and renders templates when keyword read and {@code templates.enabled} are both on. */
public class SearchTemplateCoordinator {
  private final ElasticSearchConfiguration elasticSearchConfiguration;
  private final SearchTemplateLoader loader;
  private final SearchTemplateSelector selector;
  private final SearchV3QueryBuilder queryBuilder;
  private final SearchTemplateRenderer renderer;
  private final ExecutorService ladderExecutor;

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

  public boolean active() {
    SearchTemplateConfiguration templates = templates(elasticSearchConfiguration);
    return templates.isEnabled()
        && EntitySearchIndexResolver.shouldReadV3(elasticSearchConfiguration.getEntityIndex());
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
    if (!active()) {
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
    noteIdentity(opContext, resolved, scroll, documentRerank);
    return opContext.withResolvedSearchTemplate(resolved);
  }

  /**
   * Cache identity is every rung this request will run. Scroll leaves {@code next} unread, so an
   * unread rung the process cannot run does not fail the scroll. Document rerank is advertised on a
   * search when semantic read is on and the entity list includes {@code document}. Scroll drops
   * knn, so a knn section does not fail the scroll.
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
      identity += "|sp=" + StructuredPropertyQueryFields.catalogStamp();
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
    if (!seen.add(urn) || depth > SearchTemplateLoader.MAX_PARENT_DEPTH) {
      return;
    }
    MergedSearchTemplate template = loader.load(opContext, urn);
    SearchTemplateCapabilities.requireSupported(template, documentRerank, scroll);
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
