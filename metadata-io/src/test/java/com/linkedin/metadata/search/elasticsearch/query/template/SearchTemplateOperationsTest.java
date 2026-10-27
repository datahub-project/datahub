package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateDryRender.CapabilityDecision;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.search.template.SearchTemplateDefaultRoute;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRoute;
import com.linkedin.search.template.SearchTemplateRouteArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Dry render, bounded metrics, engine slots, and template rollback. */
public class SearchTemplateOperationsTest {
  private static final String ROUTER = "urn:li:searchTemplate:opsRouter";
  private static final String LEAF = "urn:li:searchTemplate:opsLeaf";
  private static final String PARENT = "urn:li:searchTemplate:opsParent";
  private static final String CHILD = "urn:li:searchTemplate:opsChild";
  private static final String POSTGRES = "urn:li:searchTemplate:opsPostgres";
  private static final String LIGHT = "urn:li:searchTemplate:opsLight";
  private static final String FULL = "urn:li:searchTemplate:opsFull";

  private SearchTemplateFixtures.Harness harness;
  private SimpleMeterRegistry registry;
  private OperationContext opContext;
  private SearchTemplateCoordinator coordinator;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    registry = new SimpleMeterRegistry();
    MetricUtils metrics = MetricUtils.builder().registry(registry).build();
    opContext =
        harness.opContext.toBuilder()
            .systemTelemetryContext(
                SystemTelemetryContext.TEST.toBuilder().metricUtils(metrics).build())
            .build(harness.opContext.getSessionActorContext(), false);
    coordinator =
        new SearchTemplateCoordinator(
            harness.elastic(),
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    opContext = opContext.withSearchTemplateRuntime(coordinator);
  }

  @Test
  public void testPreviewResolvesRouteWithoutSearch() {
    storeRouter();
    SearchTemplateDryRender preview =
        coordinator.preview(opContext, "orders", List.of("dataset"), false, null);

    assertEquals(preview.resolvedUrn(), LEAF);
    assertEquals(preview.routeChain(), List.of(ROUTER, LEAF));
    assertEquals(preview.parentChain(), List.of(LEAF));
    assertEquals(preview.engineSlot(), "default");
    assertTrue(preview.queryDsl().contains("orders"));
    assertFalse(preview.queriedFields().isEmpty());
    assertTrue(preview.clauseCount() >= 1);
    assertTrue(preview.estimatedClauseCost() >= 0);
    assertTrue(accepted(preview, "keyword"));
    assertTrue(accepted(preview, "sections"));
    assertEquals(preview.ladderRungs(), List.of(LEAF));
    assertNull(registry.find(SearchTemplateMetrics.SERVED).counter());

    coordinator.preview(opContext, "nope", List.of("dataset"), false, null);
    assertTrue(count(SearchTemplateMetrics.FALLBACK, "kind", "route") >= 1);
    assertTrue(count(SearchTemplateMetrics.SELECTED, "template", "custom", "slot", "default") >= 1);
    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.preview(opContext, "orders", List.of(), false, null));
  }

  @Test
  public void testPreviewReportsParentChainAndRejectedKnn() {
    SearchTemplateProperties parent = template("Parent", "{ \"match_all\": {} }");
    harness.store(PARENT, parent);
    SearchTemplateProperties child = template("Child", null);
    child.setParent(UrnUtils.getUrn(PARENT));
    child.setKnn("{ \"field\": \"embedding\", \"k\": 10 }");
    harness.store(CHILD, child);
    OperationContext pinned =
        opContext.withSearchFlags(
            flags -> flags.setFulltext(true).setSearchTemplate(UrnUtils.getUrn(CHILD)));

    SearchTemplateDryRender preview =
        coordinator.preview(pinned, "orders", List.of("dataset"), false, null);

    assertEquals(preview.resolvedUrn(), CHILD);
    assertEquals(preview.parentChain(), List.of(CHILD, PARENT));
    assertEquals(preview.engineSlot(), SearchTemplateMetrics.SLOT_CALLER);
    assertTrue(preview.queryDsl().contains("match_all"));
    assertFalse(accepted(preview, "sections"));
    assertEquals(detail(preview, "sections"), "knn");
    assertTrue(count(SearchTemplateMetrics.SELECTED, "template", "custom", "slot", "caller") >= 1);
  }

  @Test
  public void testEngineSlotsAndPostgresAreNotExecuted() {
    assertEquals(
        SearchTemplateDialects.engineKey(SearchClientShim.SearchEngineType.ELASTICSEARCH_8),
        "elasticsearch8");
    assertEquals(
        SearchTemplateDialects.engineKey(SearchClientShim.SearchEngineType.ELASTICSEARCH_9),
        "elasticsearch9");
    assertEquals(
        SearchTemplateDialects.engineKey(SearchClientShim.SearchEngineType.OPENSEARCH_2),
        "opensearch2");
    assertEquals(
        SearchTemplateDialects.engineKey(SearchClientShim.SearchEngineType.OPENSEARCH_3),
        "opensearch3");
    assertEquals(SearchTemplateDialects.engineKey(null), "default");
    assertEquals(
        SearchTemplateDialects.engineKey(SearchClientShim.SearchEngineType.UNKNOWN), "default");
    assertEquals(
        SearchTemplateDialects.requestDialect(SearchClientShim.SearchEngineType.OPENSEARCH_3),
        SearchTemplateDialects.ELASTICSEARCH);

    storeRouter();
    SearchTemplateProperties postgres = template("Postgres", "{ \"match_all\": {} }");
    postgres.setDialect(SearchTemplateDialect.POSTGRES);
    harness.store(POSTGRES, postgres);
    harness
        .templates
        .getDefaults()
        .put(
            "fulltext",
            new HashMap<>(
                Map.of(
                    "opensearch3", POSTGRES,
                    "default", ROUTER)));

    SearchTemplateDryRender preview =
        coordinator.preview(
            opContext,
            "orders",
            List.of("dataset"),
            false,
            SearchClientShim.SearchEngineType.OPENSEARCH_3);

    assertEquals(preview.engineSlot(), "default");
    assertEquals(preview.resolvedUrn(), LEAF);

    harness
        .templates
        .getDefaults()
        .put("fulltext", new HashMap<>(Map.of("opensearch3", ROUTER, "default", LEAF)));
    SearchTemplateDryRender selected =
        coordinator.preview(
            opContext,
            "orders",
            List.of("dataset"),
            false,
            SearchClientShim.SearchEngineType.OPENSEARCH_3);
    assertEquals(selected.engineSlot(), "opensearch3");
    assertEquals(selected.resolvedUrn(), LEAF);
  }

  @Test
  public void testDisablingTemplatesOrKeywordReadRollsBack() {
    harness.templates.setEnabled(false);
    SearchTemplateCoordinator disabled =
        new SearchTemplateCoordinator(
            harness.elastic(),
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    assertFalse(disabled.active());
    assertNull(disabled.bind(opContext, "orders").getSearchContext().getResolvedSearchTemplate());

    harness.templates.setEnabled(true);
    ElasticSearchConfiguration keywordOff = harness.elastic();
    EntityIndexVersionConfiguration indexV2 = new EntityIndexVersionConfiguration();
    indexV2.setEnabled(true);
    keywordOff.getEntityIndex().setV2(indexV2);
    keywordOff.getEntityIndex().getV3().setKeywordReadEnabled(false);
    assertFalse(
        new SearchTemplateCoordinator(
                keywordOff,
                null,
                harness.loader,
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty())
            .active());
    assertTrue(
        new SearchTemplateCoordinator(
                harness.elastic(),
                null,
                harness.loader,
                ForkJoinPool.commonPool(),
                SearchTemplateComponentRegistry.empty())
            .active());
  }

  @Test
  public void testLadderFallbackAndRungLatency() throws Exception {
    harness.store(FULL, template("Full", "{ \"match_all\": {} }"));
    SearchTemplateProperties light = template("Light", "{ \"match_all\": {} }");
    SearchTemplateLadder ladder = new SearchTemplateLadder();
    ladder.setSelect(new StringArray("multiWord"));
    ladder.setNext(UrnUtils.getUrn(FULL));
    light.setLadder(ladder);
    harness.store(LIGHT, light);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      SearchTemplateWalk.Served<String> served =
          SearchTemplateWalk.walk(
              opContext,
              harness.loader,
              executor,
              LIGHT,
              new SearchTemplateWalk.Request(
                  "orders", null, null, 10, SearchTemplateWalk.identity()),
              urn -> SearchTemplateWalk.AttemptResult.ok(urn, 1, List.of(), List.of(), false, null),
              (first, entities, total) -> first);
      assertEquals(served.urn(), FULL);
    } finally {
      executor.shutdownNow();
    }

    assertTrue(count(SearchTemplateMetrics.FALLBACK, "kind", "ladder") >= 1);
    assertTrue(count(SearchTemplateMetrics.SERVED, "template", "custom") >= 1);
    Timer rung = registry.find(SearchTemplateMetrics.RUNG).tag("position", "next").timer();
    assertNotNull(rung);
    assertTrue(rung.count() >= 1);
    assertNotNull(registry.find(SearchTemplateMetrics.LADDER_DEPTH).summary());
  }

  @Test
  public void testClauseBudgetReductionAndSelectFailure() {
    storeRouter();
    StringBuilder query = new StringBuilder();
    for (int i = 0; i < 80; i++) {
      if (i > 0) {
        query.append(' ');
      }
      query.append("word").append(i);
    }
    coordinator.preview(opContext, query.toString(), List.of("dataset"), false, null);
    assertTrue(count(SearchTemplateMetrics.CLAUSE_BUDGET, "kind", "terms") >= 1);

    harness.templates.getDefaults().clear();
    assertThrows(
        SearchTemplateException.class,
        () -> coordinator.preview(opContext, "orders", List.of("dataset"), false, null));
    assertTrue(count(SearchTemplateMetrics.RENDER_FAILURE, "stage", "select") >= 1);
  }

  private void storeRouter() {
    harness.store(LEAF, template("Leaf", "{ \"term\": { \"name\": \"{{query}}\" } }"));
    SearchTemplateProperties router = template("Router", null);
    SearchTemplateRoute route = new SearchTemplateRoute();
    route.setQueryRegex("orders");
    route.setTemplate(UrnUtils.getUrn(LEAF));
    router.setRoutes(new SearchTemplateRouteArray(route));
    SearchTemplateDefaultRoute fallback = new SearchTemplateDefaultRoute();
    fallback.setTemplate(UrnUtils.getUrn(LEAF));
    router.setDefaultRoute(fallback);
    harness.store(ROUTER, router);
    harness.templates.getDefaults().put("fulltext", new HashMap<>(Map.of("default", ROUTER)));
  }

  private static SearchTemplateProperties template(String name, String query) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(name);
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    properties.setCapabilities(new StringArray("keyword"));
    if (query != null) {
      properties.setQuery(query);
    }
    return properties;
  }

  private static boolean accepted(SearchTemplateDryRender preview, String name) {
    return preview.capabilities().stream()
        .anyMatch(decision -> decision.name().equals(name) && decision.accepted());
  }

  private static String detail(SearchTemplateDryRender preview, String name) {
    return preview.capabilities().stream()
        .filter(decision -> decision.name().equals(name))
        .map(CapabilityDecision::detail)
        .findFirst()
        .orElse(null);
  }

  private double count(String name, String... tags) {
    Counter counter = registry.find(name).tags(tags).counter();
    return counter == null ? 0 : counter.count();
  }
}
