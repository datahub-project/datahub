package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateRolloutConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/** Surface rollout, canary buckets, shadow rendering, and the Search V3 Java fallback. */
public class SearchTemplateRolloutTest {
  private static final String QUOTED = "\"orders by region\"";

  private SearchTemplateFixtures.Harness harness;

  @AfterMethod
  public void clear() {
    harness = null;
  }

  @Test
  public void testBucketIsStableAndBounded() {
    String key =
        SearchTemplateRollout.bucketKey(
            "urn:li:corpuser:ada", SearchTemplateSurfaces.SEARCH_ENTITIES);
    int bucket = SearchTemplateRollout.bucket(key);
    assertTrue(bucket >= 0 && bucket < SearchTemplateRollout.BUCKETS);
    assertEquals(SearchTemplateRollout.bucket(key), bucket);
    assertEquals(
        SearchTemplateRollout.bucketKey(null, null), SearchTemplateRollout.bucketKey("  ", ""));
  }

  @Test
  public void testPercentKeepsOneActorOnOneSide() {
    String inside = null;
    String outside = null;
    for (int i = 0; i < 400 && (inside == null || outside == null); i++) {
      String key =
          SearchTemplateRollout.bucketKey(
              "urn:li:corpuser:user" + i, SearchTemplateSurfaces.SEARCH_ENTITIES);
      int bucket = SearchTemplateRollout.bucket(key);
      if (bucket < 25) {
        inside = key;
      } else {
        outside = key;
      }
    }
    assertNotNull(inside);
    assertNotNull(outside);
    SearchTemplateRolloutConfiguration rollout = rollout("active", 25);
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.SEARCH_ENTITIES, inside), active());
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.SEARCH_ENTITIES, outside), off());
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.SEARCH_ENTITIES, inside), active());
    rollout.setPercent(0);
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.SEARCH_ENTITIES, inside), off());
    rollout.setPercent(100);
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.SEARCH_ENTITIES, outside), active());
  }

  @Test
  public void testSurfaceModesAndAliases() {
    SearchTemplateRolloutConfiguration rollout = rollout("off", 100);
    rollout.setSearchEntities("canary");
    rollout.setSearchEntitiesPercent(100);
    rollout.setScrollEntities("render-only");
    rollout.setAggregateEntities("shadow");
    String key = SearchTemplateRollout.bucketKey("urn:li:corpuser:ada", "ignored");
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.SEARCH_ENTITIES, key), active());
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.SCROLL_ENTITIES, key), shadow());
    assertEquals(effect(true, rollout, SearchTemplateSurfaces.AGGREGATE_ENTITIES, key), shadow());
    assertEquals(effect(true, rollout, null, key), off());
    assertEquals(effect(false, rollout, SearchTemplateSurfaces.SEARCH_ENTITIES, key), off());
    expectThrows(
        IllegalArgumentException.class,
        () -> effect(true, rollout("sideways", 100), SearchTemplateSurfaces.SEARCH_ENTITIES, key));
  }

  @Test
  public void testShippedDefaultStaysOnTheJavaBuilder() throws Exception {
    SearchTemplateConfiguration configuration = new SearchTemplateConfiguration();
    assertFalse(configuration.isEnabled());
    assertEquals(configuration.getRollout().getMode(), "active");
    assertEquals(configuration.getRollout().getPercent(), 100);
    assertFalse(SearchTemplateRetirement.TEMPLATES_CONFIGURED_DEFAULT);
    assertFalse(SearchTemplateRetirement.JAVA_LEXICAL_BUILDER_RETIRED);
    assertTrue(V3SearchQueryBuilder.FROZEN_AGAINST_NEW_RELEVANCE_POLICY);
    assertTrue(SearchTemplateRetirement.DELETION_DEFERRED.contains("SEARCH_V3_TEMPLATES_ENABLED"));
    String yaml = Files.readString(applicationYaml());
    assertTrue(
        yaml.contains("enabled: ${SEARCH_V3_TEMPLATES_ENABLED:false}"),
        "templates stay off unless an operator opts in");
    assertEquals(
        effect(false, configuration.getRollout(), SearchTemplateSurfaces.SEARCH_ENTITIES, "actor"),
        off());
    assertEquals(
        effect(true, null, SearchTemplateSurfaces.SCROLL_ENTITIES, "actor"),
        active(),
        "an enabled flag with no rollout still serves every surface");
  }

  @Test
  public void testFilterLookupSkipsTemplateExecution() {
    open();
    harness.templates.getRollout().setMode("active");
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            harness.elastic(),
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    OperationContext search =
        harness.opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES);
    assertTrue(coordinator.executes(search));
    assertFalse(coordinator.executes(search.withSkipTemplateExecution(true)));
  }

  @Test
  public void testShadowRendersAndRollbackKeepsTheJavaQuery() {
    open();
    harness.templates.getRollout().setMode("shadow");
    ElasticSearchConfiguration elastic = harness.elastic();
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    V3SearchQueryBuilder builder = new V3SearchQueryBuilder(elastic.getSearch(), null);
    OperationContext search =
        harness.opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES);
    QueryBuilder javaQuery = builder.buildQuery(search, List.of(dataset()), QUOTED, true);
    assertTrue(javaQuery.toString().contains("dis_max"));

    search = search.withSearchTemplateRuntime(coordinator);
    QueryBuilder shadowed = builder.buildQuery(search, List.of(dataset()), QUOTED, true);
    assertTrue(shadowed.toString().contains("dis_max"), shadowed.toString());
    assertTrue(coordinator.shadowRenderCount() > 0);
    assertNotNull(coordinator.lastShadowDsl());
    assertFalse(coordinator.lastShadowDsl().contains("dis_max"), coordinator.lastShadowDsl());
    assertNotEquals(shadowed.toString(), coordinator.lastShadowDsl());
    assertNull(
        coordinator
            .bind(search, QUOTED, false, List.of("dataset"))
            .getSearchContext()
            .getResolvedSearchTemplate());

    int rendered = coordinator.shadowRenderCount();
    harness.templates.setEnabled(false);
    harness.templates.getRollout().setMode("active");
    harness.templates.getRollout().setPercent(100);
    harness.templates.getRollout().setSearchEntities("active");
    QueryBuilder rolledBack = builder.buildQuery(search, List.of(dataset()), QUOTED, true);
    assertTrue(rolledBack.toString().contains("dis_max"));
    assertEquals(coordinator.shadowRenderCount(), rendered);
    assertNull(
        coordinator
            .bind(search, QUOTED, false, List.of("dataset"))
            .getSearchContext()
            .getResolvedSearchTemplate());
  }

  @Test
  public void testActiveSurfaceServesTheTemplateAndAnOffSurfaceDoesNot() {
    open();
    harness.templates.getRollout().setMode("off");
    harness.templates.getRollout().setSearchEntities("active");
    ElasticSearchConfiguration elastic = harness.elastic();
    SearchTemplateCoordinator coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    harness.opContext = harness.opContext.withSearchTemplateRuntime(coordinator);
    EntitySpec dataset = dataset();
    V3SearchQueryBuilder builder = new V3SearchQueryBuilder(elastic.getSearch(), null);
    OperationContext search =
        harness.opContext.withSearchSurface(SearchTemplateSurfaces.SEARCH_ENTITIES);
    QueryBuilder served = builder.buildQuery(search, List.of(dataset), QUOTED, true);
    assertFalse(served.toString().contains("dis_max"), served.toString());
    assertEquals(
        coordinator
            .bind(search, QUOTED, false, List.of("dataset"))
            .getSearchContext()
            .getResolvedSearchTemplate()
            .getUrn(),
        "urn:li:searchTemplate:exact");
    assertEquals(coordinator.shadowRenderCount(), 0);

    OperationContext scroll =
        harness.opContext.withSearchSurface(SearchTemplateSurfaces.SCROLL_ENTITIES);
    QueryBuilder javaScroll = builder.buildQuery(scroll, List.of(dataset), QUOTED, true);
    assertTrue(javaScroll.toString().contains("dis_max"), javaScroll.toString());
    assertNull(
        coordinator
            .bind(scroll, QUOTED, true, List.of("dataset"))
            .getSearchContext()
            .getResolvedSearchTemplate());
  }

  private void open() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    harness.installCatalog();
    harness.installShippedTemplates();
    harness.templates.setEnabled(true);
    harness.templates.setDefaults(
        new LinkedHashMap<>(
            Map.of(
                "fulltext", slot("urn:li:searchTemplate:search"),
                "structured", slot("urn:li:searchTemplate:structured"))));
    harness.templates.setSurfaces(
        new LinkedHashMap<>(
            Map.of(
                SearchTemplateSurfaces.SEARCH_ENTITIES,
                Map.of("fulltext", slot("urn:li:searchTemplate:searchLight")),
                SearchTemplateSurfaces.SCROLL_ENTITIES,
                Map.of("fulltext", slot("urn:li:searchTemplate:search")),
                SearchTemplateSurfaces.AGGREGATE_ENTITIES,
                Map.of("fulltext", slot("urn:li:searchTemplate:search")))));
  }

  private static Map<String, Object> slot(String urn) {
    Map<String, Object> slot = new LinkedHashMap<>();
    slot.put("default", urn);
    return slot;
  }

  private EntitySpec dataset() {
    return harness.registry.getEntitySpec("dataset");
  }

  private static SearchTemplateRolloutConfiguration rollout(String mode, int percent) {
    SearchTemplateRolloutConfiguration rollout = new SearchTemplateRolloutConfiguration();
    rollout.setMode(mode);
    rollout.setPercent(percent);
    return rollout;
  }

  private static SearchTemplateRollout.Effect effect(
      boolean enabled, SearchTemplateRolloutConfiguration rollout, String surface, String key) {
    return SearchTemplateRollout.effect(enabled, rollout, surface, key);
  }

  private static SearchTemplateRollout.Effect off() {
    return SearchTemplateRollout.Effect.OFF;
  }

  private static SearchTemplateRollout.Effect shadow() {
    return SearchTemplateRollout.Effect.SHADOW;
  }

  private static SearchTemplateRollout.Effect active() {
    return SearchTemplateRollout.Effect.ACTIVE;
  }

  private static Path applicationYaml() {
    Path moduleRelative =
        Path.of(
            "..",
            "metadata-service",
            "configuration",
            "src",
            "main",
            "resources",
            "application.yaml");
    if (Files.exists(moduleRelative)) {
      return moduleRelative;
    }
    return Path.of(
        "metadata-service", "configuration", "src", "main", "resources", "application.yaml");
  }
}
