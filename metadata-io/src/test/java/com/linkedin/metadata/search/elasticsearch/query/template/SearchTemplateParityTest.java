package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.utils.QueryUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Shipped templates against the Java V2.5 builder. Shared signals are the clauses both sides must
 * keep. A case with a note records a difference instead of comparing the whole query JSON.
 */
public class SearchTemplateParityTest {
  private static final String URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,analytics.orders,PROD)";
  private static final String LONG_QUERY = String.join(" ", Collections.nCopies(20, "zorbel"));
  private static final String CERTIFICATION = "structuredProperties.certification_status";

  private SearchTemplateFixtures.Harness harness;
  private ElasticSearchConfiguration elastic;
  private SearchTemplateCoordinator coordinator;
  private V3SearchQueryBuilder javaBuilder;
  private EntitySpec dataset;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    harness.installCatalog();
    harness.installShippedTemplates();
    installShippedSelection();
    elastic = harness.elastic();
    coordinator =
        new SearchTemplateCoordinator(
            elastic,
            null,
            harness.loader,
            ForkJoinPool.commonPool(),
            SearchTemplateComponentRegistry.empty());
    javaBuilder = new V3SearchQueryBuilder(elastic.getSearch(), null);
    dataset = harness.registry.getEntitySpec("dataset");
    harness.opContext = harness.opContext.withSearchTemplateRuntime(coordinator);
  }

  @DataProvider
  public static Object[][] corpus() {
    return cases().stream().map(row -> new Object[] {row}).toArray(Object[][]::new);
  }

  @Test
  public void testCorpusCoversTheRequestedSurfaces() {
    Set<String> categories = cases().stream().map(Case::category).collect(Collectors.toSet());
    assertEquals(
        categories,
        Set.of(
            "identity",
            "exact",
            "dotted",
            "alphanumeric",
            "typo",
            "long",
            "joined",
            "structured",
            "structuredProperty",
            "lineage",
            "scroll",
            "light",
            "browse"));
  }

  @Test(dataProvider = "corpus")
  public void testShippedTemplateVersusJava(Case row) {
    boolean documented =
        !row.templateKeeps().isEmpty()
            || !row.javaKeeps().isEmpty()
            || !row.templateLacks().isEmpty()
            || !row.javaLacks().isEmpty();
    if (documented) {
      assertFalse(row.note().isBlank(), row.name());
    }
    OperationContext requested = request(row);
    QueryBuilder javaQuery =
        javaBuilder.buildQuery(
            requested, List.of(dataset), row.query(), row.fulltext(), row.light());
    assertNotNull(javaQuery, row.name());
    OperationContext bound =
        coordinator.bind(requested, row.query(), row.scroll(), List.of("dataset"));
    assertEquals(
        bound.getSearchContext().getResolvedSearchTemplate().getUrn(), row.template(), row.name());
    String template =
        coordinator
            .queryBuilder()
            .render(bound, List.of(dataset), row.query(), row.fulltext(), null, row.scroll())
            .query()
            .toString();
    String java = javaQuery.toString();
    require(row.name() + " template", template, row.shared());
    require(row.name() + " java", java, row.shared());
    require(row.name() + " template only", template, row.templateKeeps());
    require(row.name() + " java only", java, row.javaKeeps());
    reject(row.name() + " template", template, row.templateLacks());
    reject(row.name() + " java", java, row.javaLacks());
  }

  /**
   * Filters, caller sort, and caller facets are request mechanics. Count-only still runs the
   * lexical query and those facets, and it drops highlight and rescore. Scroll and aggregate both
   * resolve the fulltext leaf.
   */
  @Test
  public void testFulltextNamePrefixBoostMatchesJava() throws Exception {
    OperationContext requested =
        harness
            .opContext
            .withSearchFlags(flags -> flags.setFulltext(true))
            .withSearchSurface(SearchTemplateSurfaces.SCROLL_ENTITIES);
    QueryBuilder javaQuery =
        javaBuilder.buildQuery(requested, List.of(dataset), "orders", true, false);
    OperationContext bound = coordinator.bind(requested, "orders", true, List.of("dataset"));
    assertEquals(
        bound.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
    String template =
        coordinator
            .queryBuilder()
            .render(bound, List.of(dataset), "orders", true, null, true)
            .query()
            .toString();
    assertEquals(nameTextPrefixBoost(template), nameTextPrefixBoost(javaQuery.toString()));
  }

  private static double nameTextPrefixBoost(String query) throws Exception {
    List<Double> boosts = new ArrayList<>();
    collectNameTextPrefix(SearchTemplateFixtures.JSON.readTree(query), boosts);
    assertFalse(boosts.isEmpty(), query);
    double boost = boosts.get(0);
    for (double other : boosts) {
      assertEquals(other, boost, 0.0001d, query);
    }
    return boost;
  }

  private static void collectNameTextPrefix(JsonNode node, List<Double> boosts) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode prefix = node.get("match_phrase_prefix");
      if (prefix != null && prefix.has("name.text") && prefix.get("name.text").has("boost")) {
        boosts.add(prefix.get("name.text").get("boost").asDouble());
      }
      node.fields().forEachRemaining(entry -> collectNameTextPrefix(entry.getValue(), boosts));
    } else if (node.isArray()) {
      node.forEach(child -> collectNameTextPrefix(child, boosts));
    }
  }

  @Test
  public void testFiltersSortFacetsScrollAndCountOnly() {
    OperationContext scroll =
        harness.opContext.withSearchSurface(SearchTemplateSurfaces.SCROLL_ENTITIES);
    OperationContext aggregate =
        coordinator.bind(
            harness.opContext.withSearchSurface(SearchTemplateSurfaces.AGGREGATE_ENTITIES),
            "orders",
            false,
            List.of("dataset"));
    assertEquals(
        aggregate.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);
    OperationContext scrollBound = coordinator.bind(scroll, "orders", true, List.of("dataset"));
    assertEquals(
        scrollBound.getSearchContext().getResolvedSearchTemplate().getUrn(),
        SEARCH_TEMPLATE_FULLTEXT_URN);

    Filter filter = QueryUtils.newFilter("platform", "urn:li:dataPlatform:hive");
    List<SortCriterion> sort =
        List.of(new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING));
    SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            scroll,
            dataset,
            elastic,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    SearchRequest templateRequest =
        handler.getSearchRequest(scroll, "orders", filter, sort, 0, 10, List.of("platform"));
    SearchRequest templateCount =
        handler.getSearchRequest(scroll, "orders", filter, null, 0, 0, List.of("platform"));
    assertCountOnly(templateCount);

    OperationContext javaScroll = scroll.withSearchTemplateRuntime(null);
    SearchRequest javaRequest =
        handler.getSearchRequest(javaScroll, "orders", filter, sort, 0, 10, List.of("platform"));
    SearchRequest javaCount =
        handler.getSearchRequest(javaScroll, "orders", filter, null, 0, 0, List.of("platform"));
    assertCountOnly(javaCount);
    String value = "urn:li:dataPlatform:hive";
    assertTrue(templateRequest.source().toString().contains(value), "template filter");
    assertTrue(javaRequest.source().toString().contains(value), "java filter");
    assertEquals(
        sortText(templateRequest),
        sortText(javaRequest),
        "Caller sort is assembled in Java on both paths");
    assertTrue(aggregationNames(templateRequest).contains("platform"), "template facet");
    assertTrue(aggregationNames(javaRequest).contains("platform"), "java facet");
    assertTrue(aggregationNames(templateCount).contains("platform"));
    assertTrue(aggregationNames(javaCount).contains("platform"));
  }

  private static void assertCountOnly(SearchRequest request) {
    assertEquals(request.source().size(), 0);
    assertNull(request.source().highlighter());
    assertTrue(request.source().rescores() == null || request.source().rescores().isEmpty());
    assertTrue(request.source().query().toString().contains("orders"), request.source().toString());
  }

  private static List<String> sortText(SearchRequest request) {
    List<SortBuilder<?>> sorts = request.source().sorts();
    if (sorts == null) {
      return List.of();
    }
    return sorts.stream().map(SortBuilder::toString).toList();
  }

  private static List<String> aggregationNames(SearchRequest request) {
    if (request.source().aggregations() == null) {
      return List.of();
    }
    List<String> names = new ArrayList<>();
    for (AggregationBuilder aggregation :
        request.source().aggregations().getAggregatorFactories()) {
      names.add(aggregation.getName());
    }
    return names;
  }

  private OperationContext request(Case row) {
    OperationContext context =
        harness.opContext.withSearchFlags(flags -> flags.setFulltext(row.fulltext()));
    return row.surface() == null ? context : context.withSearchSurface(row.surface());
  }

  private void installShippedSelection() {
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

  private static void require(String label, String query, List<String> signals) {
    for (String signal : signals) {
      assertTrue(query.contains(signal), label + " missing [" + signal + "]\n" + clip(query));
    }
  }

  private static void reject(String label, String query, List<String> signals) {
    for (String signal : signals) {
      assertFalse(query.contains(signal), label + " still has [" + signal + "]\n" + clip(query));
    }
  }

  private static String clip(String query) {
    return query.length() > 1200 ? query.substring(0, 1200) : query;
  }

  private static List<Case> cases() {
    String quoted =
        "The exact template is a bool of a name prefix, an exact name, and URN terms. "
            + "Java V2.5 keeps that match under its dis_max root and still adds the analyzed simple query.";
    String structured =
        "Java keeps the V2.5 dis_max root around the structured fragment. "
            + "The structured template places that fragment in a bool should with the exact name and URN terms.";
    String light =
        "The light query drops fuzzy expansions on both paths. "
            + "Its exact name term is boost 100. Java keeps the constant 1000.";
    String browse =
        "Browse text uses the search router. An empty query resolves to listUrn without ranking. "
            + "Browse path, depth, and group aggregation stay in Java, so neither body names browsePathV2.";
    String lineage =
        "A lineage page uses the searchEntities seed. A short query starts on light. "
            + "The graph walk's URN filter stays outside the template. "
            + "The template's exact name term is boost 100. Java keeps the constant 1000.";
    String dotted =
        "A spaceless dotted query scores qualifiedName through the bounded path clause. "
            + "The template omits the analyzed qualifiedName boost of 50. Java still applies it.";
    String scroll = "Scroll renders fulltext. Interactive search starts on light.";
    return List.of(
        shared(
            "identity",
            "identity",
            URN,
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            false,
            false,
            "urn:li:searchTemplate:identity",
            URN,
            "qualifiedName",
            "\"id\""),
        different(
            "exact",
            "exact",
            "\"orders by region\"",
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            false,
            false,
            "urn:li:searchTemplate:exact",
            List.of("orders by region", "match_phrase_prefix", "case_insensitive"),
            List.of("urn.lowercase"),
            List.of("dis_max"),
            List.of("dis_max"),
            List.of(),
            quoted),
        different(
            "dotted",
            "dotted",
            "my_db.analytics.orders",
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            true,
            false,
            SEARCH_TEMPLATE_FULLTEXT_URN,
            List.of("qualifiedName"),
            List.of(),
            List.of("50.0"),
            List.of("50.0"),
            List.of(),
            dotted),
        shared(
            "alphanumeric",
            "alphanumeric",
            "orders2017",
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            true,
            false,
            SEARCH_TEMPLATE_FULLTEXT_URN,
            "orders",
            "2017"),
        shared(
            "typo",
            "typo",
            "custmers",
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            true,
            false,
            SEARCH_TEMPLATE_FULLTEXT_URN,
            "custmers~"),
        shared(
            "long",
            "long",
            LONG_QUERY,
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            true,
            false,
            SEARCH_TEMPLATE_FULLTEXT_URN,
            "zorbel"),
        shared(
            "joined",
            "joined",
            "load_job-0001",
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            true,
            false,
            SEARCH_TEMPLATE_FULLTEXT_URN,
            "load_job"),
        different(
            "structured",
            "structured",
            STRUCTURED_QUERY_PREFIX + "name:orders",
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            false,
            true,
            "urn:li:searchTemplate:structured",
            List.of("case_insensitive", "orders"),
            List.of(),
            List.of("dis_max"),
            List.of(),
            List.of(),
            structured),
        shared(
            "structured property",
            "structuredProperty",
            "customer orders",
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            true,
            false,
            SEARCH_TEMPLATE_FULLTEXT_URN,
            CERTIFICATION),
        different(
            "lineage",
            "lineage",
            "customer orders",
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            false,
            true,
            "urn:li:searchTemplate:light",
            List.of("multi_match"),
            List.of(),
            List.of("1000"),
            List.of("1000"),
            List.of("~"),
            lineage),
        different(
            "scroll",
            "scroll",
            "customer orders",
            SearchTemplateSurfaces.SCROLL_ENTITIES,
            true,
            false,
            SEARCH_TEMPLATE_FULLTEXT_URN,
            List.of("simple_query_string", "dis_max"),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            scroll),
        different(
            "light",
            "light",
            "customer orders",
            SearchTemplateSurfaces.SEARCH_ENTITIES,
            false,
            true,
            "urn:li:searchTemplate:light",
            List.of("multi_match"),
            List.of(),
            List.of("1000"),
            List.of("1000"),
            List.of("~"),
            light),
        different(
            "browse",
            "browse",
            "*",
            null,
            false,
            false,
            "urn:li:searchTemplate:listUrn",
            List.of("match_all"),
            List.of(),
            List.of(),
            List.of("browsePathV2"),
            List.of("browsePathV2"),
            browse));
  }

  private static Case shared(
      String name,
      String category,
      String query,
      @Nullable String surface,
      boolean scroll,
      boolean light,
      String template,
      String... signals) {
    return new Case(
        name,
        category,
        query,
        surface,
        true,
        scroll,
        light,
        template,
        List.of(signals),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        "");
  }

  private static Case different(
      String name,
      String category,
      String query,
      @Nullable String surface,
      boolean scroll,
      boolean light,
      String template,
      List<String> shared,
      List<String> templateKeeps,
      List<String> javaKeeps,
      List<String> templateLacks,
      List<String> javaLacks,
      String note) {
    return new Case(
        name,
        category,
        query,
        surface,
        true,
        scroll,
        light,
        template,
        shared,
        templateKeeps,
        javaKeeps,
        templateLacks,
        javaLacks,
        note);
  }

  private record Case(
      String name,
      String category,
      String query,
      @Nullable String surface,
      boolean fulltext,
      boolean scroll,
      boolean light,
      String template,
      List<String> shared,
      List<String> templateKeeps,
      List<String> javaKeeps,
      List<String> templateLacks,
      List<String> javaLacks,
      String note) {}
}
