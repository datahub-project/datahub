package com.linkedin.metadata.graph.inmemory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.MetadataChangeProposalConfig;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.cache.SearchCacheConfiguration;
import com.linkedin.metadata.config.cache.SearchLineageCacheConfiguration;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.graph.InMemoryGraphConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.config.search.ImpactConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchLineageConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.LimitConfig;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.lineage.core.EdgeFilter;
import com.linkedin.metadata.graph.lineage.core.GraphBuilder;
import com.linkedin.metadata.graph.lineage.core.GraphSnapshot;
import com.linkedin.metadata.graph.lineage.core.ImpactWalker;
import com.linkedin.metadata.graph.lineage.core.WalkRequest;
import com.linkedin.metadata.graph.lineage.core.WalkResult;
import com.linkedin.metadata.graph.lineage.core.WireCodec;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SearchService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The lightning search the graph server answers must be exactly what {@link LineageSearchService}
 * computes from the full walk. Runs the same random searches through two services over the same
 * fake server (the real walker and {@link LightningSearch}): one hands the search to the server,
 * the other fetches the full walk and searches it itself, and their results must be equal --
 * entities in order, paths, totals and every facet value in order.
 */
public class LineageSearchPushdownEquivalenceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final LineageRegistry LINEAGE =
      new LineageRegistry(TestOperationContexts.defaultEntityRegistry());
  // More platforms than a HashMap's first resize, so facet order depends on when the map grew.
  private static final String[] PLATFORMS = {
    "hive", "snowflake", "dbt", "kafka", "bigquery", "redshift", "postgres", "mysql", "s3",
    "looker", "tableau", "airflow", "spark", "glue", "oracle", "mssql", "trino", "databricks",
    "iceberg", "delta-lake"
  };
  private static final String[] ENVS = {"PROD", "DEV", "QA"};

  private HttpServer server;
  private GraphSnapshot snapshot;
  private final AtomicInteger searches = new AtomicInteger();
  private final AtomicInteger lightningAnswers = new AtomicInteger();
  private final AtomicInteger fullAnswers = new AtomicInteger();
  private final List<String> datasets = new ArrayList<>();
  private final List<String> schemaFields = new ArrayList<>();
  private LineageSearchService pushed;
  private LineageSearchService plain;
  private LightningSearch.NodeFacets facets;

  @BeforeClass
  public void setUp() throws Exception {
    snapshot = GraphSnapshot.of(randomGraph(new Random(7)).build());
    ImpactWalker walker = new ImpactWalker();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/ready",
        ex -> respond(ex, MAPPER.writeValueAsBytes(Map.of("state", "READY", "lagMillis", 0))));
    server.createContext(
        "/v1/impact",
        ex -> {
          JsonNode req = MAPPER.readTree(ex.getRequestBody());
          GraphSnapshot snap = snapshot;
          WalkRequest w = walkRequest(req);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          try (WalkResult r = walker.walk(snap, w, EdgeFilter.compile(snap, w.rules()))) {
            WireCodec.encode(r, 1, 0, out);
          }
          respond(ex, out.toByteArray());
        });
    server.createContext(
        "/v1/impact/search",
        ex -> {
          searches.incrementAndGet();
          JsonNode req = MAPPER.readTree(ex.getRequestBody());
          GraphSnapshot snap = snapshot;
          WalkRequest w = walkRequest(req);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          try (WalkResult r = walker.walk(snap, w, EdgeFilter.compile(snap, w.rules()))) {
            facets = LightningSearch.NodeFacets.forSnapshot(snap, facets);
            LightningSearch.Page page =
                LightningSearch.search(r, LightningSearch.specFromJson(req.get("search")), facets);
            (page.lightning() ? lightningAnswers : fullAnswers).incrementAndGet();
            LightningSearch.encode(r, page, 1, 1, 0, out);
          }
          respond(ex, out.toByteArray());
        });
    server.start();

    GraphService delegate = mock(GraphService.class);
    when(delegate.getLineageRegistry()).thenReturn(LINEAGE);
    when(delegate.getGraphServiceConfig())
        .thenReturn(
            GraphServiceConfiguration.builder()
                .limit(
                    LimitConfig.builder()
                        .results(ResultsLimitConfig.builder().apiDefault(7).max(50).build())
                        .build())
                .build());
    when(delegate.getImpactLineage(any(), any(), any(), anyInt()))
        .thenReturn(
            new EntityLineageResult()
                .setRelationships(new LineageRelationshipArray())
                .setStart(0)
                .setCount(0)
                .setTotal(0));

    SearchService searchService = mock(SearchService.class);
    SearchResult entityIndexAnswer =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setMetadata(new SearchResultMetadata())
            .setFrom(0)
            .setPageSize(0)
            .setNumEntities(0);
    when(searchService.searchAcrossEntities(any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(entityIndexAnswer);
    when(searchService.searchAcrossEntities(
            any(), any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(entityIndexAnswer);

    DataHubAppConfiguration appConfig = appConfig();
    pushed =
        new LineageSearchService(
            searchService, inMemory(delegate, appConfig, true), null, false, appConfig);
    plain =
        new LineageSearchService(
            searchService, inMemory(delegate, appConfig, false), null, false, appConfig);
  }

  @AfterClass
  public void tearDown() {
    server.stop(0);
  }

  @Test
  public void testPushedDownSearchesMatchSearchingTheFullWalk() {
    Random rnd = new Random(42);
    OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    int compared = 0;
    int withRows = 0;
    int manyPlatforms = 0;
    int fieldRows = 0;
    int failures = 0;
    for (int q = 0; q < 1500; q++) {
      boolean fromField = rnd.nextInt(3) == 0;
      String root =
          fromField
              ? schemaFields.get(rnd.nextInt(schemaFields.size()))
              : datasets.get(rnd.nextInt(datasets.size()));
      LineageDirection direction =
          rnd.nextBoolean() ? LineageDirection.DOWNSTREAM : LineageDirection.UPSTREAM;
      List<String> types = randomTypes(rnd);
      Filter filter = randomFilter(rnd);
      int from = new int[] {0, 0, 0, 3, 10, 40, -1}[rnd.nextInt(7)];
      Integer size = new Integer[] {10, 10, 1, 0, null, 50, 60}[rnd.nextInt(7)];
      OperationContext op = base;
      if (rnd.nextBoolean()) {
        op = op.withSearchFlags(f -> f.setGroupingSpec(noGrouping()));
      }
      if (rnd.nextInt(5) == 0) {
        op = op.withLineageFlags(f -> f.setForceLightningMode(true));
      }

      Object expected = run(plain, op, root, direction, types, filter, from, size);
      Object actual = run(pushed, op, root, direction, types, filter, from, size);
      String context =
          String.format(
              "root=%s dir=%s types=%s filter=%s from=%d size=%s",
              root, direction, types, filter, from, size);
      assertEquals(actual, expected, context);
      if (expected instanceof LineageSearchResult r) {
        assertEquals(
            RecordUtils.toJsonString((LineageSearchResult) actual),
            RecordUtils.toJsonString(r),
            context);
        withRows += r.getEntities().isEmpty() ? 0 : 1;
        fieldRows +=
            r.getEntities().stream()
                    .anyMatch(e -> e.getEntity().getEntityType().equals("schemaField"))
                ? 1
                : 0;
        manyPlatforms +=
            r.getMetadata().getAggregations().stream()
                    .anyMatch(
                        a -> a.getName().equals("platform") && a.getFilterValues().size() > 12)
                ? 1
                : 0;
      } else {
        failures++;
      }
      compared++;
    }
    assertEquals(compared, 1500);
    System.out.printf(
        "EQUIV rows=%d fields=%d platforms=%d failures=%d lightning=%d full=%d%n",
        withRows, fieldRows, manyPlatforms, failures, lightningAnswers.get(), fullAnswers.get());
    // The comparisons must exercise what can differ: pages, schema fields, facet order.
    assertTrue(withRows > 300, "searches with rows: " + withRows);
    assertTrue(fieldRows > 5, "searches returning schema fields: " + fieldRows);
    assertTrue(manyPlatforms > 100, "searches with more than 12 platforms: " + manyPlatforms);
    assertTrue(failures < 100, "searches that failed on both sides: " + failures);
    assertTrue(searches.get() > 0);
    assertTrue(lightningAnswers.get() > 300, "lightning answers: " + lightningAnswers.get());
    assertTrue(fullAnswers.get() > 100, "full answers: " + fullAnswers.get());
  }

  /** The result, or the exception's class and message, so failures must match too. */
  private static Object run(
      LineageSearchService service,
      OperationContext op,
      String root,
      LineageDirection direction,
      List<String> types,
      Filter filter,
      int from,
      Integer size) {
    try {
      return service.searchAcrossLineage(
          op,
          UrnUtils.getUrn(root),
          direction,
          types,
          "*",
          null,
          filter == null ? null : filter.copy(),
          null,
          from,
          size);
    } catch (Exception e) {
      return e.getClass().getName() + ": " + e.getMessage();
    }
  }

  // ------------------------------------------------------------------ fixtures

  private GraphBuilder randomGraph(Random rnd) {
    GraphBuilder b = new GraphBuilder(LightningNodeFacts.INSTANCE);
    for (int i = 0; i < 300; i++) {
      // Skewed so some platforms are common and others rare.
      String platform = PLATFORMS[(int) (PLATFORMS.length * Math.pow(rnd.nextDouble(), 2))];
      datasets.add(
          "urn:li:dataset:(urn:li:dataPlatform:"
              + platform
              + ",db.t"
              + i
              + ","
              + ENVS[rnd.nextInt(ENVS.length)]
              + ")");
    }
    for (int i = 1; i < datasets.size(); i++) {
      int parents = 1 + rnd.nextInt(3);
      for (int p = 0; p < parents; p++) {
        String parent = datasets.get(rnd.nextInt(i));
        edge(b, datasets.get(i), "DownstreamOf", parent);
        // Column lineage along every dataset edge, so field walks are as deep as dataset walks.
        String child = field(datasets.get(i), "c" + rnd.nextInt(2));
        String up = field(parent, "c" + rnd.nextInt(2));
        edge(b, child, "DownstreamOf", up);
        schemaFields.add(child);
        schemaFields.add(up);
      }
    }
    for (int i = 0; i < 40; i++) {
      String chart = "urn:li:chart:(looker,c" + i + ")";
      edge(b, chart, "Consumes", datasets.get(rnd.nextInt(datasets.size())));
      String dashboard = "urn:li:dashboard:(tableau,d" + i + ")";
      edge(b, dashboard, "Consumes", datasets.get(rnd.nextInt(datasets.size())));
      String job =
          "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow" + (i % 5) + ",PROD),job" + i + ")";
      edge(b, job, "Consumes", datasets.get(rnd.nextInt(datasets.size())));
      edge(b, job, "Produces", datasets.get(rnd.nextInt(datasets.size())));
    }
    return b;
  }

  private static String field(String dataset, String path) {
    return "urn:li:schemaField:(" + dataset + "," + path + ")";
  }

  private static void edge(GraphBuilder b, String source, String rel, String destination) {
    b.addEdge(source, rel, destination, null, null, null);
  }

  private static List<String> randomTypes(Random rnd) {
    switch (rnd.nextInt(6)) {
      case 0:
        return List.of();
      case 1:
        return List.of("dataset");
      case 2:
        return List.of("dataset", "chart", "dashboard", "dataJob");
      case 3:
        return List.of("schemaField", "dataset");
      case 4:
        return List.of("chart");
      default:
        return List.of("dataset", "chart", "dashboard", "dataJob", "schemaField", "dataFlow");
    }
  }

  private static Filter randomFilter(Random rnd) {
    List<ConjunctiveCriterion> or = new ArrayList<>();
    int branches = rnd.nextInt(4); // 0 means no filter at all
    for (int b = 0; b < branches; b++) {
      CriterionArray and = new CriterionArray();
      if (b == 0 && rnd.nextBoolean()) {
        and.add(degree(rnd));
      }
      int n = rnd.nextInt(3);
      for (int i = 0; i < n; i++) {
        and.add(randomCriterion(rnd));
      }
      if (b > 0 && rnd.nextInt(4) == 0) {
        and.add(degree(rnd)); // ignored by the degree filter, which reads only the first branch
      }
      or.add(new ConjunctiveCriterion().setAnd(and));
    }
    return branches == 0 ? null : new Filter().setOr(new ConjunctiveCriterionArray(or));
  }

  private static Criterion degree(Random rnd) {
    List<String> all = List.of("1", "2", "3+");
    List<String> values = new ArrayList<>();
    for (String v : all) {
      if (rnd.nextBoolean()) {
        values.add(v);
      }
    }
    return new Criterion().setField("degree").setValues(new StringArray(values));
  }

  private static Criterion randomCriterion(Random rnd) {
    Criterion c = new Criterion();
    switch (rnd.nextInt(3)) {
      case 0:
        c.setField("platform")
            .setValues(
                new StringArray(
                    List.of(
                        "urn:li:dataPlatform:" + PLATFORMS[rnd.nextInt(5)],
                        "urn:li:dataPlatform:" + PLATFORMS[rnd.nextInt(PLATFORMS.length)])));
        break;
      case 1:
        c.setField("origin").setValues(new StringArray(List.of(ENVS[rnd.nextInt(ENVS.length)])));
        break;
      default:
        c.setField("parent").setValues(new StringArray(List.of("db.t1", "urn:li:dataset")));
        c.setCondition(Condition.CONTAIN);
        break;
    }
    if (rnd.nextInt(4) == 0) {
      c.setCondition(Condition.CONTAIN);
      c.setValues(new StringArray(List.of(rnd.nextBoolean() ? "snow" : "PRO")));
    }
    if (rnd.nextInt(4) == 0) {
      c.setNegated(true);
    }
    if (rnd.nextInt(10) == 0) {
      c.setValues(new StringArray());
    }
    return c;
  }

  private static GroupingSpec noGrouping() {
    return new GroupingSpec().setGroupingCriteria(new GroupingCriterionArray());
  }

  private static WalkRequest walkRequest(JsonNode req) {
    List<EdgeFilter.Rule> rules = new ArrayList<>();
    req.get("rules")
        .forEach(
            r ->
                rules.add(
                    new EdgeFilter.Rule(
                        r.get("entityType").asText(),
                        r.get("relationshipType").asText(),
                        EdgeFilter.Direction.valueOf(r.get("direction").asText()),
                        r.get("opposingType").asText())));
    return new WalkRequest(
        req.get("urn").asText(),
        rules,
        req.get("maxHops").asInt(),
        req.get("maxRelations").asInt(),
        req.get("partialResults").asBoolean(),
        null,
        null,
        0);
  }

  private static void respond(HttpExchange ex, byte[] body) throws IOException {
    ex.sendResponseHeaders(200, body.length);
    ex.getResponseBody().write(body);
    ex.close();
  }

  private InMemoryLineageGraphService inMemory(
      GraphService delegate, DataHubAppConfiguration appConfig, boolean searchPushdown)
      throws InterruptedException {
    InMemoryGraphConfiguration config =
        InMemoryGraphConfiguration.builder()
            .enabled(true)
            .url("http://localhost:" + server.getAddress().getPort())
            .connectTimeoutMs(500)
            .requestTimeoutMs(5000)
            .maxLagMs(5000)
            .healthPollMs(50)
            .failureThreshold(1000)
            .openCircuitMs(60_000)
            .searchPushdown(searchPushdown)
            .build();
    InMemoryLineageGraphService svc =
        new InMemoryLineageGraphService(
            delegate, config, appConfig.getElasticSearch().getSearch().getGraph());
    for (int i = 0; i < 100 && svc.unavailableReason() != null; i++) {
      Thread.sleep(20);
    }
    return svc;
  }

  private static DataHubAppConfiguration appConfig() {
    DataHubAppConfiguration appConfig = new DataHubAppConfiguration();
    GraphQueryConfiguration graph =
        GraphQueryConfiguration.builder()
            .timeoutSeconds(50)
            .lineageMaxHops(10)
            .pointInTimeCreationEnabled(true)
            .impact(
                ImpactConfiguration.builder()
                    .maxHops(1000)
                    .maxRelations(0)
                    .partialResults(true)
                    .searchQueryTimeReservation(0.2)
                    .keepAlive("5m")
                    .build())
            .build();
    SearchConfiguration search = SearchConfiguration.builder().build();
    search.setGraph(graph);
    ElasticSearchConfiguration es = ElasticSearchConfiguration.builder().build();
    es.setSearch(search);
    appConfig.setElasticSearch(es);
    appConfig.setGraphService(
        GraphServiceConfiguration.builder()
            .limit(
                LimitConfig.builder()
                    .results(ResultsLimitConfig.builder().apiDefault(7).max(50).build())
                    .build())
            .build());
    // Low, so most searches over this small graph take the lightning path and some do not.
    appConfig.setCache(
        CacheConfiguration.builder()
            .search(
                SearchCacheConfiguration.builder()
                    .lineage(
                        SearchLineageCacheConfiguration.builder()
                            .ttlSeconds(600L)
                            .lightningThreshold(5L)
                            .build())
                    .build())
            .build());
    appConfig.setSearchService(
        SearchServiceConfiguration.builder()
            .lineage(SearchLineageConfiguration.builder().maxParentsToValidate(5).build())
            .build());
    appConfig.setMetadataChangeProposal(
        MetadataChangeProposalConfig.builder()
            .sideEffects(
                MetadataChangeProposalConfig.SideEffectsConfig.builder()
                    .schemaField(
                        MetadataChangeProposalConfig.SchemaFieldSideEffectsConfig.builder()
                            .enabled(false)
                            .build())
                    .build())
            .build());
    return appConfig;
  }
}
