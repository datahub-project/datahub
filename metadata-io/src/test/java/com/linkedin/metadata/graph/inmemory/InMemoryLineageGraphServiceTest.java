package com.linkedin.metadata.graph.inmemory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.graph.InMemoryGraphConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.config.search.ImpactConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.lineage.core.EdgeFilter;
import com.linkedin.metadata.graph.lineage.core.GraphBuilder;
import com.linkedin.metadata.graph.lineage.core.GraphSnapshot;
import com.linkedin.metadata.graph.lineage.core.ImpactWalker;
import com.linkedin.metadata.graph.lineage.core.WalkRequest;
import com.linkedin.metadata.graph.lineage.core.WalkResult;
import com.linkedin.metadata.graph.lineage.core.WireCodec;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.sun.net.httpserver.HttpServer;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InMemoryLineageGraphServiceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final OperationContext OP = TestOperationContexts.systemContextNoValidate();
  private static final LineageRegistry LINEAGE =
      new LineageRegistry(TestOperationContexts.defaultEntityRegistry());

  private HttpServer server;
  private final AtomicReference<String> state = new AtomicReference<>("READY");
  private final AtomicReference<Integer> impactStatus = new AtomicReference<>(200);
  // "ok", an HTTP status to fail with, or "garbage" for a 200 that does not decode.
  private final AtomicReference<String> searchAnswer = new AtomicReference<>("ok");
  private GraphSnapshot snapshot;
  private GraphService delegate;

  private static String ds(String name) {
    return "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)";
  }

  @BeforeMethod
  public void setUp() throws Exception {
    state.set("READY");
    impactStatus.set(200);
    searchAnswer.set("ok");
    // a <- b <- c, and a <- d: d and b are downstream of a, c of b.
    GraphBuilder b = new GraphBuilder(LightningNodeFacts.INSTANCE);
    b.addEdge(ds("b"), "DownstreamOf", ds("a"), null, 10L, null);
    b.addEdge(ds("c"), "DownstreamOf", ds("b"), null, null, null);
    b.addEdge(ds("d"), "DownstreamOf", ds("a"), null, null, null);
    snapshot = GraphSnapshot.of(b.build());
    ImpactWalker walker = new ImpactWalker();

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/ready",
        ex -> {
          byte[] body = MAPPER.writeValueAsBytes(Map.of("state", state.get(), "lagMillis", 0));
          ex.sendResponseHeaders(200, body.length);
          ex.getResponseBody().write(body);
          ex.close();
        });
    server.createContext(
        "/v1/impact",
        ex -> {
          if (impactStatus.get() != 200) {
            ex.sendResponseHeaders(impactStatus.get(), -1);
            ex.close();
            return;
          }
          JsonNode req = MAPPER.readTree(ex.getRequestBody());
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
          GraphSnapshot snap = snapshot;
          WalkRequest w =
              new WalkRequest(
                  req.get("urn").asText(),
                  rules,
                  req.get("maxHops").asInt(),
                  req.get("maxRelations").asInt(),
                  req.get("partialResults").asBoolean(),
                  null,
                  null,
                  0);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          try (WalkResult r = walker.walk(snap, w, EdgeFilter.compile(snap, rules))) {
            WireCodec.encode(r, 1, 0, out);
          }
          ex.sendResponseHeaders(200, out.size());
          ex.getResponseBody().write(out.toByteArray());
          ex.close();
        });
    server.createContext(
        "/v1/impact/search",
        ex -> {
          String answer = searchAnswer.get();
          if (!answer.equals("ok") && !answer.equals("garbage")) {
            ex.sendResponseHeaders(Integer.parseInt(answer), -1);
            ex.close();
            return;
          }
          JsonNode req = MAPPER.readTree(ex.getRequestBody());
          GraphSnapshot snap = snapshot;
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
          WalkRequest w =
              new WalkRequest(
                  req.get("urn").asText(),
                  rules,
                  req.get("maxHops").asInt(),
                  req.get("maxRelations").asInt(),
                  req.get("partialResults").asBoolean(),
                  null,
                  null,
                  0);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          if (answer.equals("garbage")) {
            out.write(new byte[] {'D', 'H', 'L', 'S', 9});
          } else {
            try (WalkResult r = walker.walk(snap, w, EdgeFilter.compile(snap, rules))) {
              LightningSearch.Page page =
                  LightningSearch.search(
                      r,
                      LightningSearch.specFromJson(req.get("search")),
                      LightningSearch.NodeFacets.forSnapshot(snap, null));
              LightningSearch.encode(r, page, 1, 1, 0, out);
            }
          }
          ex.sendResponseHeaders(200, out.size());
          ex.getResponseBody().write(out.toByteArray());
          ex.close();
        });
    server.start();

    delegate = mock(GraphService.class);
    when(delegate.getLineageRegistry()).thenReturn(LINEAGE);
    when(delegate.getImpactLineage(any(), any(), any(), anyInt()))
        .thenReturn(
            new EntityLineageResult()
                .setRelationships(new LineageRelationshipArray())
                .setStart(0)
                .setCount(0)
                .setTotal(0));
  }

  @AfterMethod
  public void tearDown() {
    server.stop(0);
  }

  private InMemoryLineageGraphService service(int maxRelations, boolean partial) throws Exception {
    return service(maxRelations, partial, true);
  }

  private InMemoryLineageGraphService service(
      int maxRelations, boolean partial, boolean searchPushdown) throws Exception {
    InMemoryGraphConfiguration config =
        InMemoryGraphConfiguration.builder()
            .enabled(true)
            .url("http://localhost:" + server.getAddress().getPort())
            .connectTimeoutMs(500)
            .requestTimeoutMs(5000)
            .maxLagMs(5000)
            .healthPollMs(50)
            .failureThreshold(2)
            .openCircuitMs(60_000)
            .searchPushdown(searchPushdown)
            .build();
    GraphQueryConfiguration graph =
        GraphQueryConfiguration.builder()
            .timeoutSeconds(50)
            .impact(
                ImpactConfiguration.builder()
                    .maxHops(1000)
                    .maxRelations(maxRelations)
                    .partialResults(partial)
                    .searchQueryTimeReservation(0.2)
                    .build())
            .build();
    InMemoryLineageGraphService svc = new InMemoryLineageGraphService(delegate, config, graph);
    awaitReason(svc, null);
    return svc;
  }

  private static void awaitReason(InMemoryLineageGraphService svc, String prefix)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      String r = svc.unavailableReason();
      if (prefix == null ? r == null : r != null && r.startsWith(prefix)) {
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("reason never became " + prefix + ", is " + svc.unavailableReason());
  }

  private static LineageGraphFilters downstream() {
    return LineageGraphFilters.forEntityType(LINEAGE, "dataset", LineageDirection.DOWNSTREAM);
  }

  @Test
  public void testServesImpactLineageFromServer() throws Exception {
    InMemoryLineageGraphService svc = service(0, true);
    EntityLineageResult result =
        svc.getImpactLineage(OP, UrnUtils.getUrn(ds("a")), downstream(), 10);

    verify(delegate, never()).getImpactLineage(any(), any(), any(), anyInt());
    assertEquals(result.getTotal().intValue(), 3);
    Map<String, LineageRelationship> byUrn = new HashMap<>();
    result.getRelationships().forEach(r -> byUrn.put(r.getEntity().toString(), r));
    LineageRelationship b = byUrn.get(ds("b"));
    assertEquals(b.getType(), "DownstreamOf");
    assertEquals(b.getDegree().intValue(), 1);
    // The service keeps no edge timestamps, actors or manual flag (looked up per page later).
    assertNull(b.getCreatedOn());
    assertNull(b.getCreatedActor());
    assertTrue(b.isExplored());
    LineageRelationship c = byUrn.get(ds("c"));
    assertEquals(c.getDegree().intValue(), 2);
    assertFalse(c.isIsManual());
    assertEquals(
        c.getPaths().get(0),
        new com.linkedin.common.UrnArray(
            List.of(UrnUtils.getUrn(ds("a")), UrnUtils.getUrn(ds("b")), UrnUtils.getUrn(ds("c")))));
    // A third hop ran (and found nothing), so c is explored too, as in the OpenSearch walk.
    assertTrue(c.isExplored());
  }

  @Test
  public void testFallsBackWhenNotReady() throws Exception {
    InMemoryLineageGraphService svc = service(0, true);
    state.set("CATCHING_UP");
    awaitReason(svc, "not_ready");
    Urn a = UrnUtils.getUrn(ds("a"));
    svc.getImpactLineage(OP, a, downstream(), 10);
    verify(delegate, times(1)).getImpactLineage(eq(OP), eq(a), any(), eq(10));
  }

  @Test
  public void testFallsBackOnErrorsAndOpensTheCircuit() throws Exception {
    InMemoryLineageGraphService svc = service(0, true);
    impactStatus.set(500);
    Urn a = UrnUtils.getUrn(ds("a"));
    svc.getImpactLineage(OP, a, downstream(), 10);
    svc.getImpactLineage(OP, a, downstream(), 10);
    verify(delegate, times(2)).getImpactLineage(any(), any(), any(), anyInt());
    assertEquals(svc.unavailableReason(), "circuit_open");
    // While the circuit is open the server is not called at all, even once it recovers.
    impactStatus.set(200);
    svc.getImpactLineage(OP, a, downstream(), 10);
    verify(delegate, times(3)).getImpactLineage(any(), any(), any(), anyInt());
  }

  @Test
  public void testStrictLimitFailsLikeOpenSearch() throws Exception {
    InMemoryLineageGraphService svc = service(1, false);
    assertThrows(
        IllegalStateException.class,
        () -> svc.getImpactLineage(OP, UrnUtils.getUrn(ds("a")), downstream(), 10));
    verify(delegate, never()).getImpactLineage(any(), any(), any(), anyInt());
  }

  private static ImpactLineageSearchPushdown.Spec spec(boolean force) {
    return new ImpactLineageSearchPushdown.Spec(Set.of(), List.of(), null, true, force, 300, 0, 10);
  }

  @Test
  public void testSearchPushdownReturnsThePageOrTheFullWalk() throws Exception {
    InMemoryLineageGraphService svc = service(0, true);
    Urn a = UrnUtils.getUrn(ds("a"));

    ImpactLineageSearchPushdown.Outcome forced =
        svc.searchImpactLineage(OP, a, downstream(), 10, spec(true)).orElseThrow();
    ImpactLineageSearchPushdown.Lightning page = (ImpactLineageSearchPushdown.Lightning) forced;
    assertEquals(page.total(), 3);
    assertEquals(page.rows().size(), 3);
    assertEquals(page.platforms().counts(), Map.of("urn:li:dataPlatform:hive", 3L));

    // Three relationships are below the lightning threshold: the full walk comes back.
    ImpactLineageSearchPushdown.Outcome small =
        svc.searchImpactLineage(OP, a, downstream(), 10, spec(false)).orElseThrow();
    assertEquals(((ImpactLineageSearchPushdown.Full) small).result().getRelationships().size(), 3);
    verify(delegate, never()).getImpactLineage(any(), any(), any(), anyInt());
  }

  @Test
  public void testSearchPushdownStepsAsideWhenTheServerCannotAnswer() throws Exception {
    Urn a = UrnUtils.getUrn(ds("a"));
    assertTrue(
        service(0, true, false).searchImpactLineage(OP, a, downstream(), 10, spec(true)).isEmpty());

    InMemoryLineageGraphService svc = service(0, true);
    // A urn the server cannot read as GMS does: not a server failure.
    searchAnswer.set("422");
    assertTrue(svc.searchImpactLineage(OP, a, downstream(), 10, spec(true)).isEmpty());
    searchAnswer.set("422");
    assertTrue(svc.searchImpactLineage(OP, a, downstream(), 10, spec(true)).isEmpty());
    assertEquals(svc.unavailableReason(), null);

    // A response that does not decode counts as a failure, like an error status.
    searchAnswer.set("garbage");
    assertTrue(svc.searchImpactLineage(OP, a, downstream(), 10, spec(true)).isEmpty());
    searchAnswer.set("500");
    assertTrue(svc.searchImpactLineage(OP, a, downstream(), 10, spec(true)).isEmpty());
    assertEquals(svc.unavailableReason(), "circuit_open");
    verify(delegate, never()).getImpactLineage(any(), any(), any(), anyInt());
  }

  @Test
  public void testSearchPushdownStrictLimitFailsLikeOpenSearch() throws Exception {
    InMemoryLineageGraphService svc = service(1, false);
    assertThrows(
        IllegalStateException.class,
        () -> svc.searchImpactLineage(OP, UrnUtils.getUrn(ds("a")), downstream(), 10, spec(true)));
  }
}
