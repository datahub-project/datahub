package com.linkedin.metadata.graph.lineage.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.graph.inmemory.ImpactLineageSearchPushdown;
import com.linkedin.metadata.graph.inmemory.LightningSearch;
import com.linkedin.metadata.graph.lineage.core.EdgeFilter;
import com.linkedin.metadata.graph.lineage.core.GraphSnapshot;
import com.linkedin.metadata.graph.lineage.core.ImpactWalker;
import com.linkedin.metadata.graph.lineage.core.WalkRequest;
import com.linkedin.metadata.graph.lineage.core.WalkResult;
import com.linkedin.metadata.graph.lineage.core.WireCodec;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;

/**
 * HTTP API on the JDK's built-in server with virtual threads. Walks are CPU-bound and take
 * milliseconds, so a small semaphore bounds how many run at once, encoding included, which also
 * bounds the walk scratch space held; further requests wait briefly, and slow clients only park
 * cheap virtual threads. Each request holds the snapshot it started on until its response is
 * written, so a reload never unmaps memory a response is still reading.
 *
 * <ul>
 *   <li>{@code GET /health}: the process is up.
 *   <li>{@code GET /ready}: 200 once a snapshot is loaded, 503 before; the snapshot's build time
 *       and age.
 *   <li>{@code GET /stats}: memory per segment, reload history and timings.
 *   <li>{@code POST /v1/impact}: a walk; binary {@code DHLG} by default, {@code ?format=json} for a
 *       summary, {@code &full=true} to include every relationship with its paths.
 *   <li>{@code POST /v1/impact/search}: a walk plus GMS's lightning lineage search over it, binary
 *       {@code DHLS}: the page and facet counts, or the full walk when the lightning path does not
 *       apply. 422 when a urn cannot be read the way GMS reads it, so GMS searches itself.
 * </ul>
 */
@Slf4j
final class HttpApi {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpServer server;
  private final LineageGraphServer app;
  private final Semaphore walks;
  private volatile LightningSearch.NodeFacets nodeFacets;

  HttpApi(int port, LineageGraphServer app, int walkConcurrency) throws IOException {
    this.app = app;
    this.walks = new Semaphore(Math.max(1, walkConcurrency), true);
    server = HttpServer.create(new InetSocketAddress(port), 128);
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.createContext("/health", ex -> json(ex, 200, Map.of("status", "ok")));
    server.createContext("/ready", this::ready);
    server.createContext("/stats", ex -> json(ex, 200, app.stats()));
    server.createContext("/v1/impact", this::impact);
    server.createContext("/v1/impact/search", this::search);
  }

  void start() {
    server.start();
  }

  void stop() {
    server.stop(1);
  }

  private void ready(HttpExchange ex) throws IOException {
    Map<String, Object> r = app.readiness();
    json(ex, LineageGraphServer.State.READY.name().equals(r.get("state")) ? 200 : 503, r);
  }

  private void impact(HttpExchange ex) throws IOException {
    try {
      if (!"POST".equals(ex.getRequestMethod())) {
        json(ex, 405, Map.of("error", "POST only"));
        return;
      }
      JsonNode body = MAPPER.readTree(ex.getRequestBody());
      WalkRequest req = request(body);
      Map<String, String> query = query(ex.getRequestURI().getRawQuery());
      boolean jsonOut = "json".equals(query.get("format"));
      boolean full = "true".equals(query.get("full"));

      walks.acquireUninterruptibly();
      GraphSnapshot snapshot = app.acquire();
      try {
        if (snapshot == null) {
          json(ex, 503, Map.of("error", "no snapshot loaded", "state", app.state().name()));
          return;
        }
        ImpactWalker walker = app.walker();
        long t0 = System.nanoTime();
        try (WalkResult result =
            walker.walk(snapshot, req, EdgeFilter.compile(snapshot, req.rules()))) {
          long walkMicros = (System.nanoTime() - t0) / 1000;
          long t1 = System.nanoTime();
          if (jsonOut) {
            ObjectNode out = summary(result, walkMicros);
            if (full) {
              out.set("relationships", relationships(result));
            }
            out.put("encodeMicros", (System.nanoTime() - t1) / 1000);
            json(ex, 200, out);
          } else {
            ex.getResponseHeaders().set("Content-Type", "application/x-datahub-lineage-graph");
            ex.getResponseHeaders().set("X-Walk-Micros", Long.toString(walkMicros));
            // Summary headers let a client (or the bench) read the outcome without decoding.
            ex.getResponseHeaders().set("X-Total", Integer.toString(result.size()));
            ex.getResponseHeaders().set("X-Hops", Integer.toString(result.hops()));
            ex.getResponseHeaders().set("X-Status", result.status().name());
            ex.getResponseHeaders().set("X-Partial", Boolean.toString(result.partial()));
            ex.sendResponseHeaders(200, 0); // chunked
            try (OutputStream os = ex.getResponseBody()) {
              WireCodec.encode(result, walkMicros, 0, os);
            }
          }
        }
      } finally {
        if (snapshot != null) {
          snapshot.release();
        }
        walks.release();
      }
    } catch (IllegalArgumentException e) {
      json(ex, 400, Map.of("error", String.valueOf(e.getMessage())));
    } catch (Exception e) {
      log.error("Walk failed", e);
      json(ex, 500, Map.of("error", String.valueOf(e)));
    } finally {
      ex.close();
    }
  }

  private void search(HttpExchange ex) throws IOException {
    try {
      if (!"POST".equals(ex.getRequestMethod())) {
        json(ex, 405, Map.of("error", "POST only"));
        return;
      }
      JsonNode body = MAPPER.readTree(ex.getRequestBody());
      WalkRequest req = request(body);
      ImpactLineageSearchPushdown.Spec spec = LightningSearch.specFromJson(body.path("search"));

      walks.acquireUninterruptibly();
      GraphSnapshot snapshot = app.acquire();
      try {
        if (snapshot == null) {
          json(ex, 503, Map.of("error", "no snapshot loaded", "state", app.state().name()));
          return;
        }
        long t0 = System.nanoTime();
        try (WalkResult result =
            app.walker().walk(snapshot, req, EdgeFilter.compile(snapshot, req.rules()))) {
          long t1 = System.nanoTime();
          long walkMicros = (t1 - t0) / 1000;
          LightningSearch.NodeFacets facets =
              LightningSearch.NodeFacets.forSnapshot(snapshot, nodeFacets);
          nodeFacets = facets;
          LightningSearch.Page page = LightningSearch.search(result, spec, facets);
          long searchMicros = (System.nanoTime() - t1) / 1000;
          ex.getResponseHeaders().set("Content-Type", "application/x-datahub-lineage-search");
          ex.getResponseHeaders().set("X-Walk-Micros", Long.toString(walkMicros));
          ex.getResponseHeaders().set("X-Search-Micros", Long.toString(searchMicros));
          ex.getResponseHeaders().set("X-Walked", Integer.toString(result.size()));
          ex.getResponseHeaders().set("X-Prefiltered", Integer.toString(page.prefiltered()));
          ex.getResponseHeaders().set("X-Total", Integer.toString(page.total()));
          ex.getResponseHeaders().set("X-Hops", Integer.toString(result.hops()));
          ex.getResponseHeaders().set("X-Status", result.status().name());
          ex.getResponseHeaders().set("X-Partial", Boolean.toString(result.partial()));
          ex.getResponseHeaders().set("X-Mode", page.lightning() ? "LIGHTNING" : "FULL");
          ex.sendResponseHeaders(200, 0); // chunked
          try (OutputStream os = ex.getResponseBody()) {
            LightningSearch.encode(result, page, walkMicros, searchMicros, 0, os);
          }
        }
      } finally {
        if (snapshot != null) {
          snapshot.release();
        }
        walks.release();
      }
    } catch (LightningSearch.UnservableException e) {
      log.warn("Lightning search not servable: {}", e.toString());
      json(ex, 422, Map.of("error", String.valueOf(e.getMessage())));
    } catch (IllegalArgumentException e) {
      json(ex, 400, Map.of("error", String.valueOf(e.getMessage())));
    } catch (Exception e) {
      log.error("Search failed", e);
      json(ex, 500, Map.of("error", String.valueOf(e)));
    } finally {
      ex.close();
    }
  }

  private static Map<String, String> query(String raw) {
    Map<String, String> out = new HashMap<>();
    if (raw == null || raw.isEmpty()) {
      return out;
    }
    for (String pair : raw.split("&")) {
      int eq = pair.indexOf('=');
      String k = eq < 0 ? pair : pair.substring(0, eq);
      String v = eq < 0 ? "" : pair.substring(eq + 1);
      out.put(
          URLDecoder.decode(k, StandardCharsets.UTF_8),
          URLDecoder.decode(v, StandardCharsets.UTF_8));
    }
    return out;
  }

  static WalkRequest request(JsonNode b) {
    if (!b.hasNonNull("urn")) {
      throw new IllegalArgumentException("urn is required");
    }
    List<EdgeFilter.Rule> rules = new ArrayList<>();
    for (JsonNode r : b.path("rules")) {
      rules.add(
          new EdgeFilter.Rule(
              r.path("entityType").asText(),
              r.path("relationshipType").asText(),
              EdgeFilter.Direction.valueOf(r.path("direction").asText()),
              r.path("opposingType").asText()));
    }
    return new WalkRequest(
        b.get("urn").asText(),
        rules,
        b.path("maxHops").asInt(1000),
        b.path("maxRelations").asInt(0),
        b.path("partialResults").asBoolean(true),
        b.hasNonNull("startTimeMillis") ? b.get("startTimeMillis").asLong() : null,
        b.hasNonNull("endTimeMillis") ? b.get("endTimeMillis").asLong() : null,
        b.path("timeoutMillis").asLong(0));
  }

  private ObjectNode summary(WalkResult r, long walkMicros) {
    ObjectNode out = MAPPER.createObjectNode();
    out.put("status", r.status().name());
    out.put("partial", r.partial());
    out.put("total", r.size());
    out.put("hops", r.hops());
    out.put("edgesScanned", r.edgesScanned());
    out.put("walkMicros", walkMicros);
    out.put("snapshotBuiltAt", r.graph().builtAtMillis());
    return out;
  }

  private static ArrayNode relationships(WalkResult r) {
    ArrayNode arr = MAPPER.createArrayNode();
    GraphSnapshot g = r.graph();
    for (int i = 0; i < r.size(); i++) {
      ObjectNode o = arr.addObject();
      o.put("urn", r.urn(i));
      o.put("type", r.relationshipType(i));
      o.put("degree", r.degree(i));
      ArrayNode degrees = o.putArray("degrees");
      for (int d : r.degrees(i)) {
        degrees.add(d);
      }
      o.put("explored", r.explored(i));
      ArrayNode paths = o.putArray("paths");
      for (int[] p : r.paths(i)) {
        ArrayNode path = paths.addArray();
        for (int n : p) {
          path.add(g.urn(n));
        }
      }
    }
    return arr;
  }

  private static void json(HttpExchange ex, int status, Object body) throws IOException {
    byte[] bytes = MAPPER.writeValueAsBytes(body);
    ex.getResponseHeaders().set("Content-Type", "application/json");
    ex.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(bytes);
    }
  }
}
