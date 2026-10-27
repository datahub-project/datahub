package com.linkedin.metadata.graph.inmemory;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.graph.InMemoryGraphConfiguration;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.graph.LineageTimeoutException;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.elastic.LineageBenchLog;
import com.linkedin.metadata.graph.lineage.core.ImpactResponse;
import com.linkedin.metadata.graph.lineage.core.ImpactSearchResponse;
import com.linkedin.metadata.graph.lineage.core.WalkResult;
import com.linkedin.metadata.graph.lineage.core.WireCodec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.LineageFlags;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.query.filter.SortCriterion;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Serves impact lineage from the in-memory lineage graph server and delegates everything else to
 * the wrapped (OpenSearch) graph service.
 *
 * <p>A walk goes to the server only while a background health poll says it is READY and within
 * {@code maxLagMs} of the change log, and while the circuit is closed. Connection errors, timeouts
 * and 5xx responses count toward opening the circuit, and that walk falls back to the delegate. A
 * walk that hits {@code maxRelations} or the time budget in strict mode fails the same way the
 * OpenSearch walk would; it does not fall back.
 */
@Slf4j
public class InMemoryLineageGraphService implements GraphService, ImpactLineageSearchPushdown {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private record Health(String state, long lagMillis, long checkedAt) {}

  private final GraphService delegate;
  private final InMemoryGraphConfiguration config;
  private final GraphQueryConfiguration graphQueryConfig;
  private final HttpClient http;
  private volatile Health health = new Health("UNKNOWN", -1, 0);
  private final AtomicInteger consecutiveFailures = new AtomicInteger();
  private volatile long circuitOpenUntil;

  public InMemoryLineageGraphService(
      GraphService delegate,
      InMemoryGraphConfiguration config,
      GraphQueryConfiguration graphQueryConfig) {
    this.delegate = delegate;
    this.config = config;
    this.graphQueryConfig = graphQueryConfig;
    this.http =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(config.getConnectTimeoutMs()))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    Thread.ofVirtual().name("lineage-graph-health").start(this::pollHealth);
    log.info("Impact lineage walks go to the in-memory lineage graph at {}", config.getUrl());
  }

  // ------------------------------------------------------------------ impact lineage

  @Nonnull
  @Override
  public EntityLineageResult getImpactLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters graphFilters,
      int maxHops) {
    String unavailable = unavailableReason();
    if (unavailable != null) {
      LineageBenchLog.inMemoryFallback(entityUrn, unavailable);
      return delegate.getImpactLineage(opContext, entityUrn, graphFilters, maxHops);
    }
    long t0 = System.nanoTime();
    byte[] body;
    long t1;
    ImpactResponse resp;
    try {
      body = call("/v1/impact", walkRequest(opContext, entityUrn, graphFilters, maxHops));
      t1 = System.nanoTime();
      resp = WireCodec.decode(body);
      consecutiveFailures.set(0);
    } catch (IOException | RuntimeException e) {
      recordFailure(e);
      LineageBenchLog.inMemoryFallback(entityUrn, "error:" + e.getClass().getSimpleName());
      return delegate.getImpactLineage(opContext, entityUrn, graphFilters, maxHops);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return delegate.getImpactLineage(opContext, entityUrn, graphFilters, maxHops);
    }
    long t2 = System.nanoTime();
    EntityLineageResult result = toResult(entityUrn, graphFilters, maxHops, resp);
    long t3 = System.nanoTime();

    Map<String, Object> extra = new LinkedHashMap<>();
    extra.put("backend", "inmem");
    extra.put("server_walk_ms", resp.walkMicros() / 1000.0);
    extra.put("http_ms", (t1 - t0) / 1_000_000.0);
    extra.put("decode_ms", (t2 - t1) / 1_000_000.0);
    extra.put("build_ms", (t3 - t2) / 1_000_000.0);
    extra.put("bytes", body.length);
    extra.put("lag_ms", resp.lagMillis());
    extra.put("watermark", resp.watermark());
    LineageBenchLog.inMemoryWalk(
        entityUrn,
        resp.hops(),
        resp.relationships().size(),
        resp.partial(),
        "ok",
        (t3 - t0) / 1_000_000,
        extra);
    return result;
  }

  // ------------------------------------------------------------------ lightning search pushdown

  @Nonnull
  @Override
  public Optional<Outcome> searchImpactLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters graphFilters,
      int maxHops,
      @Nonnull Spec spec) {
    // When the server cannot be used, getImpactLineage falls back (and logs why) right after this.
    if (!config.isSearchPushdown() || unavailableReason() != null) {
      return Optional.empty();
    }
    long t0 = System.nanoTime();
    byte[] body;
    long t1;
    ImpactSearchResponse resp;
    try {
      ObjectNode req = walkRequest(opContext, entityUrn, graphFilters, maxHops);
      ObjectNode search = req.putObject("search");
      ArrayNode types = search.putArray("entityTypes");
      spec.entityTypes().forEach(types::add);
      ArrayNode degrees = search.putArray("degreeValues");
      spec.degreeValues().forEach(degrees::add);
      if (spec.filters() != null) {
        search.put("filters", RecordUtils.toJsonString(spec.filters()));
      }
      search.put("convertSchemaFields", spec.convertSchemaFields());
      search.put("forceLightning", spec.forceLightning());
      search.put("lightningThreshold", spec.lightningThreshold());
      search.put("from", spec.from());
      search.put("size", spec.size());
      body = call("/v1/impact/search", req);
      t1 = System.nanoTime();
      resp = WireCodec.decodeSearch(body);
      consecutiveFailures.set(0);
    } catch (UnservableSearchException e) {
      // A urn the server could not read the way GMS does; GMS searches the full walk itself.
      consecutiveFailures.set(0);
      LineageBenchLog.inMemoryFallback(entityUrn, "search_unservable");
      return Optional.empty();
    } catch (IOException | RuntimeException e) {
      recordFailure(e);
      LineageBenchLog.inMemoryFallback(entityUrn, "search_error:" + e.getClass().getSimpleName());
      return Optional.empty();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    }
    long t2 = System.nanoTime();

    Outcome outcome;
    int rows;
    if (resp.mode() == ImpactSearchResponse.Mode.FULL) {
      EntityLineageResult result = toResult(entityUrn, graphFilters, maxHops, resp.full());
      outcome = new Full(result);
      rows = result.getRelationships().size();
    } else {
      failIfStrictLimitHit(entityUrn, graphFilters, maxHops, resp.status(), resp.partial());
      Map<String, Urn> urns = new HashMap<>();
      List<Row> page = new ArrayList<>(resp.rows().size());
      for (ImpactSearchResponse.Row r : resp.rows()) {
        UrnArrayArray paths = toPaths(r.paths(), urns);
        page.add(new Row(urns.computeIfAbsent(r.urn(), UrnUtils::getUrn), r.degree(), paths));
      }
      outcome =
          new Lightning(
              page,
              resp.walked(),
              resp.prefiltered(),
              resp.total(),
              facet(resp.facets().get(0)),
              facet(resp.facets().get(1)),
              facet(resp.facets().get(2)),
              resp.partial());
      rows = page.size();
    }
    long t3 = System.nanoTime();

    Map<String, Object> extra = new LinkedHashMap<>();
    extra.put("backend", "inmem_pushdown");
    extra.put("mode", resp.mode().name());
    extra.put("server_walk_ms", resp.walkMicros() / 1000.0);
    extra.put("server_search_ms", resp.searchMicros() / 1000.0);
    extra.put("http_ms", (t1 - t0) / 1_000_000.0);
    extra.put("decode_ms", (t2 - t1) / 1_000_000.0);
    extra.put("build_ms", (t3 - t2) / 1_000_000.0);
    extra.put("bytes", body.length);
    extra.put("prefiltered", resp.prefiltered());
    extra.put("matched", resp.total());
    extra.put("rows", rows);
    extra.put("lag_ms", resp.lagMillis());
    extra.put("watermark", resp.watermark());
    LineageBenchLog.inMemoryWalk(
        entityUrn, resp.hops(), resp.walked(), resp.partial(), "ok", (t3 - t0) / 1_000_000, extra);
    return Optional.of(outcome);
  }

  private static Facet facet(ImpactSearchResponse.Facet f) {
    LinkedHashMap<String, Long> counts = new LinkedHashMap<>();
    for (int i = 0; i < f.values().size(); i++) {
      counts.put(f.values().get(i), f.counts()[i]);
    }
    return new Facet(counts, f.countedAfterLastNew());
  }

  /** The server could not run the search the way GMS would (HTTP 422). */
  private static final class UnservableSearchException extends IOException {
    UnservableSearchException(String message) {
      super(message);
    }
  }

  private ObjectNode walkRequest(
      OperationContext opContext, Urn entityUrn, LineageGraphFilters graphFilters, int maxHops) {
    ObjectNode req = MAPPER.createObjectNode();
    req.put("urn", entityUrn.toString());
    ArrayNode rules = req.putArray("rules");
    LineageRegistry lineageRegistry = delegate.getLineageRegistry();
    // The filters compute each entity type's edges lazily, as a walk meets that type; the server
    // needs all of them up front.
    for (EntitySpec spec : lineageRegistry.getEntityRegistry().getEntitySpecs().values()) {
      graphFilters.getEdgeInfo(lineageRegistry, spec.getName());
    }
    graphFilters
        .streamEdgeInfo()
        .forEach(
            p -> {
              RelationshipDirection d = p.getValue().getDirection();
              if (d == RelationshipDirection.OUTGOING || d == RelationshipDirection.INCOMING) {
                rules
                    .addObject()
                    .put("entityType", p.getKey())
                    .put("relationshipType", p.getValue().getType())
                    .put("direction", d.name())
                    .put("opposingType", p.getValue().getOpposingEntityType());
              }
            });
    req.put("maxHops", maxHops);
    int maxRelations = graphQueryConfig.getImpact().getMaxRelations();
    boolean partial = graphQueryConfig.getImpact().isPartialResults();
    req.put("maxRelations", maxRelations);
    req.put("partialResults", partial);
    req.put("timeoutMillis", walkBudgetMillis(partial));
    LineageFlags flags =
        opContext.getSearchContext() == null
            ? null
            : opContext.getSearchContext().getLineageFlags();
    if (flags != null && flags.getStartTimeMillis() != null && flags.getEndTimeMillis() != null) {
      req.put("startTimeMillis", flags.getStartTimeMillis());
      req.put("endTimeMillis", flags.getEndTimeMillis());
    }
    return req;
  }

  private byte[] call(String path, ObjectNode req) throws IOException, InterruptedException {
    HttpResponse<byte[]> resp =
        http.send(
            HttpRequest.newBuilder(URI.create(config.getUrl() + path))
                .timeout(Duration.ofMillis(config.getRequestTimeoutMs()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(MAPPER.writeValueAsBytes(req)))
                .build(),
            HttpResponse.BodyHandlers.ofByteArray());
    if (resp.statusCode() == 422) {
      throw new UnservableSearchException(new String(resp.body(), StandardCharsets.UTF_8));
    }
    if (resp.statusCode() != 200) {
      throw new IOException("lineage graph server returned " + resp.statusCode());
    }
    return resp.body();
  }

  /** The same walk budget as the OpenSearch walk: the timeout less the search-phase reservation. */
  private long walkBudgetMillis(boolean partial) {
    long total = graphQueryConfig.getTimeoutSeconds() * 1000L;
    if (!partial) {
      return total;
    }
    double reservation = graphQueryConfig.getImpact().getSearchQueryTimeReservation();
    if (reservation <= 0.0 || reservation >= 1.0) {
      reservation = 0.2;
    }
    long reserved = Math.max((long) (total * reservation), Math.min(100L, total));
    return total - reserved;
  }

  private EntityLineageResult toResult(
      Urn entityUrn, LineageGraphFilters graphFilters, int maxHops, ImpactResponse resp) {
    failIfStrictLimitHit(entityUrn, graphFilters, maxHops, resp.status(), resp.partial());
    // URNs repeat across thousands of paths; parse each once.
    Map<String, Urn> urns = new HashMap<>();
    List<LineageRelationship> rels = new ArrayList<>(resp.relationships().size());
    for (ImpactResponse.Relationship r : resp.relationships()) {
      UrnArrayArray paths = toPaths(r.paths(), urns);
      IntegerArray degrees = new IntegerArray(r.degrees().length);
      for (int d : r.degrees()) {
        degrees.add(d);
      }
      LineageRelationship rel =
          new LineageRelationship()
              .setType(r.type())
              .setEntity(urns.computeIfAbsent(r.urn(), UrnUtils::getUrn))
              .setDegree(r.degree())
              .setDegrees(degrees)
              .setPaths(paths)
              .setIsManual(r.manual())
              .setTruncatedChildren(false);
      if (r.createdOn() != null) {
        rel.setCreatedOn(r.createdOn());
      }
      if (r.createdActor() != null) {
        rel.setCreatedActor(urns.computeIfAbsent(r.createdActor(), UrnUtils::getUrn));
      }
      if (r.updatedOn() != null) {
        rel.setUpdatedOn(r.updatedOn());
      }
      if (r.updatedActor() != null) {
        rel.setUpdatedActor(urns.computeIfAbsent(r.updatedActor(), UrnUtils::getUrn));
      }
      if (r.explored()) {
        rel.setExplored(true); // the OpenSearch walk only ever sets it to true
      }
      rels.add(rel);
    }
    return new EntityLineageResult()
        .setRelationships(new LineageRelationshipArray(rels))
        .setStart(0)
        .setCount(rels.size())
        .setTotal(rels.size())
        .setPartial(resp.partial());
  }

  private static UrnArrayArray toPaths(List<List<String>> in, Map<String, Urn> urns) {
    UrnArrayArray paths = new UrnArrayArray(in.size());
    for (List<String> p : in) {
      UrnArray path = new UrnArray(p.size());
      for (String u : p) {
        path.add(urns.computeIfAbsent(u, UrnUtils::getUrn));
      }
      paths.add(path);
    }
    return paths;
  }

  /** In strict mode a walk cut short fails, as the OpenSearch walk does. */
  private void failIfStrictLimitHit(
      Urn entityUrn,
      LineageGraphFilters graphFilters,
      int maxHops,
      WalkResult.Status status,
      boolean partial) {
    if (!partial) {
      if (status == WalkResult.Status.MAX_RELATIONS) {
        throw new IllegalStateException(
            String.format(
                "Lineage results exceeded the configured maxRelations limit of %d. Entity: %s, Direction: %s, MaxHops: %d. Consider reducing maxHops or increasing the maxRelations limit, or set partialResults to true to return partial results.",
                graphQueryConfig.getImpact().getMaxRelations(),
                entityUrn,
                graphFilters.getLineageDirection(),
                maxHops));
      }
      if (status == WalkResult.Status.TIMEOUT) {
        throw new LineageTimeoutException(
            String.format(
                "Lineage operation timed out after %d seconds. Entity: %s, Direction: %s, MaxHops: %d. Consider increasing the timeout or set partialResults to true to return partial results.",
                graphQueryConfig.getTimeoutSeconds(),
                entityUrn,
                graphFilters.getLineageDirection(),
                maxHops));
      }
    }
  }

  // ------------------------------------------------------------------ health and circuit

  /** Why the server should not be used right now, or null if it can be. */
  @Nullable
  String unavailableReason() {
    long now = System.currentTimeMillis();
    if (now < circuitOpenUntil) {
      return "circuit_open";
    }
    Health h = health;
    if (now - h.checkedAt() > Math.max(3 * config.getHealthPollMs(), 3000)) {
      return "health_unknown";
    }
    if (!"READY".equals(h.state())) {
      return "not_ready:" + h.state();
    }
    if (h.lagMillis() > config.getMaxLagMs()) {
      return "lagging:" + h.lagMillis() + "ms";
    }
    return null;
  }

  private void recordFailure(Exception e) {
    int n = consecutiveFailures.incrementAndGet();
    log.warn(
        "In-memory lineage graph call failed ({} in a row), falling back: {}", n, e.toString());
    if (n >= config.getFailureThreshold()) {
      circuitOpenUntil = System.currentTimeMillis() + config.getOpenCircuitMs();
      consecutiveFailures.set(0);
      log.warn("In-memory lineage graph circuit open for {} ms", config.getOpenCircuitMs());
    }
  }

  private void pollHealth() {
    while (!Thread.currentThread().isInterrupted()) {
      Health next;
      try {
        HttpResponse<byte[]> resp =
            http.send(
                HttpRequest.newBuilder(URI.create(config.getUrl() + "/ready"))
                    .timeout(Duration.ofMillis(Math.max(500, config.getConnectTimeoutMs())))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofByteArray());
        JsonNode r = MAPPER.readTree(resp.body());
        next =
            new Health(
                r.path("state").asText("UNKNOWN"),
                r.path("lagMillis").asLong(-1),
                System.currentTimeMillis());
      } catch (InterruptedException e) {
        return;
      } catch (Exception e) {
        next = new Health("UNREACHABLE", -1, System.currentTimeMillis());
      }
      if (!next.state().equals(health.state())) {
        log.info("In-memory lineage graph state: {} -> {}", health.state(), next.state());
      }
      health = next;
      try {
        Thread.sleep(Math.max(100, config.getHealthPollMs()));
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  // ------------------------------------------------------------------ delegated

  @Override
  public GraphServiceConfiguration getGraphServiceConfig() {
    return delegate.getGraphServiceConfig();
  }

  @Override
  public LineageRegistry getLineageRegistry() {
    return delegate.getLineageRegistry();
  }

  @Override
  public void addEdge(@Nonnull OperationContext opContext, @Nonnull Edge edge) {
    delegate.addEdge(opContext, edge);
  }

  @Override
  public void upsertEdge(@Nonnull OperationContext opContext, @Nonnull Edge edge) {
    delegate.upsertEdge(opContext, edge);
  }

  @Override
  public void removeEdge(@Nonnull OperationContext opContext, @Nonnull Edge edge) {
    delegate.removeEdge(opContext, edge);
  }

  @Nonnull
  @Override
  public RelatedEntitiesResult findRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      int offset,
      @Nullable Integer count) {
    return delegate.findRelatedEntities(opContext, graphFilters, offset, count);
  }

  @Nonnull
  @Override
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters graphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    return delegate.getLineage(opContext, entityUrn, graphFilters, offset, count, maxHops);
  }

  @Override
  public void removeNode(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    delegate.removeNode(opContext, urn);
  }

  @Override
  public void removeEdgesFromNode(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter) {
    delegate.removeEdgesFromNode(opContext, urn, relationshipTypes, relationshipFilter);
  }

  @Override
  public void configure() {
    delegate.configure();
  }

  @Override
  public void clear(@Nonnull OperationContext opContext) {
    delegate.clear(opContext);
  }

  @Override
  public boolean supportsMultiHop() {
    return delegate.supportsMultiHop();
  }

  @Override
  public void setEdgeStatus(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      boolean removed,
      @Nonnull EdgeUrnType... edgeUrnTypes) {
    delegate.setEdgeStatus(opContext, urn, removed, edgeUrnTypes);
  }

  @Nonnull
  @Override
  public RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {
    return delegate.scrollRelatedEntities(
        opContext,
        graphFilters,
        sortCriteria,
        scrollId,
        keepAlive,
        count,
        startTimeMillis,
        endTimeMillis);
  }

  @Override
  public List<Map<String, Object>> raw(OperationContext opContext, List<EdgeTuple> edgeTuples) {
    return delegate.raw(opContext, edgeTuples);
  }
}
