package com.linkedin.metadata.graph.lineage.server;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.graph.lineage.core.GraphBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Reads every lineage edge document from the OpenSearch graph index for a snapshot build.
 *
 * <p>Uses a sliced scroll. A scroll reads the index as it was at the initial search, so documents
 * written while the scan runs don't produce a half-old, half-new graph: it is the scan's point in
 * time. (OpenSearch's point-in-time API would also work, but it has no {@code _shard_doc} tiebreak
 * for {@code search_after}, so paging it across shards needs a unique sort field the graph index
 * doesn't have.) One thread per slice fetches and parses pages (with streaming JSON, so a 10k-hit
 * page is never a tree in memory), and a single thread feeds the {@link GraphBuilder}, which is not
 * thread safe. Only relationship types that lineage walks follow are read, since {@code
 * graph_service} also holds ownership, tags, containment and other relationships, and only the
 * fields the graph keeps: timestamps are read only to order rows.
 */
@Slf4j
final class OpenSearchGraphScanner {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory JSON = MAPPER.getFactory();
  private static final List<String> FIELDS =
      List.of("source.urn", "destination.urn", "relationshipType", "via", "createdOn", "updatedOn");

  /** One parsed edge document. */
  private record Row(
      String source, String destination, String type, String via, Long createdOn, Long updatedOn) {}

  /**
   * @param pointInTimeMillis when the scan started: the graph reflects writes up to here
   */
  record Stats(long docs, long pages, long ms, long pointInTimeMillis) {}

  private final ServerConfig config;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  OpenSearchGraphScanner(ServerConfig config) {
    this.config = config;
  }

  Stats scan(Collection<String> relationshipTypes, GraphBuilder builder) throws Exception {
    long t0 = System.nanoTime();
    long pointInTime = System.currentTimeMillis();
    int slices = Math.max(1, config.scanSlices());
    BlockingQueue<List<Row>> pages = new ArrayBlockingQueue<>(slices * 4);
    List<Row> done = new ArrayList<>(0); // sentinel, compared by identity
    AtomicLong pageCount = new AtomicLong();
    ExecutorService pool = Executors.newFixedThreadPool(slices);
    List<Future<?>> futures = new ArrayList<>();
    long docs = 0;
    try {
      for (int i = 0; i < slices; i++) {
        int slice = i;
        futures.add(
            pool.submit(
                () -> {
                  try {
                    scanSlice(slice, slices, relationshipTypes, pages, pageCount);
                  } finally {
                    pages.put(done);
                  }
                  return null;
                }));
      }
      pool.shutdown();
      int finished = 0;
      while (finished < slices) {
        List<Row> page = pages.take();
        if (page == done) {
          finished++;
          continue;
        }
        for (Row r : page) {
          builder.addEdge(
              r.source(), r.type(), r.destination(), r.via(), r.createdOn(), r.updatedOn());
        }
        docs += page.size();
      }
      for (Future<?> f : futures) {
        f.get(); // rethrow a slice failure
      }
    } finally {
      pool.shutdownNow();
    }
    long ms = (System.nanoTime() - t0) / 1_000_000;
    log.info("Scanned {} edge documents in {} pages, {} ms", docs, pageCount.get(), ms);
    return new Stats(docs, pageCount.get(), ms, pointInTime);
  }

  private void scanSlice(
      int slice,
      int slices,
      Collection<String> relationshipTypes,
      BlockingQueue<List<Row>> out,
      AtomicLong pageCount)
      throws Exception {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("size", config.scanPageSize());
    body.putArray("sort").add("_doc");
    FIELDS.forEach(body.putArray("_source")::add);
    body.putObject("query")
        .putObject("terms")
        .putPOJO("relationshipType", List.copyOf(relationshipTypes));
    if (slices > 1) {
      body.putObject("slice").put("id", slice).put("max", slices);
    }
    String scrollId = null;
    try {
      Page page =
          post(config.openSearchUrl() + "/" + config.graphIndex() + "/_search?scroll=5m", body);
      while (true) {
        scrollId = page.scrollId;
        if (page.rows.isEmpty()) {
          break;
        }
        pageCount.incrementAndGet();
        out.put(page.rows);
        ObjectNode next = MAPPER.createObjectNode().put("scroll", "5m").put("scroll_id", scrollId);
        page = post(config.openSearchUrl() + "/_search/scroll", next);
      }
    } finally {
      if (scrollId != null) {
        ObjectNode clear = MAPPER.createObjectNode();
        clear.putArray("scroll_id").add(scrollId);
        http.send(
            HttpRequest.newBuilder(URI.create(config.openSearchUrl() + "/_search/scroll"))
                .header("Content-Type", "application/json")
                .method("DELETE", HttpRequest.BodyPublishers.ofString(clear.toString()))
                .build(),
            HttpResponse.BodyHandlers.discarding());
      }
    }
  }

  private record Page(String scrollId, List<Row> rows) {}

  private Page post(String url, ObjectNode body) throws IOException, InterruptedException {
    HttpResponse<InputStream> resp =
        http.send(
            HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMinutes(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build(),
            HttpResponse.BodyHandlers.ofInputStream());
    try (InputStream in = resp.body()) {
      if (resp.statusCode() != 200) {
        throw new IOException(
            "OpenSearch "
                + url
                + " returned "
                + resp.statusCode()
                + ": "
                + new String(in.readAllBytes()));
      }
      return parse(in);
    }
  }

  /** Streams a search response, keeping only {@code _scroll_id} and each hit's {@code _source}. */
  static Page parse(InputStream in) throws IOException {
    String scrollId = null;
    List<Row> rows = new ArrayList<>();
    try (JsonParser p = JSON.createParser(in)) {
      p.nextToken(); // START_OBJECT
      while (p.nextToken() == JsonToken.FIELD_NAME) {
        String field = p.currentName();
        p.nextToken();
        if ("_scroll_id".equals(field)) {
          scrollId = p.getText();
        } else if ("hits".equals(field)) {
          while (p.nextToken() == JsonToken.FIELD_NAME) {
            String hf = p.currentName();
            p.nextToken();
            if ("hits".equals(hf)) {
              while (p.nextToken() == JsonToken.START_OBJECT) {
                Row r = parseHit(p);
                if (r != null) {
                  rows.add(r);
                }
              }
            } else {
              p.skipChildren();
            }
          }
        } else {
          p.skipChildren();
        }
      }
    }
    return new Page(scrollId, rows);
  }

  private static Row parseHit(JsonParser p) throws IOException {
    Row row = null;
    while (p.nextToken() == JsonToken.FIELD_NAME) {
      String field = p.currentName();
      p.nextToken();
      if ("_source".equals(field)) {
        row = parseSource(p);
      } else {
        p.skipChildren();
      }
    }
    return row;
  }

  private static Row parseSource(JsonParser p) throws IOException {
    String source = null;
    String destination = null;
    String type = null;
    String via = null;
    Long createdOn = null;
    Long updatedOn = null;
    while (p.nextToken() == JsonToken.FIELD_NAME) {
      String field = p.currentName();
      JsonToken t = p.nextToken();
      if (t == JsonToken.VALUE_NULL) {
        continue;
      }
      switch (field) {
        case "source" -> source = nestedText(p, "urn");
        case "destination" -> destination = nestedText(p, "urn");
        case "relationshipType" -> type = p.getText();
        case "via" -> via = p.getText();
        case "createdOn" -> createdOn = p.getLongValue();
        case "updatedOn" -> updatedOn = p.getLongValue();
        default -> p.skipChildren();
      }
    }
    if (source == null || destination == null || type == null) {
      return null; // the OpenSearch walk skips edges with a missing URN too
    }
    return new Row(source, destination, type, via, createdOn, updatedOn);
  }

  /** Reads one text field of a nested object and skips the rest. */
  private static String nestedText(JsonParser p, String name) throws IOException {
    if (p.currentToken() != JsonToken.START_OBJECT) {
      p.skipChildren();
      return null;
    }
    String value = null;
    while (p.nextToken() == JsonToken.FIELD_NAME) {
      String f = p.currentName();
      p.nextToken();
      if (name.equals(f) && p.currentToken() != JsonToken.VALUE_NULL) {
        value = p.getText();
      } else {
        p.skipChildren();
      }
    }
    return value;
  }
}
