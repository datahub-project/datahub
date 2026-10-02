package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.OpenSearchShimBridge;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.testng.annotations.Test;

/**
 * Performance smoke test for {@link BulkTelemetry} on the bulk write path.
 *
 * <p>Not a benchmark: it drives the real per-action ({@code onAdd}) and per-flush ({@code
 * beforeBulk}, {@code requestOptions}, {@code makeCurrent}, {@code afterBulk}, {@code onRequeue})
 * calls at realistic and high batch sizes, with the flags off and on, prints a compact table, and
 * asserts only against bounds generous enough never to flake in CI. Its job is to catch a
 * pathological regression such as a loop that becomes quadratic in the batch size, not to measure
 * tens of nanoseconds. The numbers it prints are what the "Cost, measured" note in {@code
 * docs/advanced/monitoring.md} is based on.
 *
 * <p>Single-threaded on purpose: the adding thread and the flushing thread are the same here, so
 * the {@code synchronized} check-and-put in {@code remember} is uncontended and shows up only as
 * its fixed cost.
 */
public class BulkTelemetryPerfSmokeTest {

  /** Batch sizes: a typical consumer batch, the default bulk limit range, and the pending cap. */
  private static final int[] SIZES = {100, 1_000, 5_000, 20_000};

  /** A new trace id every this many actions, so link dedupe (and the 64-link cap) is exercised. */
  private static final int ACTIONS_PER_TRACE = 25;

  /** One action in this many fails and is requeued. */
  private static final int FAIL_EVERY = 100;

  /** Measured rounds per size: more rounds for small batches so each cell sees ~200k actions. */
  private static int measuredRounds(int size) {
    return Math.max(10, 200_000 / size);
  }

  private static int warmupRounds(int size) {
    return Math.max(3, measuredRounds(size) / 5);
  }

  // Bounds. The disabled path is a null check and a return (single-digit ns), so its ratio to the
  // enabled path is naturally two orders of magnitude; comparing against it directly would make the
  // ratio bound meaningless or flaky. The enabled path is therefore bounded three ways: an absolute
  // per-action ceiling, a ratio against the disabled cost floored at a realistic unit of work, and
  // a scaling check (20k vs 1k) that is what actually catches a superlinear loop.
  private static final double DISABLED_MAX_NS_PER_ACTION = 2_000; // 2 us; measured ~5 ns
  private static final double ENABLED_MAX_NS_PER_ACTION = 20_000; // 20 us; measured ~0.2-0.5 us
  private static final double RATIO_FLOOR_NS = 100; // disabled cost is floored here for the ratio
  private static final double ENABLED_MAX_RATIO = 30;
  private static final double MAX_SCALING_20K_OVER_1K =
      8; // measured 2-3x (cache effects); quadratic would be 20x+

  enum Mode {
    DISABLED("off"),
    SPANS("spans"),
    SPANS_AND_HEADER("spans+header");

    final String label;

    Mode(String label) {
      this.label = label;
    }

    BulkTelemetry create(BulkTelemetryTest.Collector collector) {
      switch (this) {
        case DISABLED:
          return BulkTelemetry.disabled();
        case SPANS:
          return BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, false, "perf");
        default:
          return BulkTelemetryTest.create(BulkTelemetryTest.tracer(collector), true, true, "perf");
      }
    }
  }

  /** One measured cell: medians over the measured rounds. */
  private static final class Cell {
    final Mode mode;
    final int size;
    final double addNsPerAction;
    final double flushMicros;
    final double totalNsPerAction;
    final double bytesPerAction; // NaN when allocation accounting is unavailable

    Cell(
        Mode mode,
        int size,
        double addNsPerAction,
        double flushMicros,
        double totalNsPerAction,
        double bytesPerAction) {
      this.mode = mode;
      this.size = size;
      this.addNsPerAction = addNsPerAction;
      this.flushMicros = flushMicros;
      this.totalNsPerAction = totalNsPerAction;
      this.bytesPerAction = bytesPerAction;
    }
  }

  /** Sink so the JIT cannot drop the request-options and scope work. */
  private static volatile long sink;

  // --- per-action and per-flush cost of the telemetry calls themselves ---

  @Test
  public void perActionAndPerFlushCostOffAndOn() {
    // Global warm-up so that the first enabled cell does not pay for compiling the enabled path.
    for (Mode mode : Mode.values()) {
      measure(mode, 1_000);
    }
    Map<Mode, Map<Integer, Cell>> table = new EnumMap<>(Mode.class);
    for (Mode mode : Mode.values()) {
      Map<Integer, Cell> row = new java.util.TreeMap<>();
      for (int size : SIZES) {
        row.put(size, measure(mode, size));
      }
      table.put(mode, row);
    }
    print(table);

    for (int size : SIZES) {
      Cell off = table.get(Mode.DISABLED).get(size);
      assertTrue(
          off.totalNsPerAction < DISABLED_MAX_NS_PER_ACTION,
          "disabled cost at " + size + " actions: " + off.totalNsPerAction + " ns/action");
      double floor = Math.max(off.totalNsPerAction, RATIO_FLOOR_NS);
      for (Mode mode : new Mode[] {Mode.SPANS, Mode.SPANS_AND_HEADER}) {
        Cell on = table.get(mode).get(size);
        assertTrue(
            on.totalNsPerAction < ENABLED_MAX_NS_PER_ACTION,
            mode.label + " cost at " + size + " actions: " + on.totalNsPerAction + " ns/action");
        assertTrue(
            on.totalNsPerAction < ENABLED_MAX_RATIO * floor,
            mode.label
                + " at "
                + size
                + " actions is "
                + (on.totalNsPerAction / floor)
                + "x the (floored) disabled cost");
      }
    }
    for (Mode mode : new Mode[] {Mode.SPANS, Mode.SPANS_AND_HEADER}) {
      double at1k = table.get(mode).get(1_000).totalNsPerAction;
      double at20k = table.get(mode).get(20_000).totalNsPerAction;
      assertTrue(
          at20k < MAX_SCALING_20K_OVER_1K * Math.max(at1k, RATIO_FLOOR_NS),
          mode.label
              + " per-action cost grows with batch size: "
              + at1k
              + " ns at 1k vs "
              + at20k
              + " ns at 20k");
    }
  }

  private static Cell measure(Mode mode, int size) {
    BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
    BulkTelemetry telemetry = mode.create(collector);

    // Built once per cell so that only the telemetry calls are measured, not request construction.
    BulkRequest request = new BulkRequest();
    for (int i = 0; i < size; i++) {
      request.add(new IndexRequest("idx" + (i % 4)).id(Integer.toString(i)).source("{}", 0));
    }
    List<DocWriteRequest<?>> actions = request.requests();
    Span[] origins = origins(Math.max(1, size / ACTIONS_PER_TRACE));
    List<Object> failed = new ArrayList<>();
    for (int i = FAIL_EVERY - 1; i < size; i += FAIL_EVERY) {
      failed.add(actions.get(i));
    }

    for (int r = 0; r < warmupRounds(size); r++) {
      round(telemetry, request, actions, origins, failed);
    }
    int rounds = measuredRounds(size);
    long[] add = new long[rounds];
    long[] flush = new long[rounds];
    long[] bytes = new long[rounds];
    boolean alloc = allocationSupported();
    for (int r = 0; r < rounds; r++) {
      long b0 = alloc ? allocatedBytes() : 0;
      long[] t = round(telemetry, request, actions, origins, failed);
      bytes[r] = alloc ? allocatedBytes() - b0 : -1;
      add[r] = t[0];
      flush[r] = t[1];
    }
    assertEquals(telemetry.openBatches(), 0, "every batch ended");
    assertEquals(telemetry.carriedCount(), 0, "every failed origin requeued");
    if (mode != Mode.DISABLED) {
      assertEquals(collector.spans.size(), warmupRounds(size) + rounds, "one batch span per flush");
    }
    double addNs = median(add) / (double) size;
    double flushNs = median(flush);
    return new Cell(
        mode,
        size,
        addNs,
        flushNs / 1_000.0,
        (median(add) + flushNs) / size,
        alloc ? median(bytes) / (double) size : Double.NaN);
  }

  /**
   * One add-then-flush cycle. Returns {add nanos, flush nanos}: the adds are what the request (or
   * consumer) threads pay per action; the flush side is what the bulk processor's thread pays once
   * per batch, and it is linear in the batch size by design.
   */
  private static long[] round(
      BulkTelemetry telemetry,
      BulkRequest request,
      List<DocWriteRequest<?>> actions,
      Span[] origins,
      List<Object> failed) {
    int size = actions.size();
    long a0 = System.nanoTime();
    Scope scope = origins[0].makeCurrent();
    try {
      for (int i = 0; i < size; i++) {
        if (i % ACTIONS_PER_TRACE == 0 && i > 0) {
          scope.close();
          scope = origins[(i / ACTIONS_PER_TRACE) % origins.length].makeCurrent();
        }
        telemetry.onAdd(actions.get(i));
      }
    } finally {
      scope.close();
    }
    long a1 = System.nanoTime();

    telemetry.beforeBulk(request, actions);
    RequestOptions options = telemetry.requestOptions(request, RequestOptions.DEFAULT);
    try (Scope ignored = telemetry.makeCurrent(request)) {
      sink += options.getHeaders().size(); // the store call would happen here
    }
    telemetry.afterBulk(request, 7L, failed);
    for (Object action : failed) {
      telemetry.onRequeue(action);
    }
    long a2 = System.nanoTime();
    return new long[] {a1 - a0, a2 - a1};
  }

  private static Span[] origins(int distinctTraces) {
    Span[] spans = new Span[distinctTraces];
    for (int i = 0; i < distinctTraces; i++) {
      spans[i] =
          BulkTelemetryTest.remoteSpan(
              String.format("%032x", 0x1000L + i), String.format("%016x", 0x2000L + i));
    }
    return spans;
  }

  private static void print(Map<Mode, Map<Integer, Cell>> table) {
    StringBuilder out = new StringBuilder();
    out.append("\nBulkTelemetry cost (medians; single thread; ")
        .append(allocationSupported() ? "alloc via ThreadMXBean)" : "alloc n/a)")
        .append('\n');
    out.append(
        String.format(
            "%-13s %7s %12s %12s %12s %10s%n",
            "mode", "actions", "add ns/act", "flush us", "total ns/act", "bytes/act"));
    for (Mode mode : Mode.values()) {
      for (Cell c : table.get(mode).values()) {
        out.append(
            String.format(
                "%-13s %7d %12.1f %12.1f %12.1f %10s%n",
                c.mode.label,
                c.size,
                c.addNsPerAction,
                c.flushMicros,
                c.totalNsPerAction,
                Double.isNaN(c.bytesPerAction) ? "n/a" : String.format("%.0f", c.bytesPerAction)));
      }
    }
    System.out.print(out);
  }

  // --- a real BulkProcessor flush through BulkListener and a mocked RestClient ---

  private static final int FLUSH_ACTIONS = 5_000;

  @Test
  public void realBulkProcessorFlushOffAndOn() throws Exception {
    String body = cannedBulkBody(FLUSH_ACTIONS);
    StringBuilder out = new StringBuilder("\nBulkProcessor flush of ");
    out.append(FLUSH_ACTIONS).append(" actions through BulkListener and a mocked RestClient\n");
    out.append(String.format("%-13s %12s %12s %8s%n", "mode", "add ms", "flush ms", "spans"));

    Mode[] modes = {Mode.DISABLED, Mode.SPANS_AND_HEADER};
    // Warm-up flushes for both modes first (serialization and parsing dominate and need the JIT),
    // then interleaved measured flushes; the best measured flush per mode is reported.
    for (int attempt = 0; attempt < 3; attempt++) {
      for (Mode mode : modes) {
        flushOnce(mode.create(new BulkTelemetryTest.Collector()), body);
      }
    }
    Map<Mode, double[]> timings = new EnumMap<>(Mode.class);
    Map<Mode, Integer> spans = new EnumMap<>(Mode.class);
    for (int attempt = 0; attempt < 5; attempt++) {
      for (Mode mode : modes) {
        BulkTelemetryTest.Collector collector = new BulkTelemetryTest.Collector();
        double[] t = flushOnce(mode.create(collector), body);
        spans.put(mode, collector.spans.size());
        double[] best = timings.get(mode);
        if (best == null || t[1] < best[1]) {
          timings.put(mode, t);
        }
      }
    }
    for (Mode mode : modes) {
      double[] best = timings.get(mode);
      out.append(
          String.format(
              "%-13s %12.2f %12.2f %8d%n", mode.label, best[0], best[1], spans.get(mode)));
      assertEquals(
          spans.get(mode).intValue(), mode == Mode.DISABLED ? 0 : 1, "one batch span when on");
    }
    System.out.print(out);

    double off = timings.get(Mode.DISABLED)[1];
    double on = timings.get(Mode.SPANS_AND_HEADER)[1];
    // Serialization and parsing of 5k items dominate both; telemetry adds tens of microseconds.
    assertTrue(
        on < Math.max(off * ENABLED_MAX_RATIO, off + 250.0),
        "flush off " + off + " ms, on " + on + " ms");
  }

  /** Returns {add millis, flush millis}. */
  private static double[] flushOnce(BulkTelemetry telemetry, String body) throws Exception {
    RestClient restClient = mock(RestClient.class);
    when(restClient.performRequest(any(Request.class)))
        .thenAnswer(invocation -> jsonResponse(200, body));
    // What OpenSearchSearchClientShim.executeBulkSync does, without the shim's package-private
    // test factory: header on the request, batch span current around the store call.
    BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer =
        (request, listener) -> {
          try (Scope ignored = telemetry.makeCurrent(request)) {
            Request low = OpenSearchShimBridge.bulk(request);
            low.setOptions(telemetry.requestOptions(request, RequestOptions.DEFAULT));
            Response response = restClient.performRequest(low);
            try (XContentParser parser =
                JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    response.getEntity().getContent())) {
              listener.onResponse(BulkResponse.fromXContent(parser));
            }
          } catch (Exception e) {
            listener.onFailure(e);
          }
        };
    BulkListener listener = BulkListener.create(null, null, null, null, telemetry);
    BulkProcessor processor =
        BulkProcessor.builder(consumer, listener)
            .setBulkActions(-1)
            .setBulkSize(new ByteSizeValue(-1))
            .setBackoffPolicy(BackoffPolicy.noBackoff())
            .build();
    try {
      Span[] origins = origins(FLUSH_ACTIONS / ACTIONS_PER_TRACE);
      long a0 = System.nanoTime();
      for (int i = 0; i < FLUSH_ACTIONS; i++) {
        IndexRequest action = new IndexRequest("idx").id(Integer.toString(i)).source("{}", 0);
        try (Scope ignored = origins[(i / ACTIONS_PER_TRACE) % origins.length].makeCurrent()) {
          telemetry.onAdd(action);
          processor.add(action);
        }
      }
      long a1 = System.nanoTime();
      processor.flush();
      long a2 = System.nanoTime();
      assertEquals(telemetry.openBatches(), 0);
      assertEquals(telemetry.pendingCount(), 0);
      return new double[] {(a1 - a0) / 1e6, (a2 - a1) / 1e6};
    } finally {
      processor.close();
    }
  }

  private static String cannedBulkBody(int items) {
    StringBuilder sb = new StringBuilder(items * 160);
    sb.append("{\"took\":5,\"errors\":false,\"items\":[");
    for (int i = 0; i < items; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"index\":{\"_index\":\"idx\",\"_id\":\"")
          .append(i)
          .append("\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":1,")
          .append("\"successful\":1,\"failed\":0},\"_seq_no\":")
          .append(i)
          .append(",\"_primary_term\":1,\"status\":201}}");
    }
    return sb.append("]}").toString();
  }

  private static Response jsonResponse(int statusCode, String json) {
    Response response = mock(Response.class);
    StatusLine statusLine =
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, null);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(response.getEntity()).thenReturn(new StringEntity(json, ContentType.APPLICATION_JSON));
    return response;
  }

  // --- helpers ---

  private static boolean allocationSupported() {
    java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    if (!(bean instanceof com.sun.management.ThreadMXBean)) {
      return false;
    }
    com.sun.management.ThreadMXBean sun = (com.sun.management.ThreadMXBean) bean;
    try {
      return sun.isThreadAllocatedMemorySupported() && sun.isThreadAllocatedMemoryEnabled();
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }

  private static long allocatedBytes() {
    return ((com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean())
        .getCurrentThreadAllocatedBytes();
  }

  private static long median(long[] values) {
    long[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }
}
