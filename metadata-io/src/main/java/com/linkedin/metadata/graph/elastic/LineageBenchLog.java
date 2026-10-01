package com.linkedin.metadata.graph.elastic;

import com.linkedin.common.urn.Urn;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Structured per-request / per-walk / per-hop / per-slice / per-esQuery log lines for the lineage
 * benchmark (perf-test/lineage-bench). Off unless GMS runs with {@code -Dlineage.bench.log=true}.
 *
 * <p>The walk context lives in a ThreadLocal on the walk's calling thread. Slice work runs on
 * pit-worker threads, so callers capture {@link #current()} before handing work to the pool and
 * pass it along explicitly. After the walk ends its id stays in {@link #lastWalkId()} on the same
 * thread, so the search service and GraphQL resolver that called the walk can tag their lines.
 */
@Slf4j(topic = "LINEAGE_BENCH")
public final class LineageBenchLog {

  public static final boolean ENABLED = Boolean.getBoolean("lineage.bench.log");

  private static final ThreadLocal<Walk> CURRENT = new ThreadLocal<>();
  private static final ThreadLocal<String> LAST_WALK_ID = new ThreadLocal<>();

  private LineageBenchLog() {}

  /** Mutable timing counters for one walk; hop counters are reset by {@link #hop}. */
  public static final class Walk {
    final String id = UUID.randomUUID().toString();
    final String startUrn;
    final long startNanos = System.nanoTime();
    final AtomicLong pitCreateMs = new AtomicLong();
    final AtomicLong pitDeleteMs = new AtomicLong();
    final AtomicLong slicesMs = new AtomicLong();
    final AtomicLong postprocessMs = new AtomicLong();
    final AtomicLong sliceMaxMs = new AtomicLong();
    final AtomicLong sliceMinMs = new AtomicLong(Long.MAX_VALUE);

    Walk(String startUrn) {
      this.startUrn = startUrn;
    }

    void recordSlice(long ms) {
      sliceMaxMs.accumulateAndGet(ms, Math::max);
      sliceMinMs.accumulateAndGet(ms, Math::min);
    }

    void resetHop() {
      pitCreateMs.set(0);
      pitDeleteMs.set(0);
      slicesMs.set(0);
      postprocessMs.set(0);
      sliceMaxMs.set(0);
      sliceMinMs.set(Long.MAX_VALUE);
    }
  }

  /**
   * Per-slice accumulators (one slice runs on one thread). Times are kept in nanoseconds because
   * per-page request building and hit extraction are often well under a millisecond.
   */
  public static final class Slice {
    final long submitNanos;
    long startNanos;
    int pages;
    long buildNanos;
    long esqueryNanos;
    long extractNanos;
    String endReason = "error";

    Slice(long submitNanos) {
      this.submitNanos = submitNanos;
    }
  }

  @Nullable
  static Walk start(Urn startUrn) {
    if (!ENABLED) {
      return null;
    }
    Walk walk = new Walk(startUrn.toString());
    CURRENT.set(walk);
    return walk;
  }

  @Nullable
  static Walk current() {
    return ENABLED ? CURRENT.get() : null;
  }

  /** Id of the last walk that finished on this thread, or null. */
  @Nullable
  public static String lastWalkId() {
    return ENABLED ? LAST_WALK_ID.get() : null;
  }

  /** Forget the last walk, so a request that does not walk (e.g. a cache hit) is not mis-tagged. */
  public static void clearLastWalk() {
    if (ENABLED) {
      LAST_WALK_ID.remove();
    }
  }

  public static long msSince(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000L;
  }

  static double ms(long nanos) {
    return Math.round(nanos / 1_000.0) / 1_000.0;
  }

  static void esquery(
      @Nullable Walk walk,
      int hop,
      int slice,
      int page,
      int urns,
      int hits,
      long tookMs,
      long clientMs,
      boolean timedOut,
      int shardsFailed,
      long buildNanos,
      long extractNanos) {
    if (walk == null) {
      return;
    }
    log.info(
        "LINEAGE_BENCH {\"t\":\"esquery\",\"walk_id\":\"{}\",\"hop\":{},\"slice\":{},\"page\":{},"
            + "\"urns\":{},\"hits\":{},\"took_ms\":{},\"client_ms\":{},\"timed_out\":{},"
            + "\"shards_failed\":{},\"build_ms\":{},\"extract_ms\":{}}",
        walk.id,
        hop,
        slice,
        page,
        urns,
        hits,
        tookMs,
        clientMs,
        timedOut,
        shardsFailed,
        ms(buildNanos),
        ms(extractNanos));
  }

  static void slice(
      @Nullable Walk walk, int hop, int slice, @Nullable Slice s, int relsKept, long endNanos) {
    if (walk == null || s == null) {
      return;
    }
    long total = endNanos - s.startNanos;
    long other = total - s.buildNanos - s.esqueryNanos - s.extractNanos;
    log.info(
        "LINEAGE_BENCH {\"t\":\"slice\",\"walk_id\":\"{}\",\"hop\":{},\"slice\":{},\"pages\":{},"
            + "\"queue_ms\":{},\"total_ms\":{},\"build_ms\":{},\"esquery_ms\":{},\"extract_ms\":{},"
            + "\"other_ms\":{},\"rels_kept\":{},\"end_reason\":\"{}\"}",
        walk.id,
        hop,
        slice,
        s.pages,
        ms(s.startNanos - s.submitNanos),
        ms(total),
        ms(s.buildNanos),
        ms(s.esqueryNanos),
        ms(s.extractNanos),
        ms(other),
        relsKept,
        s.endReason);
  }

  static void hop(@Nullable Walk walk, int hop, int urnsIn, int relsOut, long totalMs) {
    if (walk == null) {
      return;
    }
    long sliceMin = walk.sliceMinMs.get();
    log.info(
        "LINEAGE_BENCH {\"t\":\"hop\",\"walk_id\":\"{}\",\"hop\":{},\"urns_in\":{},\"rels_out\":{},"
            + "\"pit_create_ms\":{},\"slices_ms\":{},\"slice_max_ms\":{},\"slice_min_ms\":{},"
            + "\"pit_delete_ms\":{},\"postprocess_ms\":{},\"total_ms\":{}}",
        walk.id,
        hop,
        urnsIn,
        relsOut,
        walk.pitCreateMs.get(),
        walk.slicesMs.get(),
        walk.sliceMaxMs.get(),
        sliceMin == Long.MAX_VALUE ? 0 : sliceMin,
        walk.pitDeleteMs.get(),
        walk.postprocessMs.get(),
        totalMs);
    walk.resetHop();
  }

  static void end(@Nullable Walk walk, int hops, int totalRels, boolean truncated, String status) {
    if (walk == null) {
      return;
    }
    CURRENT.remove();
    LAST_WALK_ID.set(walk.id);
    log.info(
        "LINEAGE_BENCH {\"t\":\"walk\",\"walk_id\":\"{}\",\"start_urn\":\"{}\",\"hops\":{},"
            + "\"total_rels\":{},\"truncated\":{},\"status\":\"{}\",\"total_ms\":{}}",
        walk.id,
        walk.startUrn.replace("\"", "\\\""),
        hops,
        totalRels,
        truncated,
        status,
        msSince(walk.startNanos));
  }

  /** Post-walk work inside LineageSearchService.searchAcrossLineage. */
  public static void search(
      @Nullable String walkId,
      long walkNanos,
      long filterNanos,
      long resultNanos,
      String codePath,
      int relationships,
      long totalNanos) {
    if (!ENABLED) {
      return;
    }
    log.info(
        "LINEAGE_BENCH {\"t\":\"search\",\"walk_id\":\"{}\",\"walk_ms\":{},\"filter_ms\":{},"
            + "\"result_ms\":{},\"code_path\":\"{}\",\"relationships\":{},\"total_ms\":{}}",
        walkId,
        ms(walkNanos),
        ms(filterNanos),
        ms(resultNanos),
        codePath,
        relationships,
        ms(totalNanos));
  }

  /** The GraphQL searchAcrossLineage resolver: entity client call and result mapping. */
  public static void resolver(
      @Nullable String walkId, long serviceNanos, long mapNanos, long totalNanos) {
    if (!ENABLED) {
      return;
    }
    log.info(
        "LINEAGE_BENCH {\"t\":\"resolver\",\"walk_id\":\"{}\",\"service_ms\":{},\"map_ms\":{},"
            + "\"total_ms\":{}}",
        walkId,
        ms(serviceNanos),
        ms(mapNanos),
        ms(totalNanos));
  }
}
