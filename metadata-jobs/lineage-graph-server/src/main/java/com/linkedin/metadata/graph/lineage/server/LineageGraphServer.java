package com.linkedin.metadata.graph.lineage.server;

import com.linkedin.metadata.graph.lineage.core.GraphSnapshot;
import com.linkedin.metadata.graph.lineage.core.ImpactWalker;
import com.linkedin.metadata.graph.lineage.core.SnapshotIO;
import java.lang.foreign.Arena;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Entry point of the Lineage Graph Service.
 *
 * <p>It serves walks over the latest snapshot the builder published ({@link
 * LineageSnapshotBuilder}) and never applies individual edits. Startup is the same as a reload: a
 * watcher polls the snapshot root, and when {@code LATEST} names a new snapshot it:
 *
 * <ol>
 *   <li>maps it (no reading yet);
 *   <li>publishes it with one volatile write, so new walks use it;
 *   <li>retires the old snapshot, which is unmapped as soon as the walks running on it finish;
 *   <li>then, once the old one is unmapped, warms the new one in the background, so the two are
 *       never both fully resident. Walks in between fault in the pages they touch.
 * </ol>
 *
 * <p>It needs no entity registry, Kafka or OpenSearch: everything a walk or search reads is in the
 * snapshot.
 */
@Slf4j
public final class LineageGraphServer {

  enum State {
    WAITING_FOR_SNAPSHOT,
    READY,
    FAILED
  }

  private static final int RELOAD_HISTORY = 20;

  private final ServerConfig config;
  private final long startedAt = ManagementFactory.getRuntimeMXBean().getStartTime();
  private final ImpactWalker walker;
  private final Map<String, Object> startup = new ConcurrentHashMap<>();
  private final Deque<Map<String, Object>> reloads = new ArrayDeque<>();
  private volatile State state = State.WAITING_FOR_SNAPSHOT;
  private volatile String failure;
  private volatile GraphSnapshot current;
  private volatile Path currentDir;

  LineageGraphServer(ServerConfig config) {
    this.config = config;
    this.walker = new ImpactWalker(config.walkConcurrency());
  }

  public static void main(String[] args) throws Exception {
    ServerConfig config = ServerConfig.fromEnv();
    LineageGraphServer app = new LineageGraphServer(config);
    HttpApi api = new HttpApi(config.port(), app, config.walkConcurrency());
    api.start();
    log.info(
        "Listening on port {}; snapshots from {}, {} walks at once",
        config.port(),
        config.snapshotRoot(),
        config.walkConcurrency());
    Runtime.getRuntime().addShutdownHook(new Thread(api::stop));
    Thread watcher = new Thread(app::watch, "snapshot-watcher");
    watcher.setDaemon(true);
    watcher.start();
  }

  /** Polls the snapshot root and reloads whenever {@code LATEST} changes. */
  void watch() {
    while (true) {
      try {
        Optional<Path> latest = SnapshotIO.latest(config.snapshotRoot());
        if (latest.isPresent() && !latest.get().equals(currentDir)) {
          reload(latest.get());
        }
      } catch (Exception e) {
        log.error("Snapshot reload failed; serving the previous snapshot", e);
        failure = String.valueOf(e);
        if (current == null) {
          state = State.FAILED;
        }
      }
      try {
        Thread.sleep(config.reloadPollMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** Maps {@code dir}, swaps it in and retires the previous snapshot. */
  synchronized void reload(Path dir) throws Exception {
    // Filled in later by the warm thread and by whichever walk releases the old snapshot last.
    Map<String, Object> r = Collections.synchronizedMap(new LinkedHashMap<>());
    long t0 = System.nanoTime();
    r.put("snapshot", dir.getFileName().toString());
    r.put("startedAt", System.currentTimeMillis());
    r.put("rssBeforeBytes", rssBytes());
    Arena arena = Arena.ofShared();
    SnapshotIO.Loaded loaded;
    try {
      loaded = SnapshotIO.load(dir, arena);
    } catch (Exception e) {
      arena.close();
      throw e;
    }
    GraphSnapshot next =
        new GraphSnapshot(
            loaded.graph(),
            loaded.meta().builtAtMillis(),
            loaded.meta().pointInTimeMillis(),
            arena);
    r.put("mapMs", ms(t0));

    GraphSnapshot old = current;
    current = next;
    currentDir = dir;
    state = State.READY;
    failure = null;
    long published = System.nanoTime();
    r.put("publishMs", ms(t0));
    r.put("snapshotBytes", SnapshotIO.sizeOnDisk(dir));
    r.put("nodes", loaded.graph().nodeCount());
    r.put("edges", loaded.graph().edgeCount());
    if (startup.isEmpty()) {
      startup.put("readySinceProcessStartMs", System.currentTimeMillis() - startedAt);
      startup.put("firstMapMs", r.get("mapMs"));
    }
    Runnable warm = () -> warm(next, dir, r);
    if (old != null) {
      r.put("oldHolders", old.holders());
      old.retire(
          () -> {
            r.put("drainMs", ms(published));
            warm.run();
          });
    } else {
      warm.run();
    }
    synchronized (reloads) {
      reloads.addFirst(r);
      while (reloads.size() > RELOAD_HISTORY) {
        reloads.removeLast();
      }
    }
    log.info(
        "Serving snapshot {} ({} nodes, {} edges), mapped in {} ms",
        dir,
        loaded.graph().nodeCount(),
        loaded.graph().edgeCount(),
        r.get("mapMs"));
  }

  /** Touches every page of {@code snapshot} on a virtual thread, unless it was replaced. */
  private static void warm(GraphSnapshot snapshot, Path dir, Map<String, Object> r) {
    Thread.ofVirtual()
        .name("snapshot-warm")
        .start(
            () -> {
              if (!snapshot.tryAcquire()) {
                return; // already replaced
              }
              try {
                r.put("rssBeforeWarmBytes", rssBytes());
                long t = System.nanoTime();
                long bytes = snapshot.base().warm();
                r.put("warmMs", ms(t));
                r.put("warmBytes", bytes);
                r.put("rssAfterWarmBytes", rssBytes());
              } catch (RuntimeException e) {
                log.warn("Warming snapshot {} failed", dir, e);
              } finally {
                snapshot.release();
              }
            });
  }

  /** The current snapshot, held for a walk; null before the first load. Release it after use. */
  GraphSnapshot acquire() {
    while (true) {
      GraphSnapshot s = current;
      if (s == null) {
        return null;
      }
      if (s.tryAcquire()) {
        return s;
      }
      // Retired between the read and the acquire: the next read sees its replacement.
    }
  }

  State state() {
    return state;
  }

  ImpactWalker walker() {
    return walker;
  }

  Map<String, Object> readiness() {
    Map<String, Object> r = new LinkedHashMap<>();
    r.put("state", state.name());
    if (failure != null) {
      r.put("failure", failure);
    }
    // The GMS client still reads a lag; a periodic snapshot has none to report.
    r.put("lagMillis", 0);
    GraphSnapshot s = current;
    if (s != null) {
      r.put("snapshot", currentDir.getFileName().toString());
      r.put("snapshotBuiltAt", s.builtAtMillis());
      r.put("snapshotAgeMillis", System.currentTimeMillis() - s.builtAtMillis());
      r.put("pointInTime", s.pointInTimeMillis());
      r.put("nodes", s.nodeCount());
      r.put("edges", s.base().edgeCount());
    }
    return r;
  }

  Map<String, Object> stats() {
    Map<String, Object> m = new LinkedHashMap<>(readiness());
    m.put("startup", new LinkedHashMap<>(startup));
    synchronized (reloads) {
      List<Map<String, Object>> history = new ArrayList<>();
      for (Map<String, Object> r : reloads) {
        synchronized (r) {
          history.add(new LinkedHashMap<>(r));
        }
      }
      m.put("reloads", history);
    }
    Map<String, Object> mem = new LinkedHashMap<>();
    GraphSnapshot s = current;
    if (s != null) {
      mem.put("graphBytes", s.base().memory());
      mem.put("walksOnCurrentSnapshot", s.holders());
    }
    MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    mem.put("heapUsedBytes", heap.getUsed());
    mem.put("heapCommittedBytes", heap.getCommitted());
    mem.put("rssBytes", rssBytes());
    m.put("memory", mem);
    return m;
  }

  static long rssBytes() {
    try {
      Process p =
          new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(ProcessHandle.current().pid()))
              .start();
      String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      p.waitFor();
      return Long.parseLong(out) * 1024;
    } catch (Exception e) {
      return -1;
    }
  }

  private static long ms(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }
}
