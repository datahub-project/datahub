package com.linkedin.metadata.opensearch.embedded;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.unit.TimeValue;

/**
 * Standalone single-node OpenSearch for local development stacks: GMS and datahub-upgrade processes
 * point {@code ELASTICSEARCH_HOST/PORT} at it and isolate themselves with {@code INDEX_PREFIX}.
 * Prints {@code READY <port>} on stdout once the cluster is yellow.
 *
 * <p>Usage: {@code --port <n> (0 = pick free)} {@code --data <dir>} {@code --cluster <name>}.
 * Direct {@code java} launchers must supply their own {@code -Xmx}; Gradle and generated start
 * scripts default to 4 GB, configurable at build time with {@code EMBEDDED_OPENSEARCH_HEAP}.
 */
public final class EmbeddedOpenSearchMain {

  /**
   * Worktrees use separate index prefixes (~120 indices per GMS instance), so after a restart
   * recovering all primaries can far exceed OpenSearchRunner.ensureYellow()'s built-in timeout.
   */
  private static final TimeValue READY_TIMEOUT = TimeValue.timeValueMinutes(10);

  private static final TimeValue HEALTH_POLL = TimeValue.timeValueSeconds(15);

  private static final int MAX_SHARDS_PER_NODE = 2000;

  private EmbeddedOpenSearchMain() {}

  public static void main(String[] args) throws Exception {
    int port = 0;
    Path dataDir = null;
    String clusterName = "embedded-opensearch";
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port" -> port = Integer.parseInt(args[++i]);
        case "--data" -> dataDir = Paths.get(args[++i]);
        case "--cluster" -> clusterName = args[++i];
        default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }
    if (port == 0) {
      port = findAvailablePort();
    }
    if (dataDir == null) {
      dataDir = Files.createTempDirectory("embedded-opensearch-");
    }
    Files.createDirectories(dataDir);
    final int httpPort = port;
    final Path base = dataDir;

    OpenSearchRunner runner = new OpenSearchRunner();
    runner.onBuild(
        (i, settingsBuilder) ->
            settingsBuilder
                .put("http.port", String.valueOf(httpPort))
                .put("transport.port", 0)
                .put("path.data", base.resolve("data").toString())
                .put("path.logs", base.resolve("logs").toString())
                .put("discovery.type", "single-node")
                .put("cluster.routing.allocation.disk.threshold_enabled", false)
                .put("action.destructive_requires_name", false)
                .put("indices.memory.index_buffer_size", "10%")
                // Default 4: with hundreds of small shards, recovery is otherwise serialised.
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 32)
                // The default 1000 fits about 8 GMS instances; 2000 leaves room for 10 worktrees
                // and additional local instances, each with its own index prefix.
                .put("cluster.max_shards_per_node", MAX_SHARDS_PER_NODE));
    OpenSearchRunner.Configs configs =
        OpenSearchRunner.newConfigs()
            .basePath(base.toString())
            .numOfNode(1)
            .clusterName(clusterName);
    if (Boolean.parseBoolean(System.getenv().getOrDefault("OPENSEARCH_KNN_ENABLED", "false"))) {
      configs.pluginTypes("org.opensearch.knn.plugin.KNNPlugin");
    }
    start(runner, configs, httpPort, () -> awaitYellow(runner));
  }

  static void start(
      OpenSearchRunner runner,
      OpenSearchRunner.Configs configs,
      int httpPort,
      Runnable awaitReady) {
    CountDownLatch shutdown = new CountDownLatch(1);
    AtomicBoolean closed = new AtomicBoolean();
    AtomicBoolean shuttingDown = new AtomicBoolean();
    Runnable close =
        () -> {
          synchronized (runner) {
            if (closed.compareAndSet(false, true)) {
              try {
                runner.close();
              } catch (Throwable e) {
                System.err.println("Failed to close embedded OpenSearch");
                e.printStackTrace(System.err);
              }
            }
          }
        };
    // SIGTERM can arrive before readiness, including while build is starting node threads.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  shuttingDown.set(true);
                  close.run();
                  shutdown.countDown();
                }));
    try {
      // Do not let shutdown close an empty runner while build is still adding the node.
      synchronized (runner) {
        if (closed.get()) {
          return;
        }
        runner.build(configs);
      }
      awaitReady.run();
      if (!closed.get()) {
        System.out.println("READY " + httpPort);
        System.out.flush();
      }
      shutdown.await();
    } catch (Throwable failure) {
      if (shuttingDown.get()) {
        // The shutdown hook owns cleanup once JVM shutdown has begun.
        return;
      }
      System.err.println("Failed to start embedded OpenSearch");
      failure.printStackTrace(System.err);
      close.run();
      // Node threads can keep the JVM alive even when startup or best-effort cleanup fails.
      System.exit(1);
    }
  }

  private static void awaitYellow(final OpenSearchRunner runner) {
    final long deadline = System.nanoTime() + READY_TIMEOUT.nanos();
    while (true) {
      final ClusterHealthResponse health =
          runner
              .client()
              .admin()
              .cluster()
              .prepareHealth()
              .setWaitForYellowStatus()
              .setTimeout(HEALTH_POLL)
              .get();
      if (health.getStatus() != ClusterHealthStatus.RED) {
        return;
      }
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException(
            "OpenSearch not yellow after "
                + READY_TIMEOUT
                + ": "
                + health.getUnassignedShards()
                + " shards unassigned");
      }
      System.err.println(
          "waiting for shard recovery: "
              + health.getActiveShards()
              + " active, "
              + health.getInitializingShards()
              + " initializing, "
              + health.getUnassignedShards()
              + " unassigned");
    }
  }

  private static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
