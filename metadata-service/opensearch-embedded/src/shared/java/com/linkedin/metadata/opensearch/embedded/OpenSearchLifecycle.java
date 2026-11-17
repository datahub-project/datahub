package com.linkedin.metadata.opensearch.embedded;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.codelibs.opensearch.runner.OpenSearchRunner;

/** Shared shutdown and startup ownership, compiled independently against each server. */
final class OpenSearchLifecycle {
  private OpenSearchLifecycle() {}

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

  static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
