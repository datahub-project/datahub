package com.linkedin.metadata.opensearch.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.codelibs.opensearch.runner.OpenSearchRunner;
import org.junit.Test;

public class EmbeddedOpenSearchStartupTest {
  @Test
  public void readinessFailureClosesNodeAndExits() throws Exception {
    Path dataDir = Files.createTempDirectory("os-startup-failure-");
    Path output = dataDir.resolve("console.log");
    Process node = null;
    try {
      String testClasses =
          Path.of(
                  EmbeddedOpenSearchStartupTest.class
                      .getProtectionDomain()
                      .getCodeSource()
                      .getLocation()
                      .toURI())
              .toString();
      node =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-Xmx1g",
                  "-cp",
                  testClasses
                      + File.pathSeparator
                      + System.getProperty("opensearch.embedded.classpath"),
                  FailingStartup.class.getName(),
                  dataDir.toString())
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      boolean exited = node.waitFor(90, TimeUnit.SECONDS);
      String console = Files.readString(output);
      assertTrue("Startup failure did not exit. Output:\n" + console, exited);
      assertEquals(console, 1, node.exitValue());
      assertFalse(console, console.contains("READY "));
      assertTrue(console, console.contains("readiness failure"));
      assertTrue(console, console.contains("Closed all nodes."));
    } finally {
      if (node != null && node.isAlive()) {
        node.destroyForcibly().waitFor();
      }
      try (var paths = Files.walk(dataDir)) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }

  public static class FailingStartup {
    public static void main(String[] args) throws Exception {
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
      OpenSearchRunner runner = new OpenSearchRunner();
      runner.onBuild(
          (i, settings) ->
              settings
                  .put("http.port", port)
                  .put("transport.port", 0)
                  .put("discovery.type", "single-node")
                  .put("cluster.routing.allocation.disk.threshold_enabled", false));
      EmbeddedOpenSearchMain.start(
          runner,
          OpenSearchRunner.newConfigs().basePath(args[0]).numOfNode(1).clusterName("startup-test"),
          port,
          () -> {
            throw new IllegalStateException("readiness failure");
          });
    }
  }
}
