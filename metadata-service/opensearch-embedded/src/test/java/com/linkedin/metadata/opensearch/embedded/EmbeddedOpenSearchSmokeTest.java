package com.linkedin.metadata.opensearch.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Boots a real node and exercises HTTP health, painless scripts, and optional k-NN mappings. */
public class EmbeddedOpenSearchSmokeTest {
  private static Process node;
  private static Path dataDir;
  private static int port;

  @BeforeClass
  public static void startNode() throws Exception {
    dataDir = Files.createTempDirectory("os-smoke-");
    Path output = dataDir.resolve("console.log");
    ProcessBuilder builder =
        new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx1g",
            "-cp",
            System.getProperty("opensearch.embedded.classpath"),
            EmbeddedOpenSearchMain.class.getName(),
            "--port",
            "0",
            "--data",
            dataDir.toString(),
            "--cluster",
            "smoke");
    builder
        .environment()
        .put("OPENSEARCH_KNN_ENABLED", System.getProperty("opensearch.knn.enabled"));
    node = builder.redirectErrorStream(true).redirectOutput(output.toFile()).start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    Pattern ready = Pattern.compile("\\bREADY (\\d+)\\b");
    while (node.isAlive() && System.nanoTime() < deadline) {
      Matcher match = ready.matcher(Files.readString(output));
      if (match.find()) {
        port = Integer.parseInt(match.group(1));
        return;
      }
      Thread.sleep(500);
    }
    fail("Embedded OpenSearch did not become ready. Output:\n" + Files.readString(output));
  }

  @AfterClass
  public static void stopNode() throws Exception {
    if (node != null) {
      node.destroy();
      if (!node.waitFor(10, TimeUnit.SECONDS)) {
        node.destroyForcibly().waitFor();
      }
    }
    if (dataDir != null) {
      try (var paths = Files.walk(dataDir)) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }

  @Test
  public void bootAndHealthCheck() throws IOException {
    assertEquals(200, request("GET", "/_cluster/health", null));
    // Script execution catches an incompatible ANTLR runtime even when health is green.
    assertEquals(
        200, request("POST", "/_scripts/painless/_execute", "{\"script\":{\"source\":\"1+1\"}}"));
  }

  @Test
  public void knnMappingWhenEnabled() throws IOException {
    assumeTrue(Boolean.getBoolean("opensearch.knn.enabled"));
    String mapping =
        "{\"settings\":{\"index.knn\":true},"
            + "\"mappings\":{\"properties\":{\"vec\":{"
            + "\"type\":\"knn_vector\",\"dimension\":3,"
            + "\"method\":{\"name\":\"hnsw\",\"engine\":\"lucene\",\"space_type\":\"l2\"}}}}}";
    assertEquals(200, request("PUT", "/test-knn", mapping));
  }

  private static int request(String method, String path, String body) throws IOException {
    HttpURLConnection connection =
        (HttpURLConnection) URI.create("http://localhost:" + port + path).toURL().openConnection();
    connection.setConnectTimeout(5_000);
    connection.setReadTimeout(30_000);
    connection.setRequestMethod(method);
    try {
      if (body != null) {
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        try (var stream = connection.getOutputStream()) {
          stream.write(body.getBytes(StandardCharsets.UTF_8));
        }
      }
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
