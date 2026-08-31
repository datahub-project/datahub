package io.datahubproject.test.search;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;

/**
 * Shared utilities for search test containers (Elasticsearch, OpenSearch). Use from suite classes
 * that provide the {@code testSearchContainer} bean.
 */
public final class SearchContainerUtils {

  private SearchContainerUtils() {}

  private static final int HTTP_PORT = 9200;
  private static final int MAX_WAIT_SECONDS = 60;
  private static final int POLL_INTERVAL_MS = 1000;

  /**
   * Waits for the cluster health endpoint to return 200 so the first client call does not get
   * "Connection refused". Works for both Elasticsearch and OpenSearch (same {@code _cluster/health}
   * API on port 9200).
   *
   * @param container the started search container
   * @throws IllegalStateException if health check does not succeed within the timeout or on
   *     interruption
   */
  public static void waitForClusterReady(GenericContainer<?> container) {
    int port = container.getMappedPort(HTTP_PORT);
    URI healthUri = URI.create("http://localhost:" + port + "/_cluster/health");
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    HttpRequest request =
        HttpRequest.newBuilder(healthUri).timeout(Duration.ofSeconds(5)).GET().build();
    try {
      for (int i = 0; i < MAX_WAIT_SECONDS; i++) {
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
          return;
        }
        Thread.sleep(POLL_INTERVAL_MS);
      }
      throw new IllegalStateException(
          "Search container _cluster/health did not return 200 within " + MAX_WAIT_SECONDS + "s");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for search container", e);
    } catch (Exception e) {
      throw new IllegalStateException("Search container did not become ready", e);
    }
  }
}
