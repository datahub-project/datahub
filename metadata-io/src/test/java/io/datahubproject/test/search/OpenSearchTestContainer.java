package io.datahubproject.test.search;

import static com.linkedin.metadata.DockerTestUtils.checkContainerEngine;

import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class OpenSearchTestContainer implements SearchTestContainer {
  private static final String OPENSEARCH_VERSION = "2.19.3";
  private static final String OPENSEARCH_IMAGE_NAME = "opensearchproject/opensearch";
  private static final String ENV_OPENSEARCH_IMAGE_FULL_NAME =
      System.getenv("OPENSEARCH_IMAGE_FULL_NAME");
  private static final String OPENSEARCH_IMAGE_FULL_NAME =
      ENV_OPENSEARCH_IMAGE_FULL_NAME != null
          ? ENV_OPENSEARCH_IMAGE_FULL_NAME
          : OPENSEARCH_IMAGE_NAME + ":" + OPENSEARCH_VERSION;
  private static final DockerImageName DOCKER_IMAGE_NAME =
      DockerImageName.parse(OPENSEARCH_IMAGE_FULL_NAME)
          .asCompatibleSubstituteFor(OPENSEARCH_IMAGE_NAME);

  // Memory allocation for OpenSearch - configurable via system property based on memory profile
  // System property is set by build.gradle based on GRADLE_MEMORY_PROFILE
  private static final String OPENSEARCH_HEAP =
      System.getProperty("testcontainers.opensearch.heap", "1024m");
  private static final String OPENSEARCH_JAVA_OPTS =
      "-Xms" + OPENSEARCH_HEAP + " -Xmx" + OPENSEARCH_HEAP;

  protected static final GenericContainer<?> OS_CONTAINER;
  private boolean isStarted = false;

  // A helper method to create an ElasticseachContainer defaulting to the current image and version,
  // with the ability
  // within firewalled environments to override with an environment variable to point to the offline
  // repository.
  static {
    OS_CONTAINER = new OpensearchContainer(DOCKER_IMAGE_NAME);
    checkContainerEngine(OS_CONTAINER.getDockerClient());
    OS_CONTAINER
        .withEnv("OPENSEARCH_JAVA_OPTS", OPENSEARCH_JAVA_OPTS)
        .withEnv(
            "plugins.security.disabled",
            "false") // Keep security plugin enabled for testing role creation
        .withEnv("plugins.security.ssl.http.enabled", "false")
        .withEnv("plugins.security.ssl.transport.enabled", "false")
        .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .withEnv("plugins.security.allow_unsafe_democertificates", "true")
        .withEnv("plugins.security.authcz.admin_dn", "")
        .withEnv("plugins.security.authcz.rest_impersonation_user", "*")
        .withEnv("plugins.security.restapi.roles_enabled", "[\"all_access\"]")
        .withStartupTimeout(STARTUP_TIMEOUT);
  }

  /**
   * Disables monitoring and analytics features in OpenSearch to reduce memory usage in test
   * containers. These features can create indices, consume memory, and cause performance issues in
   * test environments.
   *
   * <p>Cluster settings cannot be set via environment variables, so we use curl commands executed
   * inside the container after it starts.
   */
  private void disableMonitoringFeatures() {
    try {
      // Wait for OpenSearch to be fully ready
      Thread.sleep(3000);

      // Disable Top N Queries for all metrics (latency, cpu, memory)
      // This feature creates daily indices (top_queries-YYYY.MM.DD) that can cause "too many
      // fields" errors
      String[] metrics = {"latency", "cpu", "memory"};
      for (String metric : metrics) {
        disableClusterSetting("search.insights.top_queries." + metric + ".enabled", false);
      }

      // Disable Performance Analyzer if available (collects performance metrics)
      disableClusterSetting("plugins.performance_analyzer.enabled", false);

      // Disable other monitoring features that might create indices or consume resources
      // Note: These settings may not exist in all versions, so failures are expected and ignored
      disableClusterSetting("plugins.alerting.enabled", false);
      disableClusterSetting("plugins.anomaly_detection.enabled", false);

      // Disable search backpressure to reduce warnings in test environments
      // This feature monitors and cancels tasks that consume too much memory, but in test
      // containers
      // with limited memory, it can generate excessive warnings even in monitor_only mode
      disableClusterSetting("search_backpressure.enabled", false);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Warning: Interrupted while disabling monitoring features");
    } catch (Exception e) {
      // Log but don't fail - this is a best-effort optimization
      System.err.println("Warning: Could not disable monitoring features: " + e.getMessage());
    }
  }

  /**
   * Helper method to disable a cluster setting via the OpenSearch REST API.
   *
   * @param settingKey The cluster setting key to disable
   * @param value The value to set (typically false to disable)
   */
  private void disableClusterSetting(String settingKey, boolean value) {
    try {
      String jsonBody = String.format("{\"persistent\": {\"%s\": %s}}", settingKey, value);

      org.testcontainers.containers.Container.ExecResult result =
          OS_CONTAINER.execInContainer(
              "sh",
              "-c",
              "curl -sS -X PUT 'http://localhost:9200/_cluster/settings' "
                  + "-H 'Content-Type: application/json' "
                  + "-d '"
                  + jsonBody
                  + "'");

      if (result.getExitCode() == 0) {
        System.out.println("Successfully set cluster setting: " + settingKey + " = " + value);
      } else {
        // Many settings may not exist in all versions, so we only log at debug level
        String errorOutput = result.getStderr();
        if (errorOutput != null && errorOutput.contains("setting")) {
          // Setting doesn't exist - this is expected for optional features
          System.out.println(
              "Setting not available (expected for optional features): " + settingKey);
        } else {
          System.err.println(
              "Warning: Failed to set cluster setting "
                  + settingKey
                  + ". Exit code: "
                  + result.getExitCode()
                  + ", Output: "
                  + result.getStdout()
                  + ", Error: "
                  + errorOutput);
        }
      }
    } catch (Exception e) {
      // Log but don't fail - this is a best-effort optimization
      System.err.println(
          "Warning: Could not set cluster setting " + settingKey + ": " + e.getMessage());
    }
  }

  @Override
  public GenericContainer<?> startContainer() {
    if (!isStarted) {
      OS_CONTAINER.start();
      // Disable monitoring features to reduce memory usage and prevent index creation in tests
      disableMonitoringFeatures();
      isStarted = true;
    }
    return OS_CONTAINER;
  }

  @Override
  public void stopContainer() {
    OS_CONTAINER.stop();
  }
}
