package io.datahubproject.openapi.health;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.BootstrapManager;
import com.linkedin.metadata.boot.GracefulShutdownHandler;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.health.DatastoreHealthStatus;
import com.linkedin.metadata.health.PrimaryDatastoreHealthProbe;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
@Tag(name = "HealthCheck", description = "An API for checking health of GMS and its clients.")
public class HealthCheckController {

  private static final Logger log = LoggerFactory.getLogger(HealthCheckController.class);

  @Autowired(required = false)
  @Qualifier("searchClientShim")
  private SearchClientShim<?> elasticClient;

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext systemOperationContext;

  @Autowired
  @Qualifier("bootstrapManager")
  private BootstrapManager bootstrapManager;

  /**
   * Graceful shutdown handler for detecting when service is shutting down.
   *
   * <p>Only present when {@code server.shutdown=graceful} is configured (via @ConditionalOnProperty
   * in GracefulShutdownHandler). When null (immediate shutdown mode), the health endpoint does not
   * check shutdown state. This dual-mechanism design allows shutdown behavior to be toggled via
   * configuration.
   *
   * <p>Uses {@code required = false} to gracefully handle both enabled and disabled modes.
   */
  @Autowired(required = false)
  private GracefulShutdownHandler shutdownHandler;

  private final ConfigurationProvider configurationProvider;
  private final PrimaryDatastoreHealthProbe primaryDatastoreHealthProbe;
  private final Supplier<ResponseEntity<String>> memoizedElasticSupplier;
  private final Supplier<ResponseEntity<String>> memoizedDatastoreSupplier;

  public HealthCheckController(
      ConfigurationProvider config,
      @Autowired(required = false) PrimaryDatastoreHealthProbe primaryDatastoreHealthProbe) {

    this.configurationProvider = config;
    this.primaryDatastoreHealthProbe = primaryDatastoreHealthProbe;
    long cacheDurationSeconds = config.getHealthCheck().getCacheDurationSeconds();
    this.memoizedElasticSupplier = memoize(cacheDurationSeconds, this::getElasticHealth);
    this.memoizedDatastoreSupplier = memoize(cacheDurationSeconds, this::getPrimaryDatastoreHealth);
  }

  private static Supplier<ResponseEntity<String>> memoize(
      long cacheDurationSeconds, Supplier<ResponseEntity<String>> delegate) {
    if (cacheDurationSeconds <= 0) {
      return delegate;
    }
    return Suppliers.memoizeWithExpiration(delegate, cacheDurationSeconds, TimeUnit.SECONDS);
  }

  @GetMapping(path = "/check/ready", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Boolean> getCombinedHealthCheck(
      @RequestParam(name = "checks", required = false) List<String> checks) {
    ResponseEntity<Map<String, ResponseEntity<String>>> combined = getCombinedDebug(checks);
    return ResponseEntity.status(combined.getStatusCode())
        .body(combined.getStatusCode().is2xxSuccessful());
  }

  /**
   * Bootstrap-aware health endpoint that replaces the legacy /health servlet.
   *
   * <p>This endpoint implements proper readiness checking by waiting for critical bootstrap steps
   * to complete before marking the service as healthy. This prevents the race condition where
   * health checks pass before essential functionality (like admin authentication) is available.
   *
   * <p>Returns: - HTTP 503 during bootstrap (service starting, not ready for traffic) - HTTP 200
   * after bootstrap (service ready for traffic)
   *
   * <p>Response body is intentionally empty to maintain backward compatibility with existing health
   * check configurations (Docker Compose, load balancers, etc).
   *
   * <p>This endpoint is excluded from authentication via configuration:
   * authentication.excludedPaths: /health (see application.yaml)
   */
  @GetMapping(path = "/health")
  public ResponseEntity<Void> getBootstrapAwareHealth() {
    boolean isShuttingDown = shutdownHandler != null && shutdownHandler.isShutdownInProgress();
    if (!bootstrapManager.areBlockingStepsComplete() || isShuttingDown) {
      // Service is either still bootstrapping or shutting down - not ready for traffic
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }

    // Bootstrap complete and not shutting down - service ready for traffic
    return ResponseEntity.ok().build();
  }

  /**
   * Liveness check endpoint - returns 200 if the process is alive, regardless of bootstrap status.
   *
   * <p>This is useful for Kubernetes liveness probes or process monitors that need to distinguish
   * between "process crashed" (restart needed) vs "process starting up" (wait for readiness).
   *
   * <p>Use /health for readiness (traffic routing decisions) Use /health/live for liveness (restart
   * decisions)
   */
  @GetMapping(path = "/health/live")
  public ResponseEntity<Void> getLivenessCheck() {
    // Always return 200 if we can process the request - process is alive
    return ResponseEntity.ok().build();
  }

  /**
   * Detailed health endpoint that consolidates checks into a single JSON response.
   *
   * <p>Includes Elasticsearch/OpenSearch when enabled, and the primary datastore (JDBC via Ebean or
   * Cassandra) when a probe is registered.
   *
   * <p>Always returns HTTP 200 with the status in the body (so agents can parse the response even
   * when unhealthy).
   */
  @GetMapping(path = "/health/detailed", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Map<String, Object>> getDetailedHealth() {
    Map<String, Object> result = new LinkedHashMap<>();

    boolean bootstrapped = bootstrapManager.areBlockingStepsComplete();
    result.put("bootstrapped", bootstrapped);

    ResponseEntity<String> esHealth;
    try {
      esHealth = getElasticDebugWithCache();
    } catch (Exception e) {
      log.error("Unexpected error getting Elasticsearch health", e);
      esHealth = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getMessage());
    }
    boolean esHealthy = esHealth.getStatusCode() == HttpStatus.OK;
    result.put("elasticsearch", esHealthy ? "green" : "unhealthy");
    result.put("elasticsearch_detail", esHealth.getBody());

    ResponseEntity<String> dsHealth;
    try {
      dsHealth = getPrimaryDatastoreDebugWithCache();
    } catch (Exception e) {
      log.error("Unexpected error getting primary datastore health", e);
      dsHealth = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getMessage());
    }
    boolean dsHealthy = dsHealth.getStatusCode() == HttpStatus.OK;
    result.put("primary_metadata_store", dsHealthy ? "green" : "unhealthy");
    result.put("primary_metadata_store_detail", dsHealth.getBody());

    boolean ready = bootstrapped && esHealthy && dsHealthy;
    result.put("ready", ready);
    result.put("timestamp", System.currentTimeMillis());

    return ResponseEntity.ok(result);
  }

  /**
   * Combined health check for configured infrastructure clients (search cluster and primary
   * datastore).
   *
   * @return Map of component name to health ResponseEntity; HTTP 503 if any active check fails.
   */
  @GetMapping(path = "/debug/ready", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Map<String, ResponseEntity<String>>> getCombinedDebug(
      @RequestParam(name = "checks", required = false) List<String> checks) {
    Map<String, Supplier<ResponseEntity<String>>> healthChecks = new LinkedHashMap<>();
    healthChecks.put("elasticsearch", this::getElasticDebugWithCache);
    healthChecks.put("primary_metadata_store", this::getPrimaryDatastoreDebugWithCache);

    List<String> componentsToCheck =
        checks != null && !checks.isEmpty() ? checks : new ArrayList<>(healthChecks.keySet());

    Map<String, ResponseEntity<String>> componentHealth = new LinkedHashMap<>();
    for (String check : componentsToCheck) {
      componentHealth.put(
          check,
          healthChecks
              .getOrDefault(
                  check,
                  () ->
                      ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                          .body("Unrecognized component " + check))
              .get());
    }

    boolean isHealthy =
        componentHealth.values().stream().allMatch(resp -> resp.getStatusCode() == HttpStatus.OK);
    if (isHealthy) {
      return ResponseEntity.ok(componentHealth);
    }
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(componentHealth);
  }

  @GetMapping(path = "/check/elastic", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Boolean> getElasticHealthWithCache() {
    return ResponseEntity.status(getElasticDebugWithCache().getStatusCode())
        .body(getElasticDebugWithCache().getStatusCode().is2xxSuccessful());
  }

  @GetMapping(path = "/debug/elastic", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getElasticDebugWithCache() {
    return this.memoizedElasticSupplier.get();
  }

  /** Cached JDBC/Cassandra primary entity store probe (same TTL as Elasticsearch health). */
  @GetMapping(path = "/debug/primary_metadata_store", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getPrimaryDatastoreDebugWithCache() {
    return this.memoizedDatastoreSupplier.get();
  }

  private ResponseEntity<String> getPrimaryDatastoreHealth() {
    if (primaryDatastoreHealthProbe == null) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body("Primary datastore health probe is not registered");
    }
    DatastoreHealthStatus status = primaryDatastoreHealthProbe.probe();
    if (status.isHealthy()) {
      return ResponseEntity.ok(status.getMessage());
    }
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(status.getMessage());
  }

  private ResponseEntity<String> getElasticHealth() {
    ElasticSearchConfiguration es = configurationProvider.getElasticSearch();
    if (es == null || !es.isEnabled()) {
      return ResponseEntity.ok(
          "Elasticsearch/OpenSearch integration disabled (elasticsearch.enabled=false)");
    }
    if (elasticClient == null) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(
              "Elasticsearch/OpenSearch is enabled in configuration but the search client is not "
                  + "available");
    }
    String responseString = null;
    try {
      ClusterHealthRequest request = new ClusterHealthRequest();
      // TODO(opcontext-pr6): cannot use per-event opContext — health probe has no per-event
      // identity
      ClusterHealthResponse response =
          elasticClient.clusterHealth(systemOperationContext, request, RequestOptions.DEFAULT);

      boolean isHealthy = !response.isTimedOut() && response.getStatus() != ClusterHealthStatus.RED;
      responseString = response.toString();
      if (isHealthy) {
        return ResponseEntity.ok(responseString);
      }
    } catch (Exception e) {
      if (responseString == null) {
        responseString = e.getMessage();
      }
    }
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(responseString);
  }
}
