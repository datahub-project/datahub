package com.datahub.health.controller;

import com.google.common.base.Suppliers;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/check")
@Tag(name = "HealthCheck", description = "An API for checking health of GMS and its clients.")
public class HealthCheckController {
  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient elasticClient;
  private final Supplier<ResponseEntity<String>> memoizedSupplier;

  public HealthCheckController(ConfigurationProvider config) {

    this.memoizedSupplier = Suppliers.memoizeWithExpiration(
        this::getElasticHealth, config.getHealthCheck().getCacheDurationSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Combined health check endpoint for checking GMS clients.
   * For now, just checks the health of the ElasticSearch client
   * @return A ResponseEntity with a Map of String (component name) to ResponseEntity (the health check status of
   * that component). The status code will be 200 if all components are okay, and 500 if one or more components are not
   * healthy.
   */
  @GetMapping(path = "/ready", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Map<String, ResponseEntity<String>>> getCombinedHealthCheck(String... checks) {

    Map<String, Supplier<ResponseEntity<String>>> healthChecks = new HashMap<>();
    healthChecks.put("elasticsearch", this::getElasticHealthWithCache);
    // Add new components here

    List<String> componentsToCheck = checks != null && checks.length > 0
        ? Arrays.asList(checks)
        : new ArrayList<>(healthChecks.keySet());

    Map<String, ResponseEntity<String>> componentHealth = new HashMap<>();
    for (String check : componentsToCheck) {
      componentHealth.put(check,
          healthChecks.getOrDefault(check,
              () -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Unrecognized component " + check))
              .get());
    }


    boolean isHealthy = componentHealth.values().stream().allMatch(resp -> resp.getStatusCode() == HttpStatus.OK);
    if (isHealthy) {
      return ResponseEntity.ok(componentHealth);
    }
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(componentHealth);
  }

  /**
   * Checks the memoized cache for the latest elastic health check result
   * @return The ResponseEntity containing the health check result
   */
  @GetMapping(path = "/elastic", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getElasticHealthWithCache() {
    return this.memoizedSupplier.get();
  }

  /**
   * Query ElasticSearch health endpoint
   * @return A response including the result from ElasticSearch
   */
  private ResponseEntity<String> getElasticHealth() {
    String responseString = null;
    try {
      ClusterHealthRequest request = new ClusterHealthRequest();
      ClusterHealthResponse response = elasticClient.cluster().health(request, RequestOptions.DEFAULT);

      boolean isHealthy = !response.isTimedOut() && response.getStatus() != ClusterHealthStatus.RED;
      responseString = response.toString();
      if (isHealthy) {
        return ResponseEntity.ok(responseString);
      }
    } catch (Exception e) {
      if (responseString == null) {
        // Couldn't get a response, fill in the string from the exception message
        responseString = e.getMessage();
      }
    }
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(responseString);
  }
}
