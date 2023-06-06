package com.datahub.health.controller;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.TimeUnit;
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
  private Supplier<ResponseEntity<String>> memoizedSupplier;

  public HealthCheckController() {
    this.memoizedSupplier = Suppliers.memoizeWithExpiration(
        this::getElasticHealth, 5, TimeUnit.SECONDS);
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
   *
   * @return
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
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(responseString);
  }
}
