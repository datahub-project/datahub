package com.linkedin.metadata.system_info;

import com.linkedin.metadata.system_info.collectors.PropertiesCollector;
import com.linkedin.metadata.system_info.collectors.SpringComponentsCollector;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service for collecting and providing comprehensive system information.
 *
 * <p>This service orchestrates the collection of system information from various sources including:
 *
 * <ul>
 *   <li>Spring application configuration properties and their metadata
 *   <li>Spring component status (GMS, MAE Consumer, MCE Consumer)
 *   <li>System properties and environment variables
 *   <li>Property source information and filtering statistics
 * </ul>
 *
 * <p><strong>Security Considerations:</strong>
 *
 * <ul>
 *   <li>This service exposes sensitive system configuration data
 *   <li>Access should be restricted to administrators with MANAGE_SYSTEM_OPERATIONS_PRIVILEGE
 *   <li>Sensitive properties (passwords, secrets, keys) are automatically redacted
 *   <li>Property filtering is applied to prevent accidental exposure of credentials
 * </ul>
 *
 * <p><strong>Performance:</strong>
 *
 * <ul>
 *   <li>Uses parallel execution for improved performance
 *   <li>Includes timeouts for remote component fetching
 *   <li>Graceful degradation when components are unavailable
 * </ul>
 *
 * @see SystemInfoController for REST API endpoints
 * @see PropertiesCollector for configuration property collection
 * @see SpringComponentsCollector for component status collection
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SystemInfoService {

  // Thread pool for parallel execution
  private final ExecutorService executorService = Executors.newFixedThreadPool(10);

  // Collectors
  private final SpringComponentsCollector springComponentsCollector;
  private final PropertiesCollector propertiesCollector;

  /**
   * Get Spring components information in parallel.
   *
   * @return SpringComponentsInfo containing status of GMS, MAE Consumer, and MCE Consumer
   */
  public SpringComponentsInfo getSpringComponentsInfo() {
    return springComponentsCollector.collect(executorService);
  }

  /**
   * Get all system properties with detailed metadata.
   *
   * <p>Returns comprehensive property information including:
   *
   * <ul>
   *   <li>Individual property details with sources and resolution
   *   <li>Property source metadata
   *   <li>Filtering and redaction statistics
   * </ul>
   *
   * @return SystemPropertiesInfo with detailed property metadata
   */
  public SystemPropertiesInfo getSystemPropertiesInfo() {
    return propertiesCollector.collect();
  }

  /**
   * Get only configuration properties as a simple map (for backward compatibility).
   *
   * <p>This method provides a simplified view of system properties without metadata, suitable for
   * legacy integrations or simple configuration debugging.
   *
   * @return Map of property keys to resolved values
   */
  public Map<String, Object> getPropertiesAsMap() {
    return propertiesCollector.getPropertiesAsMap();
  }

  /**
   * Get complete system information - spring components and properties in parallel.
   *
   * <p>This is the primary method for retrieving comprehensive system information. It executes both
   * component and property collection in parallel for optimal performance.
   *
   * @return SystemInfoResponse containing both component and property information
   * @throws SystemInfoException if collection fails or times out
   */
  public SystemInfoResponse getSystemInfo() {
    List<CompletableFuture<?>> futures =
        Arrays.asList(
            CompletableFuture.supplyAsync(this::getSpringComponentsInfo, executorService),
            CompletableFuture.supplyAsync(this::getSystemPropertiesInfo, executorService));

    try {
      CompletableFuture<Void> allFutures =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      allFutures.get(30, TimeUnit.SECONDS);

      return SystemInfoResponse.builder()
          .springComponents((SpringComponentsInfo) futures.get(0).getNow(null))
          .properties((SystemPropertiesInfo) futures.get(1).getNow(null))
          .build();
    } catch (Exception e) {
      log.error("Error collecting system info", e);
      throw new SystemInfoException("Failed to collect system information", e);
    }
  }

  @PreDestroy
  public void shutdown() {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
