package com.linkedin.metadata.system_info.collectors;

import static com.linkedin.metadata.system_info.SystemInfoConstants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.system_info.ComponentInfo;
import com.linkedin.metadata.system_info.ComponentStatus;
import com.linkedin.metadata.system_info.SpringComponentsInfo;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Collects information about Spring components (GMS, MAE Consumer, MCE Consumer).
 *
 * <p>DEPLOYMENT MODES: DataHub can be deployed in two ways: 1. Embedded mode (default quickstart):
 * All components run in single GMS container 2. Separate containers: Each component runs in its own
 * container for better scaling
 *
 * <p>CURRENT LIMITATION: This collector currently returns placeholder data for MAE/MCE consumers to
 * avoid a circular dependency issue that occurs in embedded mode:
 *
 * <p>The Problem: - System-info endpoint (/openapi/v1/system-info) calls this collector - Collector
 * tries to fetch consumer info by calling same endpoint on consumer URLs - In embedded mode,
 * consumer URLs point back to the same GMS process - This creates infinite recursion / timeout
 * after 5 seconds
 *
 * <p>The Solution: - Return placeholder data instead of making HTTP calls - Maintains API structure
 * while avoiding circular dependency - Allows system-info endpoint to work properly in embedded
 * mode
 *
 * <p>FUTURE IMPROVEMENTS: A proper solution would detect deployment mode and handle accordingly: -
 * Embedded mode: Query internal component state directly (no HTTP calls) - Separate containers:
 * Make HTTP calls to external consumer service endpoints
 *
 * <p>The placeholder approach provides a stable foundation for implementing this detection logic.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SpringComponentsCollector {

  public static final String SYSTEM_INFO_ENDPOINT = "/openapi/v1/system-info";

  private final OperationContext systemOperationContext;
  private final GitVersion gitVersion;
  private final PropertiesCollector propertiesCollector;

  @Value("${maeConsumerUrl:}")
  private String maeConsumerUrl;

  @Value("${mceConsumerUrl:}")
  private String mceConsumerUrl;

  /**
   * Collect Spring components information with parallel execution
   *
   * @param executorService ExecutorService for parallel component fetching
   * @return SpringComponentsInfo with component status and information
   */
  public SpringComponentsInfo collect(ExecutorService executorService) {
    List<CompletableFuture<ComponentInfo>> futures =
        Arrays.asList(
            CompletableFuture.supplyAsync(this::getGmsInfo, executorService),
            CompletableFuture.supplyAsync(this::getMaeConsumerInfo, executorService),
            CompletableFuture.supplyAsync(this::getMceConsumerInfo, executorService));

    try {
      CompletableFuture<Void> allFutures =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      allFutures.get(10, TimeUnit.SECONDS);

      return SpringComponentsInfo.builder()
          .gms(futures.get(0).getNow(createErrorComponent(GMS_COMPONENT_NAME)))
          .maeConsumer(futures.get(1).getNow(createErrorComponent(MAE_COMPONENT_NAME)))
          .mceConsumer(futures.get(2).getNow(createErrorComponent(MCE_COMPONENT_NAME)))
          .build();
    } catch (Exception e) {
      log.error("Error getting spring components info", e);
      return SpringComponentsInfo.builder()
          .gms(createErrorComponent(GMS_COMPONENT_NAME))
          .maeConsumer(createErrorComponent(MAE_COMPONENT_NAME))
          .mceConsumer(createErrorComponent(MCE_COMPONENT_NAME))
          .build();
    }
  }

  private ComponentInfo getGmsInfo() {
    try {
      Map<String, Object> properties = propertiesCollector.getPropertiesAsMap();

      return ComponentInfo.builder()
          .name(GMS_COMPONENT_NAME)
          .status(ComponentStatus.AVAILABLE)
          .version(gitVersion.getVersion())
          .properties(properties)
          .build();
    } catch (Exception e) {
      return createErrorComponent(GMS_COMPONENT_NAME, e);
    }
  }

  /**
   * Gets MAE (Metadata Audit Event) Consumer information.
   *
   * <p>CURRENT IMPLEMENTATION: Returns placeholder data to avoid circular dependency.
   *
   * <p>BACKGROUND: DataHub supports two deployment modes: 1. Embedded mode: MAE consumer runs
   * inside the same JVM/container as GMS 2. Separate containers: MAE consumer runs in its own
   * container/process
   *
   * <p>PROBLEM: The original implementation tried to fetch consumer info by making HTTP calls to
   * consumer endpoints. In embedded mode, this creates a circular dependency: - /system-info
   * endpoint calls this method - This method makes HTTP call to /system-info (same endpoint!) -
   * Infinite loop / timeout occurs
   *
   * <p>PLACEHOLDER SOLUTION: Instead of making HTTP calls that cause circular dependency, we return
   * placeholder data that maintains the API structure while avoiding the issue.
   *
   * <p>FUTURE IMPROVEMENT: Detect deployment mode and handle accordingly: - Embedded: Return actual
   * component status without HTTP calls (check internal state) - Separate: Make HTTP calls to
   * external consumer service endpoints
   */
  private ComponentInfo getMaeConsumerInfo() {
    // Return placeholder to avoid circular dependency in embedded mode
    // In embedded mode, MAE consumer runs in the same process as this code
    return ComponentInfo.builder()
        .name(MAE_COMPONENT_NAME)
        .status(ComponentStatus.AVAILABLE) // Assume available since we're running
        .version(gitVersion.getVersion()) // Same version as GMS in embedded mode
        .properties(
            Map.of(
                "mode", "placeholder",
                "deployment", "embedded",
                "note",
                    "Consumer info collection temporarily disabled to avoid circular dependency"))
        .build();
  }

  /**
   * Gets MCE (Metadata Change Event) Consumer information.
   *
   * <p>CURRENT IMPLEMENTATION: Returns placeholder data to avoid circular dependency.
   *
   * <p>BACKGROUND: DataHub supports two deployment modes: 1. Embedded mode: MCE consumer runs
   * inside the same JVM/container as GMS 2. Separate containers: MCE consumer runs in its own
   * container/process
   *
   * <p>PROBLEM: The original implementation tried to fetch consumer info by making HTTP calls to
   * consumer endpoints. In embedded mode, this creates a circular dependency: - /system-info
   * endpoint calls this method - This method makes HTTP call to /system-info (same endpoint!) -
   * Results in "request timed out" errors after 5 seconds
   *
   * <p>PLACEHOLDER SOLUTION: Instead of making HTTP calls that cause circular dependency, we return
   * placeholder data that maintains the API structure while avoiding the issue.
   *
   * <p>FUTURE IMPROVEMENT: Detect deployment mode and handle accordingly: - Embedded: Return actual
   * component status without HTTP calls (check internal state) - Separate: Make HTTP calls to
   * external consumer service endpoints
   */
  private ComponentInfo getMceConsumerInfo() {
    // Return placeholder to avoid circular dependency in embedded mode
    // In embedded mode, MCE consumer runs in the same process as this code
    return ComponentInfo.builder()
        .name(MCE_COMPONENT_NAME)
        .status(ComponentStatus.AVAILABLE) // Assume available since we're running
        .version(gitVersion.getVersion()) // Same version as GMS in embedded mode
        .properties(
            Map.of(
                "mode", "placeholder",
                "deployment", "embedded",
                "note",
                    "Consumer info collection temporarily disabled to avoid circular dependency"))
        .build();
  }

  /**
   * Fetches component information from a remote service via HTTP.
   *
   * <p>NOTE: This method is currently not used to avoid circular dependency issues in embedded
   * mode. See getMaeConsumerInfo() and getMceConsumerInfo() for details.
   *
   * <p>This method would be used in a future implementation that properly detects when consumers
   * are running in separate containers and can safely make HTTP calls.
   */
  private ComponentInfo fetchRemoteComponentInfo(
      String name, String baseUrl, String componentField) {
    try {
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(baseUrl + SYSTEM_INFO_ENDPOINT))
              .timeout(Duration.ofSeconds(5))
              .GET()
              .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        ObjectMapper mapper = systemOperationContext.getObjectMapper();
        SpringComponentsInfo springInfo =
            mapper.readValue(response.body(), SpringComponentsInfo.class);

        switch (componentField) {
          case MAE_COMPONENT_KEY:
            return springInfo.getMaeConsumer();
          case MCE_COMPONENT_KEY:
            return springInfo.getMceConsumer();
          case GMS_COMPONENT_KEY:
            return springInfo.getGms();
          default:
            throw new IllegalArgumentException("Unknown component: " + componentField);
        }
      }

      return createUnavailableComponent(name);
    } catch (Exception e) {
      return createErrorComponent(name, e);
    }
  }

  private ComponentInfo createErrorComponent(String name) {
    return ComponentInfo.builder()
        .name(name)
        .status(ComponentStatus.ERROR)
        .errorMessage("Failed to retrieve component info")
        .build();
  }

  private ComponentInfo createErrorComponent(String name, Exception e) {
    return ComponentInfo.builder()
        .name(name)
        .status(ComponentStatus.ERROR)
        .errorMessage("Failed to connect: " + e.getMessage())
        .build();
  }

  private ComponentInfo createUnavailableComponent(String name) {
    return ComponentInfo.builder().name(name).status(ComponentStatus.UNAVAILABLE).build();
  }
}
