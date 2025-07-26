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

@Slf4j
@Service
@RequiredArgsConstructor
public class SystemInfoService {

  // Thread pool for parallel execution
  private final ExecutorService executorService = Executors.newFixedThreadPool(10);

  // Collectors
  private final SpringComponentsCollector springComponentsCollector;
  private final PropertiesCollector propertiesCollector;

  /** Get Spring components information in parallel */
  public SpringComponentsInfo getSpringComponentsInfo() {
    return springComponentsCollector.collect(executorService);
  }

  /** Get all system properties with metadata */
  public SystemPropertiesInfo getSystemPropertiesInfo() {
    return propertiesCollector.collect();
  }

  /** Get only configuration properties as a simple map (for backward compatibility) */
  public Map<String, Object> getPropertiesAsMap() {
    return propertiesCollector.getPropertiesAsMap();
  }

  /** Get system information - spring components and properties in parallel */
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
