package com.linkedin.metadata.system_info;

import com.linkedin.metadata.system_info.SystemInfoDtos.*;
import com.linkedin.metadata.system_info.collectors.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
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
  private final StorageSystemsCollector storageSystemsCollector;
  private final KubernetesCollector kubernetesCollector;
  private final KafkaCollector kafkaCollector;
  private final PropertiesCollector propertiesCollector;

  /** Get Spring components information in parallel */
  public SpringComponentsInfo getSpringComponentsInfo() {
    return springComponentsCollector.collect(executorService);
  }

  /** Get storage systems information in parallel */
  public StorageInfo getStorageInfo() {
    return storageSystemsCollector.collect(executorService);
  }

  /** Get Kubernetes information in parallel */
  public KubernetesInfo getKubernetesInfo() {
    return kubernetesCollector.collect(executorService);
  }

  /** Get Kafka information in parallel */
  public KafkaInfo getKafkaInfo() {
    return kafkaCollector.collect(executorService);
  }

  /** Get all system properties with metadata */
  public SystemPropertiesInfo getSystemPropertiesInfo() {
    return propertiesCollector.collect();
  }

  /** Get complete system information - all components in parallel */
  public CompleteSystemInfo getAllSystemInfo() {
    List<CompletableFuture<?>> futures =
        Arrays.asList(
            CompletableFuture.supplyAsync(this::getSpringComponentsInfo, executorService),
            CompletableFuture.supplyAsync(this::getStorageInfo, executorService),
            CompletableFuture.supplyAsync(this::getKubernetesInfo, executorService),
            CompletableFuture.supplyAsync(this::getKafkaInfo, executorService),
            CompletableFuture.supplyAsync(this::getSystemPropertiesInfo, executorService));

    try {
      CompletableFuture<Void> allFutures =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      allFutures.get(30, TimeUnit.SECONDS);

      return CompleteSystemInfo.builder()
          .springComponents((SpringComponentsInfo) futures.get(0).getNow(null))
          .storage((StorageInfo) futures.get(1).getNow(null))
          .kubernetes((KubernetesInfo) futures.get(2).getNow(null))
          .kafka((KafkaInfo) futures.get(3).getNow(null))
          .properties((SystemPropertiesInfo) futures.get(4).getNow(null))
          .build();
    } catch (Exception e) {
      log.error("Error collecting complete system info", e);
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
