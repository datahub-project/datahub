package com.linkedin.gms.factory.system_info;

import com.linkedin.metadata.system_info.SystemInfoDtos;
import com.linkedin.metadata.system_info.SystemInfoService;
import com.linkedin.metadata.system_info.collectors.KafkaCollector;
import com.linkedin.metadata.system_info.collectors.KubernetesCollector;
import com.linkedin.metadata.system_info.collectors.PropertiesCollector;
import com.linkedin.metadata.system_info.collectors.SpringComponentsCollector;
import com.linkedin.metadata.system_info.collectors.StorageSystemsCollector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan("com.linkedin.metadata.system_info.collectors")
public class SystemInfoServiceFactory {

  @Autowired private ApplicationContext applicationContext;

  /** Provide a no-op KubernetesCollector when KubernetesClient is not available */
  @Bean
  @ConditionalOnMissingBean(io.fabric8.kubernetes.client.KubernetesClient.class)
  public KubernetesCollector noOpKubernetesCollector() {
    return new KubernetesCollector(null) {
      @Override
      public SystemInfoDtos.KubernetesInfo collect(
          java.util.concurrent.ExecutorService executorService) {
        return SystemInfoDtos.KubernetesInfo.builder().runningInKubernetes(false).build();
      }
    };
  }

  /** Provide a no-op StorageSystemsCollector when storage clients are not available */
  @Bean
  @ConditionalOnMissingBean(StorageSystemsCollector.class)
  public StorageSystemsCollector noOpStorageSystemsCollector() {
    return new StorageSystemsCollector() {
      @Override
      public SystemInfoDtos.StorageInfo collect(
          java.util.concurrent.ExecutorService executorService) {
        return SystemInfoDtos.StorageInfo.builder()
            .mysql(
                SystemInfoDtos.StorageSystemInfo.builder()
                    .type("MySQL")
                    .available(false)
                    .errorMessage("MySQL not configured")
                    .build())
            .postgres(
                SystemInfoDtos.StorageSystemInfo.builder()
                    .type("PostgreSQL")
                    .available(false)
                    .errorMessage("PostgreSQL not configured")
                    .build())
            .elasticsearch(
                SystemInfoDtos.StorageSystemInfo.builder()
                    .type("Elasticsearch")
                    .available(false)
                    .errorMessage("Elasticsearch not configured")
                    .build())
            .opensearch(
                SystemInfoDtos.StorageSystemInfo.builder()
                    .type("OpenSearch")
                    .available(false)
                    .errorMessage("OpenSearch not configured")
                    .build())
            .neo4j(
                SystemInfoDtos.StorageSystemInfo.builder()
                    .type("Neo4j")
                    .available(false)
                    .errorMessage("Neo4j not configured")
                    .build())
            .cassandra(
                SystemInfoDtos.StorageSystemInfo.builder()
                    .type("Cassandra")
                    .available(false)
                    .errorMessage("Cassandra not configured")
                    .build())
            .build();
      }
    };
  }

  /** Provide a no-op KafkaCollector when KafkaProperties is not available */
  @Bean
  @ConditionalOnMissingBean(org.springframework.boot.autoconfigure.kafka.KafkaProperties.class)
  public KafkaCollector noOpKafkaCollector() {
    return new KafkaCollector() {
      @Override
      public SystemInfoDtos.KafkaInfo collect(
          java.util.concurrent.ExecutorService executorService) {
        return SystemInfoDtos.KafkaInfo.builder()
            .available(false)
            .errorMessage("Kafka not configured")
            .build();
      }
    };
  }

  @Bean
  public SystemInfoService systemInfoService(
      KafkaCollector kafkaCollector,
      KubernetesCollector kubernetesCollector,
      SpringComponentsCollector springComponentsCollector,
      StorageSystemsCollector storageSystemsCollector,
      PropertiesCollector propertiesCollector) {
    SystemInfoService service =
        new SystemInfoService(
            springComponentsCollector,
            storageSystemsCollector,
            kubernetesCollector,
            kafkaCollector,
            propertiesCollector);
    applicationContext.getAutowireCapableBeanFactory().autowireBean(service);
    return service;
  }
}
