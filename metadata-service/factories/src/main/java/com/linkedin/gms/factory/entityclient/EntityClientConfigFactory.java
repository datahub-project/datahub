package com.linkedin.gms.factory.entityclient;

import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityClientConfigFactory {

  @Bean
  public EntityClientCacheConfig entityClientCacheConfig(
      @Qualifier("configurationProvider") final ConfigurationProvider configurationProvider) {
    return configurationProvider.getCache().getClient().getEntityClient();
  }

  @Bean
  public EntityClientConfig entityClientConfig(
      final @Value("${entityClient.retryInterval:2}") int retryInterval,
      final @Value("${entityClient.numRetries:3}") int numRetries,
      final @Value("${entityClient.restli.get.batchSize}") int batchGetV2Size,
      final @Value("${entityClient.restli.get.batchConcurrency}") int batchGetV2Concurrency,
      final @Value("${entityClient.restli.get.batchQueueSize}") int batchGetV2QueueSize,
      final @Value("${entityClient.restli.get.batchThreadKeepAlive}") int batchGetV2KeepAlive,
      final @Value("${entityClient.restli.ingest.batchSize}") int batchIngestSize,
      final @Value("${entityClient.restli.ingest.batchConcurrency}") int batchIngestConcurrency,
      final @Value("${entityClient.restli.ingest.batchQueueSize}") int batchIngestQueueSize,
      final @Value("${entityClient.restli.ingest.batchThreadKeepAlive}") int batchIngestKeepAlive) {
    return EntityClientConfig.builder()
        .backoffPolicy(new ExponentialBackoff(retryInterval))
        .retryCount(numRetries)
        .batchGetV2Size(batchGetV2Size)
        .batchGetV2Concurrency(batchGetV2Concurrency)
        .batchGetV2QueueSize(batchGetV2QueueSize)
        .batchGetV2KeepAlive(batchGetV2KeepAlive)
        .batchIngestSize(batchIngestSize)
        .batchIngestConcurrency(batchIngestConcurrency)
        .batchIngestQueueSize(batchIngestQueueSize)
        .batchIngestKeepAlive(batchIngestKeepAlive)
        .build();
  }
}
