package com.linkedin.gms.factory.event;

import com.linkedin.metadata.config.messaging.KafkaMessagingDisabled;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.inmemory.InMemoryMetadataQueueStore;
import java.io.IOException;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@code postgres.pgQueue.store=inmemory}: the pgqueue transport backed by a heap store instead of
 * Postgres, for local development and tests. Single-process only. With {@code
 * postgres.pgQueue.inmemorySpoolFile} set, the store is loaded at startup and saved at shutdown so
 * events produced during system-update can reach GMS in the next process.
 */
@Slf4j
@Configuration
@KafkaMessagingDisabled
@ConditionalOnProperty(
    name = PostgresQueueStoreCondition.PROPERTY,
    havingValue = PostgresQueueStoreCondition.INMEMORY)
public class InMemoryMetadataQueueStoreFactory {

  @Bean(destroyMethod = "close")
  public MetadataQueueStore metadataQueueStore(
      @Value("${postgres.pgQueue.inmemorySpoolFile:}") String spoolFile) throws IOException {
    log.warn(
        "postgres.pgQueue.store=inmemory: metadata events are queued in this JVM's heap only. "
            + "Not for multi-node or durable deployments.");
    if (spoolFile != null && !spoolFile.isBlank()) {
      return InMemoryMetadataQueueStore.withSpool(Paths.get(spoolFile.trim()));
    }
    return new InMemoryMetadataQueueStore();
  }
}
