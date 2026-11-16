package com.linkedin.gms.factory.event;

import com.linkedin.metadata.config.messaging.KafkaMessagingDisabled;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.inmemory.InMemoryMetadataQueueStore;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * {@code postgres.pgQueue.store=inmemory}: the pgqueue transport backed by a heap store instead of
 * Postgres, for local development and tests. Single-process only. With {@code
 * postgres.pgQueue.inmemorySpoolFile} set, the store is loaded at startup and saved at shutdown so
 * events produced during system-update can reach GMS in the next process. The spool path belongs to
 * one live process: one writes on exit, then the next loads at startup. Overlapping processes must
 * use different paths to avoid overwriting each other's snapshots.
 */
@Slf4j
@Configuration
@KafkaMessagingDisabled
@Conditional(PostgresQueueStoreCondition.InMemory.class)
public class InMemoryMetadataQueueStoreFactory {

  @Bean(destroyMethod = "close")
  public MetadataQueueStore metadataQueueStore(
      @Value("${postgres.pgQueue.inmemorySpoolFile:}") String spoolFile) throws IOException {
    log.warn(
        "postgres.pgQueue.store=inmemory: metadata events are queued in this JVM's heap only. "
            + "Not for multi-node or durable deployments.");
    if (spoolFile != null && !spoolFile.isBlank()) {
      Path path = Paths.get(spoolFile.trim());
      log.warn(
          "Queue spool {} belongs to one live process: one writes on exit, then the next loads "
              + "at startup. Overlapping processes sharing this path overwrite each other's snapshots.",
          path);
      return InMemoryMetadataQueueStore.withSpool(path);
    }
    return new InMemoryMetadataQueueStore();
  }
}
