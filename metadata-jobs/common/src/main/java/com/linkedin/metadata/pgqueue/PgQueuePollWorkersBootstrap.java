package com.linkedin.metadata.pgqueue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.PgQueueConsumerInitializationManager;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PgQueueConsumerPartitionSharding;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Flattens all {@link PgQueuePollerSource} beans and starts pgQueue poll threads: each registration
 * may expand to multiple threads when {@code postgres.pgQueue.topicDefaults.consumerConcurrency} or
 * per-topic overrides exceed one (same-JVM partition sharding via {@link
 * com.linkedin.metadata.queue.PgQueueConsumerPartitionSharding}).
 */
@Slf4j
@Component
@Conditional(PgQueueMessagingTransportCondition.class)
public class PgQueuePollWorkersBootstrap implements DisposableBean {

  /** Per-thread join budget so shutdown does not hang indefinitely with many workers. */
  private static final long SHUTDOWN_JOIN_MILLIS_PER_THREAD = 15_000L;

  private final List<PgQueuePollWorker> workers = new ArrayList<>();
  private final List<Thread> workerThreads = new ArrayList<>();

  @Autowired
  public PgQueuePollWorkersBootstrap(
      List<PgQueuePollerSource> sources,
      MetadataQueueStore metadataQueueStore,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      ConfigurationProvider configurationProvider,
      @Autowired(required = false) @Nullable @Qualifier("pgQueueConsumerAvroDeserializer")
          Deserializer<GenericRecord> avroDeserializer,
      PgQueueConsumerInitializationManager lifecycle) {
    PgQueueSetupOptions pgQueueOpts =
        postgresSqlSetupProperties.buildPgQueueOptions(configurationProvider.getKafka());
    if (pgQueueOpts == null) {
      throw new IllegalStateException(
          "PgQueuePollWorkersBootstrap requires pgQueue enabled options");
    }
    List<PgQueuePollerRegistration> registrations =
        sources.stream().flatMap(s -> s.registrations()).toList();
    int totalThreads =
        registrations.stream()
            .mapToInt(
                r -> PgQueueConsumerPartitionSharding.workerShardCount(r.topicNames(), pgQueueOpts))
            .sum();
    log.info(
        "PgQueue poll workers: {} logical consumer registration(s), {} OS thread(s)",
        registrations.size(),
        totalThreads);
    for (PgQueuePollerRegistration reg : registrations) {
      int shards = PgQueueConsumerPartitionSharding.workerShardCount(reg.topicNames(), pgQueueOpts);
      for (int i = 0; i < shards; i++) {
        int shardIndex = i;
        PgQueuePollWorker worker =
            new PgQueuePollWorker(
                reg,
                metadataQueueStore,
                postgresSqlSetupProperties,
                configurationProvider,
                pgQueueOpts,
                shardIndex,
                shards,
                avroDeserializer);
        workers.add(worker);
        lifecycle.whenStarted(
            () -> {
              String threadLabel =
                  shards > 1 ? reg.threadName() + "-" + shardIndex : reg.threadName();
              Thread t = new Thread(worker, threadLabel);
              t.setDaemon(true);
              synchronized (workerThreads) {
                workerThreads.add(t);
              }
              t.start();
            });
      }
    }
  }

  @Override
  public void destroy() {
    workers.forEach(PgQueuePollWorker::stop);
    List<Thread> threads;
    synchronized (workerThreads) {
      threads = List.copyOf(workerThreads);
    }
    for (Thread t : threads) {
      t.interrupt();
    }
    for (Thread t : threads) {
      try {
        t.join(SHUTDOWN_JOIN_MILLIS_PER_THREAD);
        if (t.isAlive()) {
          log.warn(
              "PgQueue poll worker thread {} still alive after {} ms join; process may exit with"
                  + " in-flight work",
              t.getName(),
              SHUTDOWN_JOIN_MILLIS_PER_THREAD);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Interrupted while joining pgQueue worker {}", t.getName(), e);
      }
    }
    if (threads.stream().anyMatch(Thread::isAlive)) {
      log.warn(
          "{} pgQueue worker thread(s) still running after shutdown join",
          threads.stream().filter(Thread::isAlive).count());
    }
  }

  /** Used by pgQueue poll health checks. */
  public int pollWorkerThreadCount() {
    synchronized (workerThreads) {
      return workerThreads.size();
    }
  }

  public boolean allPollWorkerThreadsAlive() {
    synchronized (workerThreads) {
      if (workerThreads.isEmpty()) {
        return true;
      }
      return workerThreads.stream().allMatch(Thread::isAlive);
    }
  }
}
