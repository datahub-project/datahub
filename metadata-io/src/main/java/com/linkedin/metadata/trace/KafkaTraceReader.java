package com.linkedin.metadata.trace;

import static io.datahubproject.metadata.context.TraceContext.TELEMETRY_TRACE_KEY;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.v1.models.TraceStorageStatus;
import io.datahubproject.openapi.v1.models.TraceWriteStatus;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.SchemaException;

@Slf4j
@SuperBuilder
public abstract class KafkaTraceReader<T extends RecordTemplate> {
  private final AdminClient adminClient;
  private final Supplier<Consumer<String, GenericRecord>> consumerSupplier;
  private final int pollDurationMs;
  private final int pollMaxAttempts;

  @Nonnull private final ExecutorService executorService;
  private final long timeoutSeconds;

  private final Cache<String, TopicPartition> topicPartitionCache =
      Caffeine.newBuilder()
          .maximumSize(1_000) // Maximum number of entries
          .expireAfterWrite(Duration.ofHours(1)) // expire entries after 1 hour
          .build();
  private final Cache<TopicPartition, OffsetAndMetadata> offsetCache =
      Caffeine.newBuilder()
          .maximumSize(100) // unlikely to have more than 100 partitions
          .expireAfterWrite(Duration.ofMinutes(5)) // Shorter expiry for offsets
          .build();

  public KafkaTraceReader(
      AdminClient adminClient,
      Supplier<Consumer<String, GenericRecord>> consumerSupplier,
      int pollDurationMillis,
      int pollMaxAttempts,
      ExecutorService executorService,
      long timeoutSeconds) {
    this.adminClient = adminClient;
    this.consumerSupplier = consumerSupplier;
    this.pollDurationMs = pollDurationMillis;
    this.pollMaxAttempts = pollMaxAttempts;
    this.executorService = executorService;
    this.timeoutSeconds = timeoutSeconds;
  }

  @Nonnull
  protected abstract String getTopicName();

  @Nullable
  protected abstract String getConsumerGroupId();

  public abstract Optional<T> read(@Nullable GenericRecord genericRecord);

  protected abstract Optional<Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>
      matchConsumerRecord(
          ConsumerRecord<String, GenericRecord> consumerRecord, String traceId, String aspectName);

  /**
   * Determines the write status of a trace by comparing consumer offset with message offset.
   *
   * @return PENDING if the message exists but hasn't been consumed yet, UNKNOWN if no consumer
   *     offset exists, ERROR in other cases
   */
  public Map<Urn, Map<String, TraceStorageStatus>> tracePendingStatuses(
      Map<Urn, List<String>> urnAspectPairs, String traceId, Long traceTimestampMillis) {
    return tracePendingStatuses(urnAspectPairs, traceId, traceTimestampMillis, false);
  }

  public Map<Urn, Map<String, TraceStorageStatus>> tracePendingStatuses(
      Map<Urn, List<String>> urnAspectPairs,
      String traceId,
      Long traceTimestampMillis,
      boolean skipCache) {

    List<CompletableFuture<Map.Entry<Urn, Map<String, TraceStorageStatus>>>> futures =
        urnAspectPairs.entrySet().stream()
            .map(
                entry ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          try {
                            Map<String, TraceStorageStatus> result =
                                tracePendingStatuses(
                                    entry.getKey(),
                                    entry.getValue(),
                                    traceId,
                                    traceTimestampMillis,
                                    skipCache);
                            return Map.entry(entry.getKey(), result);
                          } catch (Exception e) {
                            log.error(
                                "Error processing trace status for URN: {}", entry.getKey(), e);
                            return Map.entry(
                                entry.getKey(), Collections.<String, TraceStorageStatus>emptyMap());
                          }
                        },
                        executorService))
            .collect(Collectors.toList());

    try {
      List<Map.Entry<Urn, Map<String, TraceStorageStatus>>> results =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
              .thenApply(
                  v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()))
              .get(timeoutSeconds, TimeUnit.SECONDS);

      return results.stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> existing));
    } catch (Exception e) {
      log.error("Error processing parallel trace status requests", e);
      throw new RuntimeException("Failed to process parallel trace status requests", e);
    }
  }

  /**
   * Find messages in the kafka topic by urn, aspect names, and trace id using the timestamp to seek
   * to the expected location.
   *
   * @return Map of aspect name to matching record pair, containing only the aspects that were found
   */
  public Map<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>>
      findMessages(
          Map<Urn, List<String>> urnAspectPairs, String traceId, Long traceTimestampMillis) {

    List<
            CompletableFuture<
                Map.Entry<
                    Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>>>>
        futures =
            urnAspectPairs.entrySet().stream()
                .map(
                    entry ->
                        CompletableFuture.supplyAsync(
                            () -> {
                              try {
                                Map<
                                        String,
                                        Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>
                                    result =
                                        findMessages(
                                            entry.getKey(),
                                            entry.getValue(),
                                            traceId,
                                            traceTimestampMillis);
                                return Map.entry(entry.getKey(), result);
                              } catch (Exception e) {
                                log.error("Error processing trace for URN: {}", entry.getKey(), e);
                                return Map.entry(
                                    entry.getKey(),
                                    Collections
                                        .<String,
                                            Pair<
                                                ConsumerRecord<String, GenericRecord>,
                                                SystemMetadata>>
                                            emptyMap());
                              }
                            },
                            executorService))
                .collect(Collectors.toList());

    try {
      List<Map.Entry<Urn, Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>>>>
          results =
              CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                  .thenApply(
                      v ->
                          futures.stream()
                              .map(CompletableFuture::join)
                              .collect(Collectors.toList()))
                  .get(timeoutSeconds, TimeUnit.SECONDS);

      return results.stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> existing));
    } catch (Exception e) {
      log.error("Error processing parallel trace requests", e);
      throw new RuntimeException("Failed to process parallel trace requests", e);
    }
  }

  private Map<String, TraceStorageStatus> tracePendingStatuses(
      Urn urn,
      Collection<String> aspectNames,
      String traceId,
      Long traceTimestampMillis,
      boolean skipCache) {
    try {
      TopicPartition topicPartition = getTopicPartition(urn);
      Optional<OffsetAndMetadata> offsetMetadata = getOffsetAndMetadata(topicPartition, skipCache);
      if (offsetMetadata.isEmpty()) {
        log.warn("No consumer offset to compare with.");
        return aspectNames.stream()
            .collect(
                Collectors.toMap(
                    aspectName -> aspectName,
                    aspectName ->
                        TraceStorageStatus.ok(
                            TraceWriteStatus.UNKNOWN, "Missing consumer offsets.")));
      }

      Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> messages =
          findMessages(urn, aspectNames, traceId, traceTimestampMillis);

      return aspectNames.stream()
          .collect(
              Collectors.toMap(
                  aspectName -> aspectName,
                  aspectName -> {
                    Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata> message =
                        messages.get(aspectName);
                    if (message != null
                        && offsetMetadata.get().offset() < message.getFirst().offset()) {
                      return TraceStorageStatus.ok(
                          TraceWriteStatus.PENDING, "Consumer has not processed offset.");
                    }
                    return TraceStorageStatus.fail(
                        TraceWriteStatus.ERROR, "Consumer has processed past the offset.");
                  }));
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the offset metadata for a specific TopicPartition from the consumer group. This method is
   * now the primary interface for offset lookup and uses caching.
   */
  private Optional<OffsetAndMetadata> getOffsetAndMetadata(
      TopicPartition topicPartition, boolean skipCache) {
    if (skipCache) {
      offsetCache.invalidate(topicPartition);
    }

    return Optional.ofNullable(
        offsetCache.get(
            topicPartition,
            tp -> {
              final String consumerGroupId = Objects.requireNonNull(getConsumerGroupId());

              try {
                ListConsumerGroupOffsetsResult offsetsResult =
                    adminClient.listConsumerGroupOffsets(consumerGroupId);

                if (offsetsResult == null) {
                  log.error("Failed to get consumer group offsets for group: {}", consumerGroupId);
                  return null;
                }

                Map<TopicPartition, OffsetAndMetadata> offsets =
                    offsetsResult.partitionsToOffsetAndMetadata().get();

                if (offsets == null) {
                  log.error("Null offsets returned for consumer group: {}", consumerGroupId);
                  return null;
                }

                OffsetAndMetadata offsetAndMetadata = offsets.get(tp);
                if (offsetAndMetadata == null) {
                  log.warn(
                      "No committed offset found for Topic: {}, Partition: {}, Group: {}",
                      tp.topic(),
                      tp.partition(),
                      consumerGroupId);
                  return null;
                }

                log.debug(
                    "Found offset metadata {} for Topic: {}, Partition: {}, Group: {}",
                    offsetAndMetadata,
                    tp.topic(),
                    tp.partition(),
                    consumerGroupId);

                return offsetAndMetadata;
              } catch (SchemaException e) {
                log.error("Schema error when fetching consumer group offsets", e);
                return null;
              } catch (Exception e) {
                log.error("Error fetching consumer group offsets", e);
                return null;
              }
            }));
  }

  private Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> findMessages(
      Urn urn, Collection<String> aspectNames, String traceId, Long traceTimestampMillis)
      throws ExecutionException, InterruptedException {

    TopicPartition topicPartition = getTopicPartition(urn);

    try (Consumer<String, GenericRecord> consumer = consumerSupplier.get()) {
      // Assign the partition we want to read from
      consumer.assign(Collections.singleton(topicPartition));

      // Get offset for timestamp
      OffsetAndTimestamp offsetAndTimestamp =
          getOffsetByTime(consumer, topicPartition, traceTimestampMillis);

      if (offsetAndTimestamp == null) {
        log.debug(
            "No offset found for timestamp {} in partition {}",
            traceTimestampMillis,
            topicPartition);
        return Collections.emptyMap();
      }

      // Seek to the offset for the timestamp
      consumer.seek(topicPartition, offsetAndTimestamp.offset());
      log.debug(
          "Seeking to timestamp-based offset {} for partition {}",
          offsetAndTimestamp.offset(),
          topicPartition);

      // Poll with a maximum number of attempts
      int attempts = 0;
      long lastProcessedOffset = -1;
      Map<String, Pair<ConsumerRecord<String, GenericRecord>, SystemMetadata>> results =
          new HashMap<>();

      while (attempts < pollMaxAttempts && results.size() < aspectNames.size()) {
        var records = consumer.poll(java.time.Duration.ofMillis(pollDurationMs));
        attempts++;

        if (records.isEmpty()) {
          break;
        }

        // Check if we're making progress
        long currentOffset = consumer.position(topicPartition);
        if (currentOffset == lastProcessedOffset) {
          break;
        }
        lastProcessedOffset = currentOffset;

        // Process records for each aspect name we haven't found yet
        for (String aspectName : aspectNames) {
          if (!results.containsKey(aspectName)) {
            var matchingRecord =
                records.records(topicPartition).stream()
                    .filter(record -> record.key().equals(urn.toString()))
                    .flatMap(record -> matchConsumerRecord(record, traceId, aspectName).stream())
                    .findFirst();

            matchingRecord.ifPresent(pair -> results.put(aspectName, pair));
          }
        }
      }

      return results;
    }
  }

  protected static boolean traceIdMatch(@Nullable SystemMetadata systemMetadata, String traceId) {
    return systemMetadata != null
        && systemMetadata.getProperties() != null
        && traceId.equals(systemMetadata.getProperties().get(TELEMETRY_TRACE_KEY));
  }

  private TopicPartition getTopicPartition(Urn urn) {
    return topicPartitionCache.get(
        urn.toString(),
        key -> {
          try {
            DefaultPartitioner partitioner = new DefaultPartitioner();

            TopicDescription topicDescription =
                adminClient
                    .describeTopics(Collections.singletonList(getTopicName()))
                    .all()
                    .get()
                    .get(getTopicName());

            if (topicDescription == null) {
              throw new IllegalStateException("Topic " + getTopicName() + " not found");
            }

            List<PartitionInfo> partitions =
                topicDescription.partitions().stream()
                    .map(
                        p ->
                            new PartitionInfo(
                                getTopicName(),
                                p.partition(),
                                p.leader(),
                                p.replicas().toArray(new Node[0]),
                                p.isr().toArray(new Node[0]),
                                p.replicas().toArray(new Node[0])))
                    .collect(Collectors.toList());

            List<Node> nodes =
                partitions.stream()
                    .map(PartitionInfo::leader)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            Cluster cluster =
                new Cluster(
                    null, nodes, partitions, Collections.emptySet(), Collections.emptySet());

            int partition =
                partitioner.partition(getTopicName(), key, key.getBytes(), null, null, cluster);

            return new TopicPartition(getTopicName(), partition);
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get topic partition for " + key, e);
          }
        });
  }

  private static OffsetAndTimestamp getOffsetByTime(
      Consumer<String, GenericRecord> consumer,
      TopicPartition topicPartition,
      Long traceTimestampMillis) {
    // If we have a timestamp, first seek to that approximate location
    Map<TopicPartition, Long> timestampsToSearch =
        Collections.singletonMap(topicPartition, traceTimestampMillis);

    return consumer.offsetsForTimes(timestampsToSearch).get(topicPartition);
  }
}
