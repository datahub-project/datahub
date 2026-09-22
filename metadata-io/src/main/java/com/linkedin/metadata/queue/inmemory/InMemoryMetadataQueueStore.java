package com.linkedin.metadata.queue.inmemory;

import com.linkedin.metadata.queue.ConsumerOffsetResetDetail;
import com.linkedin.metadata.queue.ConsumerOffsetResetReport;
import com.linkedin.metadata.queue.ConsumerOffsetResetSpec;
import com.linkedin.metadata.queue.ConsumerRegistrationRow;
import com.linkedin.metadata.queue.EnqueueBatchItem;
import com.linkedin.metadata.queue.MetadataQueueRouting;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PartitionOffsetSkew;
import com.linkedin.metadata.queue.PgQueueContiguousOffset;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueLogPeekRow;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueMessageHeader;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Single-process, heap-backed {@link MetadataQueueStore} with the same topic, partition, lease and
 * consumer-offset semantics as the PostgreSQL store, minus durability. It exists so a DataHub node
 * can run the {@code pgqueue} transport (producer, poll workers and every MCL/MCP/PE consumer
 * unchanged) without Postgres or Kafka: local development, CI and single-node test stacks.
 *
 * <p>Messages are retained until every registered consumer group has committed past them, then
 * dropped by {@link #applyRetention()} (also invoked opportunistically on enqueue). Priorities are
 * honored head-of-line per priority value within a partition, like the Postgres store, but without
 * weighted bands.
 */
@Slf4j
public class InMemoryMetadataQueueStore implements MetadataQueueStore {

  private static final int MAX_PRIORITIES = QueueTopicMetadata.MAX_PRIORITY + 1;

  private volatile Clock clock;
  private final Object lock = new Object();
  private final AtomicLong nextTopicId = new AtomicLong(1);
  private final AtomicLong nextMessageId = new AtomicLong(1);
  private final Map<String, Topic> topicsByName = new ConcurrentHashMap<>();
  private final Map<Long, Topic> topicsById = new ConcurrentHashMap<>();

  public InMemoryMetadataQueueStore() {
    this(Clock.systemUTC());
  }

  public InMemoryMetadataQueueStore(@Nonnull Clock clock) {
    this.clock = clock;
  }

  /** Replace the clock (test-only). Allows time-advance without reflection. */
  public void setClock(@Nonnull Clock clock) {
    this.clock = clock;
  }

  // ---------------------------------------------------------------------------------------------
  // Spool: hand the unconsumed log to the next process (system-update -> GMS) the way a durable
  // queue would. Leases are dropped; committed offsets and consumer registrations are kept.

  @Nullable private java.nio.file.Path spoolFile;

  /** Loads {@code file} now and saves back to it on {@link #close()}. */
  @Nonnull
  public static InMemoryMetadataQueueStore withSpool(@Nonnull java.nio.file.Path file)
      throws java.io.IOException {
    InMemoryMetadataQueueStore store = new InMemoryMetadataQueueStore();
    store.spoolFile = file;
    store.loadFrom(file);
    return store;
  }

  /** Saves to the spool file, if one was configured. Bound as the Spring bean's destroy method. */
  public void close() throws java.io.IOException {
    if (spoolFile != null) {
      saveTo(spoolFile);
    }
  }

  /**
   * Loads a spool written by {@link #saveTo}. Empty or missing file = nothing to load. Meant to run
   * once, before any producer or consumer touches the store.
   */
  public synchronized void loadFrom(@Nonnull java.nio.file.Path file) throws java.io.IOException {
    if (!java.nio.file.Files.exists(file) || java.nio.file.Files.size(file) == 0) {
      return;
    }
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(file.toFile());
    synchronized (lock) {
      int loaded = 0;
      for (com.fasterxml.jackson.databind.JsonNode t : root.path("topics")) {
        Topic topic =
            new Topic(
                nextTopicId.getAndIncrement(),
                t.path("name").asText(),
                t.path("partitionCount").asInt(1),
                t.hasNonNull("defaultContentTypeMime")
                    ? t.path("defaultContentTypeMime").asText()
                    : null);
        topicsByName.put(topic.name, topic);
        topicsById.put(topic.id, topic);
        for (com.fasterxml.jackson.databind.JsonNode c : t.path("consumers")) {
          Instant at = Instant.parse(c.path("registeredAt").asText());
          topic.consumers.put(
              c.path("group").asText(),
              new ConsumerRegistrationRow(c.path("group").asText(), topic.id, at, at));
        }
        for (com.fasterxml.jackson.databind.JsonNode p : t.path("partitions")) {
          Partition partition = topic.partitions.get(p.path("id").asInt());
          partition.nextSeq = p.path("nextSeq").asLong(1);
          p.path("committedOffsets")
              .fields()
              .forEachRemaining(
                  e -> partition.committedOffsets.put(e.getKey(), e.getValue().asLong()));
          for (com.fasterxml.jackson.databind.JsonNode m : p.path("messages")) {
            List<QueueMessageHeader> headers = new ArrayList<>();
            for (com.fasterxml.jackson.databind.JsonNode h : m.path("headers")) {
              headers.add(
                  new QueueMessageHeader(
                      h.path("key").asText(),
                      java.util.Base64.getDecoder().decode(h.path("value").asText())));
            }
            long messageId = nextMessageId.getAndIncrement();
            QueueMessageHandle handle =
                new QueueMessageHandle(
                    messageId,
                    Instant.parse(m.path("enqueuedAt").asText()),
                    topic.id,
                    partition.id,
                    m.path("seq").asLong());
            Message message =
                new Message(
                    handle,
                    m.path("routingKey").asText(),
                    m.path("priority").asInt(QueueTopicMetadata.DEFAULT_PRIORITY),
                    java.util.Base64.getDecoder().decode(m.path("payload").asText()),
                    m.hasNonNull("contentType")
                        ? Optional.of(m.path("contentType").asText())
                        : Optional.empty(),
                    headers,
                    PgQueuePayloadCompression.valueOf(m.path("compression").asText("NONE")));
            for (com.fasterxml.jackson.databind.JsonNode g : m.path("ackedBy")) {
              message.leases.put(g.asText(), new Lease("", Instant.MAX, true));
            }
            partition.messages.add(message);
            partition.byId.put(messageId, message);
            loaded++;
          }
        }
      }
      log.info(
          "Loaded queue spool {}: {} topic(s), {} message(s)", file, topicsById.size(), loaded);
    }
  }

  /** Writes every topic, unconsumed message, ack and committed offset to {@code file}. */
  public synchronized void saveTo(@Nonnull java.nio.file.Path file) throws java.io.IOException {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    com.fasterxml.jackson.databind.node.ObjectNode root = mapper.createObjectNode();
    com.fasterxml.jackson.databind.node.ArrayNode topics = root.putArray("topics");
    int saved = 0;
    synchronized (lock) {
      for (Topic topic : topicsById.values()) {
        com.fasterxml.jackson.databind.node.ObjectNode t = topics.addObject();
        t.put("name", topic.name).put("partitionCount", topic.partitionCount);
        if (topic.defaultContentTypeMime != null) {
          t.put("defaultContentTypeMime", topic.defaultContentTypeMime);
        }
        com.fasterxml.jackson.databind.node.ArrayNode consumers = t.putArray("consumers");
        for (ConsumerRegistrationRow row : topic.consumers.values()) {
          consumers
              .addObject()
              .put("group", row.consumerGroup())
              .put("registeredAt", row.registeredAt().toString());
        }
        com.fasterxml.jackson.databind.node.ArrayNode partitions = t.putArray("partitions");
        for (Partition p : topic.partitions) {
          com.fasterxml.jackson.databind.node.ObjectNode pn = partitions.addObject();
          pn.put("id", p.id).put("nextSeq", p.nextSeq);
          com.fasterxml.jackson.databind.node.ObjectNode offsets = pn.putObject("committedOffsets");
          p.committedOffsets.forEach(offsets::put);
          com.fasterxml.jackson.databind.node.ArrayNode messages = pn.putArray("messages");
          for (Message m : p.messages) {
            com.fasterxml.jackson.databind.node.ObjectNode mn = messages.addObject();
            mn.put("seq", m.handle.enqueueSeq())
                .put("enqueuedAt", m.handle.enqueuedAt().toString())
                .put("routingKey", m.routingKey)
                .put("priority", m.priority)
                .put("payload", java.util.Base64.getEncoder().encodeToString(m.payload))
                .put("compression", m.payloadCompression.name());
            m.contentType.ifPresent(ct -> mn.put("contentType", ct));
            com.fasterxml.jackson.databind.node.ArrayNode headers = mn.putArray("headers");
            for (QueueMessageHeader h : m.headers) {
              headers
                  .addObject()
                  .put("key", h.key())
                  .put("value", java.util.Base64.getEncoder().encodeToString(h.value()));
            }
            com.fasterxml.jackson.databind.node.ArrayNode acked = mn.putArray("ackedBy");
            m.leases.forEach(
                (group, lease) -> {
                  if (lease.acked) {
                    acked.add(group);
                  }
                });
            saved++;
          }
        }
      }
    }
    java.nio.file.Files.createDirectories(file.toAbsolutePath().getParent());
    java.nio.file.Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
    mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), root);
    java.nio.file.Files.move(
        tmp,
        file,
        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    log.info("Saved queue spool {}: {} topic(s), {} message(s)", file, topicsById.size(), saved);
  }

  // ---------------------------------------------------------------------------------------------
  // Topics

  @Nonnull
  @Override
  public Optional<QueueTopicMetadata> fetchTopic(@Nonnull String topicName) {
    Topic topic = topicsByName.get(topicName);
    return topic == null ? Optional.empty() : Optional.of(topic.metadata());
  }

  @Override
  public long ensureTopic(@Nonnull String topicName, @Nonnull QueueTopicDefaults defaults) {
    return topicsByName.computeIfAbsent(
            topicName,
            name -> {
              Topic topic =
                  new Topic(
                      nextTopicId.getAndIncrement(),
                      name,
                      Math.max(1, defaults.partitionCount()),
                      defaults.defaultContentTypeMime());
              topicsById.put(topic.id, topic);
              log.info(
                  "In-memory queue topic created: {} (id={}, partitions={})",
                  name,
                  topic.id,
                  topic.partitionCount);
              return topic;
            })
        .id;
  }

  // ---------------------------------------------------------------------------------------------
  // Enqueue

  @Nonnull
  @Override
  public QueueMessageHandle enqueue(
      @Nonnull String topicName,
      @Nonnull String routingKey,
      @Nonnull QueueTopicDefaults defaults,
      int priority,
      @Nonnull byte[] payload,
      @Nonnull Optional<String> contentType,
      @Nonnull List<QueueMessageHeader> headers,
      @Nonnull PgQueuePayloadCompression payloadCompression) {
    QueueTopicMetadata.validatePriority(priority);
    Topic topic = topicsById.get(ensureTopic(topicName, defaults));
    synchronized (lock) {
      return topic.append(
          nextMessageId.getAndIncrement(),
          clock.instant(),
          routingKey,
          priority,
          payload,
          contentType,
          headers,
          payloadCompression);
    }
  }

  @Nonnull
  @Override
  public List<QueueMessageHandle> enqueueBatch(
      @Nonnull List<EnqueueBatchItem> items, @Nonnull QueueTopicDefaults defaults) {
    synchronized (lock) {
      List<QueueMessageHandle> handles = new ArrayList<>(items.size());
      for (EnqueueBatchItem item : items) {
        handles.add(
            enqueue(
                item.topicName(),
                item.routingKey(),
                defaults,
                item.priority(),
                item.payload(),
                item.contentType(),
                item.headers(),
                item.payloadCompression()));
      }
      return handles;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Log inspection

  @Nonnull
  @Override
  public Map<Integer, Long> partitionNextExclusiveSeqs(long topicId, int partitionCount) {
    Map<Integer, Long> out = new LinkedHashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      out.put(i, 1L);
    }
    Topic topic = topicsById.get(topicId);
    if (topic != null) {
      synchronized (lock) {
        for (Partition p : topic.partitions) {
          out.put(p.id, p.maxSeq() + 1);
        }
      }
    }
    return out;
  }

  @Nonnull
  @Override
  public Map<Integer, Long> partitionMaxEnqueueSeqs(long topicId, int partitionCount) {
    Map<Integer, Long> out = new LinkedHashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      out.put(i, 0L);
    }
    Topic topic = topicsById.get(topicId);
    if (topic != null) {
      synchronized (lock) {
        for (Partition p : topic.partitions) {
          out.put(p.id, p.maxSeq());
        }
      }
    }
    return out;
  }

  @Nonnull
  @Override
  public List<PartitionOffsetSkew> detectOffsetAheadOfLog(
      @Nonnull String consumerGroup, long topicId, int partitionCount) {
    List<PartitionOffsetSkew> skewed = new ArrayList<>();
    Topic topic = topicsById.get(topicId);
    if (topic == null) {
      return skewed;
    }
    synchronized (lock) {
      for (Partition p : topic.partitions) {
        long committed = p.committedOffset(consumerGroup);
        long maxSeq = p.maxSeq();
        if (committed > maxSeq) {
          skewed.add(
              PartitionOffsetSkew.builder()
                  .consumerGroup(consumerGroup)
                  .topicId(topicId)
                  .topicName(topic.name)
                  .partitionId(p.id)
                  .committedOffset(committed)
                  .maxSeq(maxSeq)
                  .aheadBy(committed - maxSeq)
                  .build());
        }
      }
    }
    return skewed;
  }

  @Nonnull
  @Override
  public OptionalLong minEnqueueSeqAtOrAfter(
      long topicId, int partitionId, @Nonnull Instant minEnqueuedAt) {
    Partition p = partition(topicId, partitionId);
    if (p == null) {
      return OptionalLong.empty();
    }
    synchronized (lock) {
      return p.messages.stream()
          .filter(m -> !m.handle.enqueuedAt().isBefore(minEnqueuedAt))
          .mapToLong(m -> m.handle.enqueueSeq())
          .min();
    }
  }

  @Nonnull
  @Override
  public OptionalLong minEnqueueSeq(long topicId, int partitionId) {
    Partition p = partition(topicId, partitionId);
    if (p == null) {
      return OptionalLong.empty();
    }
    synchronized (lock) {
      return p.messages.stream().mapToLong(m -> m.handle.enqueueSeq()).min();
    }
  }

  @Nonnull
  @Override
  public List<QueueLogPeekRow> peekTopicLog(
      long topicId, @Nonnull Map<Integer, Long> partitionToMinExclusiveSeq, int limit) {
    Topic topic = topicsById.get(topicId);
    if (topic == null || limit <= 0 || partitionToMinExclusiveSeq.isEmpty()) {
      return List.of();
    }
    synchronized (lock) {
      return topic.partitions.stream()
          .filter(p -> partitionToMinExclusiveSeq.containsKey(p.id))
          .flatMap(
              p ->
                  p.messages.stream()
                      .filter(
                          m ->
                              m.handle.enqueueSeq()
                                  >= partitionToMinExclusiveSeq.getOrDefault(p.id, 0L)))
          .sorted(Comparator.comparingLong(m -> m.handle.id()))
          .limit(limit)
          .map(
              m ->
                  new QueueLogPeekRow(
                      m.handle,
                      m.priority,
                      m.payload,
                      m.contentType,
                      m.payloadCompression,
                      m.headers,
                      m.routingKey))
          .toList();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Consumption

  @Nonnull
  @Override
  public List<QueueReceivedMessage> receiveBatchForGroup(
      @Nonnull String consumerGroup,
      long topicId,
      @Nonnull List<Integer> partitionIds,
      @Nonnull String lockOwner,
      @Nonnull Duration visibilityTimeout,
      int maxMessages) {
    Topic topic = topicsById.get(topicId);
    if (topic == null || maxMessages <= 0 || partitionIds.isEmpty()) {
      return List.of();
    }
    List<QueueReceivedMessage> out = new ArrayList<>();
    synchronized (lock) {
      Instant now = clock.instant();
      Instant lockedUntil = now.plus(visibilityTimeout);
      for (int partitionId : partitionIds) {
        if (out.size() >= maxMessages) {
          break;
        }
        Partition p = topic.partition(partitionId);
        if (p == null) {
          continue;
        }
        long committed = p.committedOffset(consumerGroup);
        // Head-of-line per priority value: a leased message blocks later ones of the same
        // priority, so a group never observes a later seq before an earlier in-flight one.
        for (int priority = 0; priority < MAX_PRIORITIES && out.size() < maxMessages; priority++) {
          for (Message m : p.messages) {
            if (out.size() >= maxMessages) {
              break;
            }
            if (m.priority != priority || m.handle.enqueueSeq() <= committed) {
              continue;
            }
            Lease lease = m.leases.get(consumerGroup);
            if (lease != null && lease.acked) {
              continue;
            }
            // Postgres acquires a lease only once locked_until has passed, whoever held it; handing
            // a live lease back to its own owner re-delivered a failed batch in a tight loop.
            if (lease != null && lease.lockedUntil.isAfter(now)) {
              break;
            }
            m.leases.put(consumerGroup, new Lease(lockOwner, lockedUntil, false));
            out.add(
                new QueueReceivedMessage(
                    m.handle,
                    m.priority,
                    m.payload,
                    m.contentType.or(() -> Optional.ofNullable(topic.defaultContentTypeMime)),
                    m.payloadCompression,
                    m.headers,
                    m.routingKey,
                    lockOwner));
          }
        }
      }
    }
    return out;
  }

  @Override
  public int commitForGroup(
      @Nonnull String consumerGroup,
      @Nonnull List<QueueMessageHandle> handles,
      boolean updateConsumerOffset) {
    if (handles.isEmpty()) {
      return 0;
    }
    int marked = 0;
    synchronized (lock) {
      Map<Partition, Boolean> touched = new HashMap<>();
      for (QueueMessageHandle h : handles) {
        Partition p = partition(h.topicId(), h.partitionId());
        Message m = p == null ? null : p.byId.get(h.id());
        if (m == null) {
          continue;
        }
        m.leases.put(consumerGroup, new Lease(lockOwnerOf(m, consumerGroup), Instant.MAX, true));
        marked++;
        touched.put(p, Boolean.TRUE);
      }
      if (updateConsumerOffset) {
        for (Partition p : touched.keySet()) {
          long current = p.committedOffset(consumerGroup);
          List<Long> ackedSeqs =
              p.messages.stream()
                  .filter(m -> m.handle.enqueueSeq() > current)
                  .filter(m -> m.isAckedBy(consumerGroup))
                  .map(m -> m.handle.enqueueSeq())
                  .toList();
          long next = PgQueueContiguousOffset.advanceWatermark(current, ackedSeqs);
          if (next > current) {
            p.committedOffsets.put(consumerGroup, next);
          }
        }
      }
    }
    return marked;
  }

  @Override
  public int extendVisibilityForGroup(
      @Nonnull String consumerGroup,
      @Nonnull List<QueueMessageHandle> handles,
      @Nonnull String lockOwner,
      @Nonnull Duration extendBy) {
    int updated = 0;
    synchronized (lock) {
      Instant until = clock.instant().plus(extendBy);
      for (QueueMessageHandle h : handles) {
        Partition p = partition(h.topicId(), h.partitionId());
        Message m = p == null ? null : p.byId.get(h.id());
        if (m == null) {
          continue;
        }
        Lease lease = m.leases.get(consumerGroup);
        if (lease != null && !lease.acked && lease.owner.equals(lockOwner)) {
          m.leases.put(consumerGroup, new Lease(lockOwner, until, false));
          updated++;
        }
      }
    }
    return updated;
  }

  @Override
  public long getCommittedOffset(@Nonnull String consumerGroup, long topicId, int partitionId) {
    Partition p = partition(topicId, partitionId);
    if (p == null) {
      return 0L;
    }
    synchronized (lock) {
      return p.committedOffset(consumerGroup);
    }
  }

  @Override
  public void registerConsumer(@Nonnull String consumerGroup, long topicId) {
    Topic topic = topicsById.get(topicId);
    if (topic == null) {
      throw new IllegalArgumentException("Unknown topic id " + topicId);
    }
    Instant now = clock.instant();
    topic.consumers.compute(
        consumerGroup,
        (g, existing) ->
            existing == null
                ? new ConsumerRegistrationRow(g, topicId, now, now)
                : new ConsumerRegistrationRow(g, topicId, existing.registeredAt(), now));
  }

  @Nonnull
  @Override
  public List<ConsumerRegistrationRow> listRegisteredConsumers(long topicId) {
    Topic topic = topicsById.get(topicId);
    return topic == null ? List.of() : new ArrayList<>(topic.consumers.values());
  }

  @Override
  public boolean unregisterConsumer(@Nonnull String consumerGroup, long topicId) {
    Topic topic = topicsById.get(topicId);
    return topic != null && topic.consumers.remove(consumerGroup) != null;
  }

  @Nonnull
  @Override
  public ConsumerOffsetResetReport resetConsumerOffsets(@Nonnull ConsumerOffsetResetSpec spec) {
    List<ConsumerOffsetResetDetail> resets = new ArrayList<>();
    synchronized (lock) {
      for (Topic topic : topicsById.values()) {
        if (spec.getTopicName() != null && !spec.getTopicName().equals(topic.name)) {
          continue;
        }
        for (Partition p : topic.partitions) {
          if (spec.getPartitionId() != null && spec.getPartitionId() != p.id) {
            continue;
          }
          for (Map.Entry<String, Long> e : new ArrayList<>(p.committedOffsets.entrySet())) {
            if (spec.getConsumerGroup() != null && !spec.getConsumerGroup().equals(e.getKey())) {
              continue;
            }
            long maxSeq = p.maxSeq();
            if (spec.isOnlyStuckAhead() && e.getValue() <= maxSeq) {
              continue;
            }
            p.committedOffsets.put(e.getKey(), maxSeq);
            resets.add(
                ConsumerOffsetResetDetail.builder()
                    .consumerGroup(e.getKey())
                    .topicName(topic.name)
                    .partitionId(p.id)
                    .previousOffset(e.getValue())
                    .newOffset(maxSeq)
                    .maxSeq(maxSeq)
                    .build());
          }
        }
      }
    }
    return ConsumerOffsetResetReport.builder()
        .partitionsUpdated(resets.size())
        .resets(resets)
        .build();
  }

  /** Drops messages every registered consumer group of the topic has committed past. */
  @Override
  // Only committed-offset retention — age and row caps from QueueTopicDefaults are not enforced.
  // Acceptable for the lite stack where queues are ephemeral and messages flow through in seconds.
  public void applyRetention() {
    synchronized (lock) {
      for (Topic topic : topicsById.values()) {
        if (topic.consumers.isEmpty()) {
          continue;
        }
        for (Partition p : topic.partitions) {
          long floor =
              topic.consumers.keySet().stream().mapToLong(p::committedOffset).min().orElse(0L);
          p.messages.removeIf(
              m -> {
                boolean drop = m.handle.enqueueSeq() <= floor;
                if (drop) {
                  p.byId.remove(m.handle.id());
                }
                return drop;
              });
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------

  @Nullable
  private Partition partition(long topicId, int partitionId) {
    Topic topic = topicsById.get(topicId);
    return topic == null ? null : topic.partition(partitionId);
  }

  private static String lockOwnerOf(Message m, String consumerGroup) {
    Lease lease = m.leases.get(consumerGroup);
    return lease == null ? "" : lease.owner;
  }

  private static final class Topic {
    final long id;
    final String name;
    final int partitionCount;
    @Nullable final String defaultContentTypeMime;
    final List<Partition> partitions = new ArrayList<>();
    final Map<String, ConsumerRegistrationRow> consumers = new ConcurrentHashMap<>();
    private long appendsSinceRetention;

    Topic(long id, String name, int partitionCount, @Nullable String defaultContentTypeMime) {
      this.id = id;
      this.name = name;
      this.partitionCount = partitionCount;
      this.defaultContentTypeMime = defaultContentTypeMime;
      for (int i = 0; i < partitionCount; i++) {
        partitions.add(new Partition(i));
      }
    }

    QueueTopicMetadata metadata() {
      return new QueueTopicMetadata(id, partitionCount, Optional.empty());
    }

    @Nullable
    Partition partition(int partitionId) {
      return partitionId >= 0 && partitionId < partitionCount ? partitions.get(partitionId) : null;
    }

    QueueMessageHandle append(
        long messageId,
        Instant now,
        String routingKey,
        int priority,
        byte[] payload,
        Optional<String> contentType,
        List<QueueMessageHeader> headers,
        PgQueuePayloadCompression payloadCompression) {
      Partition p =
          partitions.get(MetadataQueueRouting.stablePartitionId(routingKey, partitionCount));
      QueueMessageHandle handle = new QueueMessageHandle(messageId, now, id, p.id, p.nextSeq++);
      Message m =
          new Message(
              handle, routingKey, priority, payload, contentType, headers, payloadCompression);
      p.messages.add(m);
      p.byId.put(messageId, m);
      // Bound heap growth on busy topics without a maintenance thread.
      if (++appendsSinceRetention >= 1_000) {
        appendsSinceRetention = 0;
        for (Partition part : partitions) {
          long floor =
              consumers.isEmpty()
                  ? 0L
                  : consumers.keySet().stream().mapToLong(part::committedOffset).min().orElse(0L);
          part.messages.removeIf(
              msg -> {
                boolean drop = msg.handle.enqueueSeq() <= floor;
                if (drop) {
                  part.byId.remove(msg.handle.id());
                }
                return drop;
              });
        }
      }
      return handle;
    }
  }

  private static final class Partition {
    final int id;
    long nextSeq = 1;
    final List<Message> messages = new ArrayList<>();
    final Map<Long, Message> byId = new HashMap<>();
    final Map<String, Long> committedOffsets = new HashMap<>();

    Partition(int id) {
      this.id = id;
    }

    long maxSeq() {
      return nextSeq - 1;
    }

    long committedOffset(String consumerGroup) {
      return committedOffsets.getOrDefault(consumerGroup, 0L);
    }
  }

  private static final class Message {
    final QueueMessageHandle handle;
    final String routingKey;
    final int priority;
    final byte[] payload;
    final Optional<String> contentType;
    final List<QueueMessageHeader> headers;
    final PgQueuePayloadCompression payloadCompression;
    final Map<String, Lease> leases = new HashMap<>();

    Message(
        QueueMessageHandle handle,
        String routingKey,
        int priority,
        byte[] payload,
        Optional<String> contentType,
        List<QueueMessageHeader> headers,
        PgQueuePayloadCompression payloadCompression) {
      this.handle = handle;
      this.routingKey = routingKey;
      this.priority = priority;
      this.payload = payload;
      this.contentType = contentType;
      this.headers = List.copyOf(headers);
      this.payloadCompression = payloadCompression;
    }

    boolean isAckedBy(String consumerGroup) {
      Lease lease = leases.get(consumerGroup);
      return lease != null && lease.acked;
    }
  }

  private record Lease(String owner, Instant lockedUntil, boolean acked) {}
}
