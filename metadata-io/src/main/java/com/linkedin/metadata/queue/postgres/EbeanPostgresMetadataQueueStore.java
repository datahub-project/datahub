package com.linkedin.metadata.queue.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.ConsumerOffsetResetDetail;
import com.linkedin.metadata.queue.ConsumerOffsetResetReport;
import com.linkedin.metadata.queue.ConsumerOffsetResetSpec;
import com.linkedin.metadata.queue.ConsumerRegistrationRow;
import com.linkedin.metadata.queue.EnqueueBatchItem;
import com.linkedin.metadata.queue.MetadataQueueRouting;
import com.linkedin.metadata.queue.MetadataQueueStore;
import com.linkedin.metadata.queue.PartitionOffsetSkew;
import com.linkedin.metadata.queue.PgQueueContiguousOffset;
import com.linkedin.metadata.queue.PgQueueLeaseMarkers;
import com.linkedin.metadata.queue.PgQueueOffsetSkewWarnings;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.PriorityBand;
import com.linkedin.metadata.queue.PriorityBandConfig;
import com.linkedin.metadata.queue.QueueHeadersJson;
import com.linkedin.metadata.queue.QueueLogPeekRow;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueMessageHeader;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import com.linkedin.metadata.queue.QueueTableNames;
import com.linkedin.metadata.queue.QueueTopicDefaults;
import com.linkedin.metadata.queue.QueueTopicMetadata;
import com.linkedin.metadata.queue.ebean.EbeanPgQueueTopic;
import io.ebean.Database;
import io.ebean.SqlRow;
import io.ebean.Transaction;
import io.ebean.TxScope;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * PostgreSQL pgQueue SqlSetup implementation using Ebean {@link Transaction} boundaries and JDBC
 * for native SQL (partitioned message table, per-group leases, advisory locks on enqueue). {@link
 * com.linkedin.metadata.queue.ebean.EbeanPgQueueTopic} is used for reads when physical layout is
 * the SqlSetup default ({@link QueueTableNames#matchesDefaultEntityPhysicalMapping()}).
 */
public class EbeanPostgresMetadataQueueStore implements MetadataQueueStore {

  private final Database database;
  private final QueueTableNames tableNames;
  private final PriorityBandConfig priorityBandConfig;

  @Nullable private final PgQueueSetupOptions pgQueueOptions;

  /** MIME catalog ids are immutable once registered. */
  private final ConcurrentHashMap<String, Short> contentTypeIdByMime = new ConcurrentHashMap<>();

  /**
   * Topic catalog metadata for this process. Refreshed after {@link #insertTopicIfMissing}; {@code
   * partition_count} may grow but names are stable.
   */
  private final ConcurrentHashMap<String, QueueTopicMetadata> topicMetaByName =
      new ConcurrentHashMap<>();

  public EbeanPostgresMetadataQueueStore(
      @Nonnull Database database,
      @Nonnull QueueTableNames tableNames,
      @Nonnull PriorityBandConfig priorityBandConfig) {
    this.database = database;
    this.tableNames = tableNames;
    this.pgQueueOptions = null;
    this.priorityBandConfig = priorityBandConfig;
  }

  public EbeanPostgresMetadataQueueStore(
      @Nonnull Database database,
      @Nonnull PostgresSqlSetupProperties postgresProperties,
      @Nullable KafkaConfiguration kafkaConfiguration,
      @Nonnull ObjectMapper objectMapper) {
    this.database = database;
    PgQueueSetupOptions opts = postgresProperties.buildPgQueueOptions(kafkaConfiguration);
    this.tableNames = QueueTableNames.fromPostgresProperties(postgresProperties);
    this.pgQueueOptions = opts;
    this.priorityBandConfig =
        PriorityBandConfig.parse(objectMapper, opts.getTopicDefaultPriorityBands());
  }

  @Override
  @Nonnull
  public Optional<QueueTopicMetadata> fetchTopic(@Nonnull String topicName) {
    QueueTopicMetadata cached = topicMetaByName.get(topicName);
    if (cached != null) {
      return Optional.of(cached);
    }
    if (tableNames.matchesDefaultEntityPhysicalMapping()) {
      EbeanPgQueueTopic row =
          database.find(EbeanPgQueueTopic.class).where().eq("topicName", topicName).findOne();
      if (row == null) {
        return Optional.empty();
      }
      QueueTopicMetadata meta =
          new QueueTopicMetadata(
              row.getId(),
              row.getPartitionCount(),
              Optional.ofNullable(row.getDefaultContentTypeId()));
      topicMetaByName.put(topicName, meta);
      return Optional.of(meta);
    }
    SqlRow row =
        database
            .sqlQuery(
                "SELECT id, partition_count, default_content_type_id FROM "
                    + tableNames.qualifiedTopic()
                    + " WHERE topic_name = :tn")
            .setParameter("tn", topicName)
            .findOne();
    if (row == null) {
      return Optional.empty();
    }
    QueueTopicMetadata meta =
        new QueueTopicMetadata(
            row.getLong("id"),
            row.getInteger("partition_count"),
            Optional.ofNullable(row.getInteger("default_content_type_id")));
    topicMetaByName.put(topicName, meta);
    return Optional.of(meta);
  }

  @Override
  public long ensureTopic(@Nonnull String topicName, @Nonnull QueueTopicDefaults defaults) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        insertTopicIfMissing(conn, topicName, resolveEffectiveDefaults(topicName, defaults));
        long id = getTopicMeta(conn, topicName).id();
        tx.commit();
        return id;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("ensureTopic failed for " + topicName, e);
      }
    }
  }

  @Override
  @Nonnull
  public QueueMessageHandle enqueue(
      @Nonnull String topicName,
      @Nonnull String routingKey,
      @Nonnull QueueTopicDefaults defaults,
      int priority,
      @Nonnull byte[] payload,
      @Nonnull Optional<String> contentType,
      @Nonnull List<QueueMessageHeader> headers,
      @Nonnull PgQueuePayloadCompression payloadCompression) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        QueueTopicDefaults effective = resolveEffectiveDefaults(topicName, defaults);
        insertTopicIfMissing(conn, topicName, effective);
        QueueMessageHandle handle =
            enqueueInOpenTransaction(
                conn,
                getTopicMeta(conn, topicName),
                routingKey,
                priority,
                payload,
                contentType,
                payloadCompression,
                headers);
        tx.commit();
        return handle;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("enqueue failed for " + topicName, e);
      }
    }
  }

  @Override
  @Nonnull
  public List<QueueMessageHandle> enqueueBatch(
      @Nonnull List<EnqueueBatchItem> items, @Nonnull QueueTopicDefaults defaults) {
    if (items.isEmpty()) {
      return List.of();
    }
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        List<QueueMessageHandle> out = new ArrayList<>(items.size());
        Map<String, QueueTopicMetadata> topicMetaInBatch = new HashMap<>();
        for (EnqueueBatchItem it : items) {
          QueueTopicMetadata meta =
              topicMetaInBatch.computeIfAbsent(
                  it.topicName(),
                  topic -> {
                    try {
                      QueueTopicDefaults effectiveForTopic =
                          resolveEffectiveDefaults(topic, defaults);
                      insertTopicIfMissing(conn, topic, effectiveForTopic);
                      return getTopicMeta(conn, topic);
                    } catch (SQLException e) {
                      throw new IllegalStateException(
                          "enqueueBatch topic ensure failed: " + topic, e);
                    }
                  });
          out.add(
              enqueueInOpenTransaction(
                  conn,
                  meta,
                  it.routingKey(),
                  it.priority(),
                  it.payload(),
                  it.contentType(),
                  it.payloadCompression(),
                  it.headers()));
        }
        tx.commit();
        return out;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("enqueueBatch failed", e);
      }
    }
  }

  @Override
  @Nonnull
  public Map<Integer, Long> partitionNextExclusiveSeqs(long topicId, int partitionCount) {
    Map<Integer, Long> out = new LinkedHashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      out.put(i, 1L);
    }
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "SELECT partition_id, COALESCE(MAX(enqueue_seq), 0) + 1 AS n FROM "
                + tableNames.qualifiedMessage()
                + " WHERE topic_id = ? GROUP BY partition_id";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setLong(1, topicId);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              out.put(rs.getInt(1), rs.getLong(2));
            }
          }
        }
        tx.commit();
        return out;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("partitionNextExclusiveSeqs failed", e);
      }
    }
  }

  @Override
  @Nonnull
  public Map<Integer, Long> partitionMaxEnqueueSeqs(long topicId, int partitionCount) {
    Map<Integer, Long> out = new LinkedHashMap<>();
    for (int i = 0; i < partitionCount; i++) {
      out.put(i, 0L);
    }
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "SELECT partition_id, COALESCE(MAX(enqueue_seq), 0) AS n FROM "
                + tableNames.qualifiedMessage()
                + " WHERE topic_id = ? GROUP BY partition_id";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setLong(1, topicId);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              out.put(rs.getInt(1), rs.getLong(2));
            }
          }
        }
        tx.commit();
        return out;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("partitionMaxEnqueueSeqs failed", e);
      }
    }
  }

  @Override
  @Nonnull
  public List<PartitionOffsetSkew> detectOffsetAheadOfLog(
      @Nonnull String consumerGroup, long topicId, int partitionCount) {
    Map<Integer, Long> maxSeqs = partitionMaxEnqueueSeqs(topicId, partitionCount);
    List<PartitionOffsetSkew> skewed = new ArrayList<>();
    for (int p = 0; p < partitionCount; p++) {
      long maxSeq = maxSeqs.getOrDefault(p, 0L);
      long committed = getCommittedOffset(consumerGroup, topicId, p);
      if (committed > maxSeq) {
        skewed.add(
            PartitionOffsetSkew.builder()
                .consumerGroup(consumerGroup)
                .topicId(topicId)
                .partitionId(p)
                .committedOffset(committed)
                .maxSeq(maxSeq)
                .aheadBy(committed - maxSeq)
                .build());
      }
    }
    return skewed;
  }

  @Override
  @Nonnull
  public OptionalLong minEnqueueSeqAtOrAfter(
      long topicId, int partitionId, @Nonnull Instant minEnqueuedAt) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "SELECT MIN(enqueue_seq) FROM "
                + tableNames.qualifiedMessage()
                + " WHERE topic_id = ? AND partition_id = ? AND enqueued_at >= ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setLong(1, topicId);
          ps.setInt(2, partitionId);
          ps.setTimestamp(3, Timestamp.from(minEnqueuedAt));
          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next() && rs.getObject(1) != null) {
              long v = rs.getLong(1);
              tx.commit();
              return OptionalLong.of(v);
            }
          }
        }
        tx.commit();
        return OptionalLong.empty();
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("minEnqueueSeqAtOrAfter failed", e);
      }
    }
  }

  @Override
  @Nonnull
  public OptionalLong minEnqueueSeq(long topicId, int partitionId) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "SELECT MIN(enqueue_seq) FROM "
                + tableNames.qualifiedMessage()
                + " WHERE topic_id = ? AND partition_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setLong(1, topicId);
          ps.setInt(2, partitionId);
          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next() && rs.getObject(1) != null) {
              long v = rs.getLong(1);
              tx.commit();
              return OptionalLong.of(v);
            }
          }
        }
        tx.commit();
        return OptionalLong.empty();
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("minEnqueueSeq failed", e);
      }
    }
  }

  @Override
  @Nonnull
  public List<QueueLogPeekRow> peekTopicLog(
      long topicId, @Nonnull Map<Integer, Long> partitionToMinExclusiveSeq, int limit) {
    if (limit <= 0 || partitionToMinExclusiveSeq.isEmpty()) {
      return List.of();
    }
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        List<QueueLogPeekRow> rows =
            peekTopicLogUsingConnection(conn, topicId, partitionToMinExclusiveSeq, limit);
        tx.commit();
        return rows;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("peekTopicLog failed", e);
      }
    }
  }

  private List<QueueLogPeekRow> peekTopicLogUsingConnection(
      Connection conn, long topicId, Map<Integer, Long> partitionToMinExclusiveSeq, int limit)
      throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(
        "SELECT m.id, m.enqueued_at, m.topic_id, m.partition_id, m.enqueue_seq, m.priority, m.payload, "
            + "ct.mime AS content_type, m.payload_compression, m.headers, m.routing_key FROM ");
    sb.append(tableNames.qualifiedMessage());
    sb.append(" m INNER JOIN ");
    sb.append(tableNames.qualifiedTopic());
    sb.append(
        " t ON t.id = m.topic_id LEFT JOIN "
            + tableNames.qualifiedContentType()
            + " ct ON ct.id = COALESCE(m.content_type_id, t.default_content_type_id) WHERE m.topic_id = ? AND (");
    int i = 0;
    for (Map.Entry<Integer, Long> ignored : partitionToMinExclusiveSeq.entrySet()) {
      if (i++ > 0) {
        sb.append(" OR ");
      }
      sb.append("(m.partition_id = ? AND m.enqueue_seq >= ?)");
    }
    sb.append(") ORDER BY m.partition_id, m.priority ASC, m.enqueue_seq ASC LIMIT ?");
    try (PreparedStatement ps = conn.prepareStatement(sb.toString())) {
      int idx = 1;
      ps.setLong(idx++, topicId);
      for (Map.Entry<Integer, Long> e : partitionToMinExclusiveSeq.entrySet()) {
        ps.setInt(idx++, e.getKey());
        ps.setLong(idx++, e.getValue());
      }
      ps.setInt(idx, limit);
      try (ResultSet rs = ps.executeQuery()) {
        List<QueueLogPeekRow> out = new ArrayList<>();
        while (rs.next()) {
          QueueMessageHandle handle =
              new QueueMessageHandle(
                  rs.getLong(1),
                  rs.getTimestamp(2).toInstant(),
                  rs.getLong(3),
                  rs.getInt(4),
                  rs.getLong(5));
          byte[] payload = rs.getBytes(7);
          short compressionRaw = rs.getShort(9);
          if (rs.wasNull()) {
            compressionRaw = PgQueuePayloadCompression.NONE.wireCode();
          }
          String routingKeyPeek = rs.getString(11);
          out.add(
              new QueueLogPeekRow(
                  handle,
                  rs.getInt(6),
                  payload != null ? payload : new byte[0],
                  Optional.ofNullable(rs.getString(8)),
                  PgQueuePayloadCompression.fromWire(compressionRaw),
                  QueueHeadersJson.deserialize(rs.getObject(10)),
                  routingKeyPeek != null ? routingKeyPeek : ""));
        }
        return out;
      }
    }
  }

  private QueueTopicDefaults resolveEffectiveDefaults(
      String topicName, QueueTopicDefaults passedDefaults) {
    if (pgQueueOptions == null) {
      return passedDefaults;
    }
    return QueueTopicDefaults.resolveForTopic(pgQueueOptions, topicName);
  }

  /**
   * Upserts the topic catalog row. {@code partition_count} uses {@code GREATEST} so it never drops
   * below the prior catalog value or below {@code MAX(partition_id)+1} for existing message rows
   * (avoids consumers polling fewer partitions than enqueued data).
   */
  private void insertTopicIfMissing(Connection conn, String topicName, QueueTopicDefaults defaults)
      throws SQLException {
    String mime = defaults.defaultContentTypeMime();
    if (mime == null || mime.isBlank()) {
      mime = "application/avro";
    }
    short defaultCtId = ensureContentTypeRegistered(conn, mime);
    String sql =
        "INSERT INTO "
            + tableNames.qualifiedTopic()
            + " AS ptopic (topic_name, partition_count, retention_max_age_seconds, "
            + "max_rows_per_topic, max_total_payload_bytes, default_content_type_id, aggressive_retention) "
            + "VALUES (?,?,?,?,?,?,?) ON CONFLICT (topic_name) DO UPDATE SET "
            + "partition_count = GREATEST(1, EXCLUDED.partition_count, ptopic.partition_count, "
            + "COALESCE((SELECT MAX(m.partition_id) FROM "
            + tableNames.qualifiedMessage()
            + " m WHERE m.topic_id = ptopic.id), -1) + 1), "
            + "retention_max_age_seconds = EXCLUDED.retention_max_age_seconds, "
            + "max_rows_per_topic = EXCLUDED.max_rows_per_topic, "
            + "max_total_payload_bytes = EXCLUDED.max_total_payload_bytes, "
            + "default_content_type_id = EXCLUDED.default_content_type_id, "
            + "aggressive_retention = EXCLUDED.aggressive_retention";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, topicName);
      ps.setInt(2, defaults.partitionCount());
      ps.setInt(3, defaults.retentionMaxAgeSeconds());
      ps.setLong(4, defaults.maxRowsPerTopic());
      ps.setLong(5, defaults.maxTotalPayloadBytesPerTopic());
      ps.setShort(6, defaultCtId);
      ps.setBoolean(7, defaults.aggressiveRetention());
      ps.executeUpdate();
    }
    refreshTopicMetaCache(conn, topicName);
  }

  @Nonnull
  private QueueTopicMetadata getTopicMeta(Connection conn, String topicName) throws SQLException {
    QueueTopicMetadata cached = topicMetaByName.get(topicName);
    if (cached != null) {
      return cached;
    }
    return refreshTopicMetaCache(conn, topicName);
  }

  @Nonnull
  private QueueTopicMetadata refreshTopicMetaCache(Connection conn, String topicName)
      throws SQLException {
    QueueTopicMetadata meta = loadTopicMeta(conn, topicName);
    topicMetaByName.put(topicName, meta);
    return meta;
  }

  private QueueTopicMetadata loadTopicMeta(Connection conn, String topicName) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT id, partition_count, default_content_type_id FROM "
                + tableNames.qualifiedTopic()
                + " WHERE topic_name = ?")) {
      ps.setString(1, topicName);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new IllegalStateException("topic not found after ensure: " + topicName);
        }
        short dctRaw = rs.getShort(3);
        Integer dct = rs.wasNull() ? null : Integer.valueOf(dctRaw);
        return new QueueTopicMetadata(rs.getLong(1), rs.getInt(2), Optional.ofNullable(dct));
      }
    }
  }

  /**
   * Resolves {@code mime} to a catalog id without burning the {@code smallint} identity when the
   * MIME already exists. Lookup first; {@code INSERT … ON CONFLICT DO NOTHING} avoids identity burn
   * and keeps the transaction valid under concurrent registration (plain INSERT + catch would leave
   * Postgres in an aborted-transaction state).
   */
  private short ensureContentTypeRegistered(Connection conn, String mime) throws SQLException {
    Short cached = contentTypeIdByMime.get(mime);
    if (cached != null) {
      return cached;
    }
    Short existing = lookupContentTypeId(conn, mime);
    if (existing != null) {
      contentTypeIdByMime.put(mime, existing);
      return existing;
    }
    try (PreparedStatement ins =
        conn.prepareStatement(
            "INSERT INTO "
                + tableNames.qualifiedContentType()
                + " (mime) VALUES (?) ON CONFLICT (mime) DO NOTHING")) {
      ins.setString(1, mime);
      ins.executeUpdate();
    }
    Short inserted = lookupContentTypeId(conn, mime);
    if (inserted == null) {
      throw new IllegalStateException("content_type missing after insert: " + mime);
    }
    contentTypeIdByMime.put(mime, inserted);
    return inserted;
  }

  @Nullable
  private Short lookupContentTypeId(Connection conn, String mime) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT id FROM " + tableNames.qualifiedContentType() + " WHERE mime = ?")) {
      ps.setString(1, mime);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        return rs.getShort(1);
      }
    }
  }

  /**
   * Stores {@code NULL} when the resolved MIME matches the topic default (omit redundant FK on the
   * row).
   */
  @Nullable
  private Integer computeStoredContentTypeId(
      Connection conn, QueueTopicMetadata meta, Optional<String> contentType) throws SQLException {
    Optional<Integer> topicDefault = meta.defaultContentTypeId();
    if (contentType.isEmpty()) {
      return null;
    }
    short id = ensureContentTypeRegistered(conn, contentType.get());
    if (topicDefault.isPresent() && topicDefault.get() == (int) id) {
      return null;
    }
    return (int) id;
  }

  private String receiveReturningContentTypeExpr() {
    return "(SELECT ct.mime FROM "
        + tableNames.qualifiedContentType()
        + " ct WHERE ct.id = COALESCE(m.content_type_id, (SELECT t.default_content_type_id FROM "
        + tableNames.qualifiedTopic()
        + " t WHERE t.id = m.topic_id)))";
  }

  private QueueMessageHandle enqueueInOpenTransaction(
      Connection conn,
      QueueTopicMetadata meta,
      String routingKey,
      int priority,
      byte[] payload,
      Optional<String> contentType,
      PgQueuePayloadCompression payloadCompression,
      List<QueueMessageHeader> headers)
      throws SQLException {
    QueueTopicMetadata.validatePriority(priority);
    Integer storedContentTypeId = computeStoredContentTypeId(conn, meta, contentType);
    int partitionId = MetadataQueueRouting.stablePartitionId(routingKey, meta.partitionCount());
    long topicPk = meta.id();
    long lockKey = MetadataQueueRouting.advisoryLockKey(topicPk, partitionId);

    try (PreparedStatement lockPs = conn.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
      lockPs.setLong(1, lockKey);
      lockPs.executeQuery();
    }

    String sql =
        "WITH mx AS (SELECT COALESCE(MAX(enqueue_seq), 0) AS max_seq FROM "
            + tableNames.qualifiedMessage()
            + " WHERE topic_id = ? AND partition_id = ?) INSERT INTO "
            + tableNames.qualifiedMessage()
            + " (topic_id, partition_id, routing_key, enqueue_seq, priority, payload, content_type_id, payload_compression, headers) "
            + "SELECT ?, ?, ?, mx.max_seq + 1, ?, ?, ?, ?, CAST(? AS jsonb) FROM mx RETURNING id, enqueued_at, enqueue_seq";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, topicPk);
      ps.setInt(2, partitionId);
      ps.setLong(3, topicPk);
      ps.setInt(4, partitionId);
      ps.setString(5, routingKey);
      ps.setInt(6, priority);
      ps.setBytes(7, payload);
      if (storedContentTypeId != null) {
        ps.setInt(8, storedContentTypeId);
      } else {
        ps.setNull(8, Types.SMALLINT);
      }
      ps.setShort(9, payloadCompression.wireCode());
      setNullableJsonb(ps, 10, QueueHeadersJson.serialize(headers));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new IllegalStateException("enqueue RETURNING produced no row");
        }
        long id = rs.getLong(1);
        Instant enqueuedAt = rs.getTimestamp(2).toInstant();
        long seq = rs.getLong(3);
        return new QueueMessageHandle(id, enqueuedAt, topicPk, partitionId, seq);
      }
    }
  }

  private void upsertConsumerOffsets(
      Connection conn, String consumerGroup, List<QueueMessageHandle> handles) throws SQLException {
    Map<Long, Map<Integer, Boolean>> partitionsByTopic = new HashMap<>();
    for (QueueMessageHandle h : handles) {
      partitionsByTopic
          .computeIfAbsent(h.topicId(), k -> new HashMap<>())
          .put(h.partitionId(), Boolean.TRUE);
    }
    String sql =
        "INSERT INTO "
            + tableNames.qualifiedConsumerOffset()
            + " AS co (consumer_group, topic_id, partition_id, offset_value, epoch) "
            + "VALUES (?,?,?,?,0) ON CONFLICT (consumer_group, topic_id, partition_id) "
            + "DO UPDATE SET offset_value = EXCLUDED.offset_value";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (Map.Entry<Long, Map<Integer, Boolean>> te : partitionsByTopic.entrySet()) {
        long topicId = te.getKey();
        for (Integer partitionId : te.getValue().keySet()) {
          long current = loadCommittedOffset(conn, consumerGroup, topicId, partitionId);
          List<Long> ackedSeqs =
              loadAckedEnqueueSeqs(conn, consumerGroup, topicId, partitionId, current);
          long newOffset = PgQueueContiguousOffset.advanceWatermark(current, ackedSeqs);
          if (newOffset <= current) {
            continue;
          }
          ps.setString(1, consumerGroup);
          ps.setLong(2, topicId);
          ps.setInt(3, partitionId);
          ps.setLong(4, newOffset);
          ps.addBatch();
        }
      }
      ps.executeBatch();
    }
  }

  private List<Long> loadAckedEnqueueSeqs(
      Connection conn, String consumerGroup, long topicId, int partitionId, long minExclusiveSeq)
      throws SQLException {
    String sql =
        "SELECT m.enqueue_seq FROM "
            + tableNames.qualifiedMessage()
            + " m INNER JOIN "
            + tableNames.qualifiedMessageGroupLease()
            + " l ON l.message_id = m.id AND l.message_enqueued_at = m.enqueued_at AND l.consumer_group = ? "
            + "WHERE m.topic_id = ? AND m.partition_id = ? AND m.enqueue_seq > ? AND l.lock_owner = ? "
            + "ORDER BY m.enqueue_seq";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, consumerGroup);
      ps.setLong(2, topicId);
      ps.setInt(3, partitionId);
      ps.setLong(4, minExclusiveSeq);
      ps.setString(5, PgQueueLeaseMarkers.ACKED_LOCK_OWNER);
      try (ResultSet rs = ps.executeQuery()) {
        List<Long> seqs = new ArrayList<>();
        while (rs.next()) {
          seqs.add(rs.getLong(1));
        }
        return seqs;
      }
    }
  }

  private static void setNullableJsonb(PreparedStatement ps, int index, @Nullable String json)
      throws SQLException {
    if (json == null) {
      ps.setNull(index, Types.OTHER);
    } else {
      ps.setString(index, json);
    }
  }

  @Override
  @Nonnull
  public List<QueueReceivedMessage> receiveBatchForGroup(
      @Nonnull String consumerGroup,
      long topicId,
      @Nonnull List<Integer> partitionIds,
      @Nonnull String lockOwner,
      @Nonnull Duration visibilityTimeout,
      int maxMessages) {
    if (maxMessages <= 0 || partitionIds.isEmpty()) {
      return List.of();
    }
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        List<QueueReceivedMessage> combined = new ArrayList<>();
        int remaining = maxMessages;
        for (int pid : partitionIds) {
          if (remaining <= 0) {
            break;
          }
          combined.addAll(
              receivePartitionForGroup(
                  conn, consumerGroup, topicId, pid, lockOwner, visibilityTimeout, remaining));
          remaining = maxMessages - combined.size();
        }
        tx.commit();
        return combined;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("receiveBatchForGroup failed", e);
      }
    }
  }

  private List<QueueReceivedMessage> receivePartitionForGroup(
      Connection conn,
      String consumerGroup,
      long topicId,
      int partitionId,
      String lockOwner,
      Duration visibilityTimeout,
      int limit)
      throws SQLException {
    long committedOffset = loadCommittedOffset(conn, consumerGroup, topicId, partitionId);
    long maxSeq = loadMaxEnqueueSeq(conn, topicId, partitionId);
    if (committedOffset > maxSeq) {
      PgQueueOffsetSkewWarnings.getInstance()
          .warnIfAhead(
              PartitionOffsetSkew.builder()
                  .consumerGroup(consumerGroup)
                  .topicId(topicId)
                  .partitionId(partitionId)
                  .committedOffset(committedOffset)
                  .maxSeq(maxSeq)
                  .aheadBy(committedOffset - maxSeq)
                  .build());
    }
    List<QueueReceivedMessage> out = new ArrayList<>();
    double visSecs = visibilityTimeout.toNanos() / 1_000_000_000.0;
    receivePartitionRound(
        conn, consumerGroup, topicId, partitionId, committedOffset, lockOwner, visSecs, limit, out);
    return out;
  }

  /**
   * For each priority band, acquire leases head-of-line per priority value (do not skip a leased
   * lower {@code enqueue_seq} within the same priority).
   */
  private void receivePartitionRound(
      Connection conn,
      String consumerGroup,
      long topicId,
      int partitionId,
      long minExclusiveSeq,
      String lockOwner,
      double visibilitySeconds,
      int remaining,
      List<QueueReceivedMessage> out)
      throws SQLException {
    int[] limits = priorityBandConfig.batchLimits(remaining);
    List<PriorityBand> bands = priorityBandConfig.bands();
    int bandRemaining = remaining;
    int spare = 0;
    for (int i = 0; i < bands.size() && (bandRemaining > 0 || spare > 0); i++) {
      int bandLimit = limits[i] + spare;
      spare = 0;
      if (bandLimit <= 0 || bandRemaining <= 0) {
        continue;
      }
      int cap = Math.min(bandLimit, bandRemaining);
      int acquired =
          receiveBandHeadOfLine(
              conn,
              consumerGroup,
              topicId,
              partitionId,
              minExclusiveSeq,
              bands.get(i),
              cap,
              lockOwner,
              visibilitySeconds,
              out);
      bandRemaining -= acquired;
      if (acquired < cap) {
        spare += cap - acquired;
      }
    }
  }

  private int receiveBandHeadOfLine(
      Connection conn,
      String consumerGroup,
      long topicId,
      int partitionId,
      long minExclusiveSeq,
      PriorityBand band,
      int bandLimit,
      String lockOwner,
      double visibilitySeconds,
      List<QueueReceivedMessage> out)
      throws SQLException {
    if (bandLimit <= 0) {
      return 0;
    }
    int acquired = 0;
    for (int priority = band.minPriority();
        priority <= band.maxPriority() && acquired < bandLimit;
        priority++) {
      long seqFloor = minExclusiveSeq;
      while (acquired < bandLimit) {
        List<QueueMessageHandle> heads =
            selectCandidateHandlesForPriority(
                conn, consumerGroup, topicId, partitionId, seqFloor, priority, 1);
        if (heads.isEmpty()) {
          break;
        }
        QueueMessageHandle head = heads.get(0);
        if (!tryAcquireLeaseForGroup(conn, head, consumerGroup, lockOwner, visibilitySeconds)) {
          break;
        }
        out.add(loadReceivedMessageRow(conn, head, lockOwner));
        acquired++;
        seqFloor = head.enqueueSeq();
      }
    }
    return acquired;
  }

  private long loadMaxEnqueueSeq(Connection conn, long topicId, int partitionId)
      throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT COALESCE(MAX(enqueue_seq), 0) FROM "
                + tableNames.qualifiedMessage()
                + " WHERE topic_id = ? AND partition_id = ?")) {
      ps.setLong(1, topicId);
      ps.setInt(2, partitionId);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return 0L;
        }
        return rs.getLong(1);
      }
    }
  }

  private long loadCommittedOffset(
      Connection conn, String consumerGroup, long topicId, int partitionId) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT offset_value FROM "
                + tableNames.qualifiedConsumerOffset()
                + " WHERE consumer_group = ? AND topic_id = ? AND partition_id = ?")) {
      ps.setString(1, consumerGroup);
      ps.setLong(2, topicId);
      ps.setInt(3, partitionId);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return 0L;
        }
        return rs.getLong(1);
      }
    }
  }

  private List<QueueMessageHandle> selectCandidateHandlesForPriority(
      Connection conn,
      String consumerGroup,
      long topicId,
      int partitionId,
      long minExclusiveSeq,
      int priority,
      int limit)
      throws SQLException {
    String sql =
        "SELECT m.id, m.enqueued_at, m.topic_id, m.partition_id, m.enqueue_seq FROM "
            + tableNames.qualifiedMessage()
            + " m LEFT JOIN "
            + tableNames.qualifiedMessageGroupLease()
            + " l ON l.message_id = m.id AND l.message_enqueued_at = m.enqueued_at AND l.consumer_group = ? "
            + "WHERE m.topic_id = ? AND m.partition_id = ? AND m.enqueue_seq > ? "
            + "AND m.priority = ? "
            + "AND (l.id IS NULL OR l.lock_owner <> ?) "
            + "ORDER BY m.enqueue_seq ASC LIMIT ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, consumerGroup);
      ps.setLong(2, topicId);
      ps.setInt(3, partitionId);
      ps.setLong(4, minExclusiveSeq);
      ps.setInt(5, priority);
      ps.setString(6, PgQueueLeaseMarkers.ACKED_LOCK_OWNER);
      ps.setInt(7, limit);
      try (ResultSet rs = ps.executeQuery()) {
        List<QueueMessageHandle> handles = new ArrayList<>();
        while (rs.next()) {
          handles.add(
              new QueueMessageHandle(
                  rs.getLong(1),
                  rs.getTimestamp(2).toInstant(),
                  rs.getLong(3),
                  rs.getInt(4),
                  rs.getLong(5)));
        }
        return handles;
      }
    }
  }

  private boolean tryAcquireLeaseForGroup(
      Connection conn,
      QueueMessageHandle h,
      String consumerGroup,
      String lockOwner,
      double visibilitySeconds)
      throws SQLException {
    String lease = tableNames.qualifiedMessageGroupLease();
    String sql =
        "INSERT INTO "
            + lease
            + " (message_id, message_enqueued_at, consumer_group, lock_owner, locked_until) "
            + "VALUES (?,?,?,?, NOW() + (? * INTERVAL '1 second')) "
            + "ON CONFLICT (message_id, message_enqueued_at, consumer_group) DO UPDATE SET "
            + "lock_owner = EXCLUDED.lock_owner, locked_until = EXCLUDED.locked_until "
            + "WHERE "
            + lease
            + ".locked_until < NOW() AND "
            + lease
            + ".lock_owner <> ? "
            + "RETURNING "
            + lease
            + ".message_id";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, h.id());
      ps.setTimestamp(2, Timestamp.from(h.enqueuedAt()));
      ps.setString(3, consumerGroup);
      ps.setString(4, lockOwner);
      ps.setDouble(5, visibilitySeconds);
      ps.setString(6, PgQueueLeaseMarkers.ACKED_LOCK_OWNER);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private QueueReceivedMessage loadReceivedMessageRow(
      Connection conn, QueueMessageHandle h, String lockOwner) throws SQLException {
    String sql =
        "SELECT m.priority, m.payload, "
            + receiveReturningContentTypeExpr()
            + " AS content_type, m.payload_compression, m.headers, m.routing_key FROM "
            + tableNames.qualifiedMessage()
            + " m WHERE m.id = ? AND m.enqueued_at = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, h.id());
      ps.setTimestamp(2, Timestamp.from(h.enqueuedAt()));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new IllegalStateException("message row missing after lease: " + h);
        }
        byte[] payload = rs.getBytes(2);
        String ctype = rs.getString(3);
        short compressionRaw = rs.getShort(4);
        if (rs.wasNull()) {
          compressionRaw = PgQueuePayloadCompression.NONE.wireCode();
        }
        String routingKeyRecv = rs.getString(6);
        return new QueueReceivedMessage(
            h,
            rs.getInt(1),
            payload != null ? payload : new byte[0],
            Optional.ofNullable(ctype),
            PgQueuePayloadCompression.fromWire(compressionRaw),
            QueueHeadersJson.deserialize(rs.getObject(5)),
            routingKeyRecv != null ? routingKeyRecv : "",
            lockOwner);
      }
    }
  }

  @Override
  public int commitForGroup(
      @Nonnull String consumerGroup,
      @Nonnull List<QueueMessageHandle> handles,
      boolean updateConsumerOffset) {
    if (handles.isEmpty()) {
      return 0;
    }
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        int marked = markAckedForGroup(conn, consumerGroup, handles);
        if (updateConsumerOffset) {
          upsertConsumerOffsets(conn, consumerGroup, handles);
        }
        tx.commit();
        return marked;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("commitForGroup failed", e);
      }
    }
  }

  private int markAckedForGroup(
      Connection conn, String consumerGroup, List<QueueMessageHandle> handles) throws SQLException {
    String sql =
        "INSERT INTO "
            + tableNames.qualifiedMessageGroupLease()
            + " (message_id, message_enqueued_at, consumer_group, lock_owner, locked_until) "
            + "VALUES (?,?,?,?,?) ON CONFLICT (message_id, message_enqueued_at, consumer_group) "
            + "DO UPDATE SET lock_owner = EXCLUDED.lock_owner, locked_until = EXCLUDED.locked_until";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (QueueMessageHandle h : handles) {
        ps.setLong(1, h.id());
        ps.setTimestamp(2, Timestamp.from(h.enqueuedAt()));
        ps.setString(3, consumerGroup);
        ps.setString(4, PgQueueLeaseMarkers.ACKED_LOCK_OWNER);
        ps.setTimestamp(5, PgQueueLeaseMarkers.ACKED_LOCKED_UNTIL_TS);
        ps.addBatch();
      }
      ps.executeBatch();
      return handles.size();
    }
  }

  @Override
  public int extendVisibilityForGroup(
      @Nonnull String consumerGroup,
      @Nonnull List<QueueMessageHandle> handles,
      @Nonnull String lockOwner,
      @Nonnull Duration extendBy) {
    if (handles.isEmpty()) {
      return 0;
    }
    double secs = extendBy.toNanos() / 1_000_000_000.0;
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        StringBuilder sb =
            new StringBuilder("UPDATE ")
                .append(tableNames.qualifiedMessageGroupLease())
                .append(" AS l SET locked_until = NOW() + (? * INTERVAL '1 second') ")
                .append("FROM (VALUES ");
        for (int i = 0; i < handles.size(); i++) {
          if (i > 0) {
            sb.append(", ");
          }
          sb.append("(?,?::timestamptz)");
        }
        sb.append(
            ") AS v(id, enqueued_at) WHERE l.message_id = v.id AND l.message_enqueued_at = v.enqueued_at "
                + "AND l.consumer_group = ? AND l.lock_owner = ?");
        try (PreparedStatement ps = conn.prepareStatement(sb.toString())) {
          ps.setDouble(1, secs);
          int idx = 2;
          for (QueueMessageHandle h : handles) {
            ps.setLong(idx++, h.id());
            ps.setTimestamp(idx++, Timestamp.from(h.enqueuedAt()));
          }
          ps.setString(idx++, consumerGroup);
          ps.setString(idx, lockOwner);
          int updated = ps.executeUpdate();
          tx.commit();
          return updated;
        }
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("extendVisibilityForGroup failed", e);
      }
    }
  }

  @Override
  public long getCommittedOffset(@Nonnull String consumerGroup, long topicId, int partitionId) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        long v = loadCommittedOffset(conn, consumerGroup, topicId, partitionId);
        tx.commit();
        return v;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("getCommittedOffset failed", e);
      }
    }
  }

  @Override
  public void registerConsumer(@Nonnull String consumerGroup, long topicId) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "INSERT INTO "
                + tableNames.qualifiedConsumerRegistration()
                + " (consumer_group, topic_id) VALUES (?, ?) "
                + "ON CONFLICT (consumer_group, topic_id) DO UPDATE SET last_heartbeat_at = NOW()";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, consumerGroup);
          ps.setLong(2, topicId);
          ps.executeUpdate();
        }
        tx.commit();
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("registerConsumer failed", e);
      }
    }
  }

  @Override
  @Nonnull
  public List<ConsumerRegistrationRow> listRegisteredConsumers(long topicId) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "SELECT consumer_group, topic_id, registered_at, last_heartbeat_at FROM "
                + tableNames.qualifiedConsumerRegistration()
                + " WHERE topic_id = ? ORDER BY consumer_group";
        List<ConsumerRegistrationRow> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setLong(1, topicId);
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              out.add(
                  new ConsumerRegistrationRow(
                      rs.getString(1),
                      rs.getLong(2),
                      rs.getTimestamp(3).toInstant(),
                      rs.getTimestamp(4).toInstant()));
            }
          }
        }
        tx.commit();
        return out;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("listRegisteredConsumers failed", e);
      }
    }
  }

  @Override
  public boolean unregisterConsumer(@Nonnull String consumerGroup, long topicId) {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "DELETE FROM "
                + tableNames.qualifiedConsumerRegistration()
                + " WHERE consumer_group = ? AND topic_id = ?";
        int deleted;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, consumerGroup);
          ps.setLong(2, topicId);
          deleted = ps.executeUpdate();
        }
        tx.commit();
        return deleted > 0;
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("unregisterConsumer failed", e);
      }
    }
  }

  @Override
  @Nonnull
  public ConsumerOffsetResetReport resetConsumerOffsets(@Nonnull ConsumerOffsetResetSpec spec) {
    if (spec.getTopicName() != null && fetchTopic(spec.getTopicName()).isEmpty()) {
      throw new IllegalArgumentException("Topic not found: " + spec.getTopicName());
    }
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        String sql =
            "WITH head AS ("
                + "  SELECT topic_id, partition_id, COALESCE(MAX(enqueue_seq), 0) AS max_seq"
                + "  FROM "
                + tableNames.qualifiedMessage()
                + "  GROUP BY topic_id, partition_id"
                + "), candidates AS ("
                + "  SELECT co.id, co.consumer_group, t.topic_name, co.partition_id,"
                + "         co.offset_value AS previous_offset, h.max_seq AS new_offset"
                + "  FROM "
                + tableNames.qualifiedConsumerOffset()
                + " co"
                + "  INNER JOIN head h ON co.topic_id = h.topic_id AND co.partition_id = h.partition_id"
                + "  INNER JOIN "
                + tableNames.qualifiedTopic()
                + " t ON t.id = co.topic_id"
                + "  WHERE (? IS NULL OR co.consumer_group = ?)"
                + "    AND (? IS NULL OR t.topic_name = ?)"
                + "    AND (? IS NULL OR co.partition_id = ?)"
                + "    AND (NOT ? OR co.offset_value > h.max_seq)"
                + ") "
                + "UPDATE "
                + tableNames.qualifiedConsumerOffset()
                + " co"
                + " SET offset_value = c.new_offset, epoch = 0"
                + " FROM candidates c"
                + " WHERE co.id = c.id"
                + " RETURNING c.consumer_group, c.topic_name, c.partition_id,"
                + "           c.previous_offset, co.offset_value AS new_offset, c.new_offset AS max_seq";
        List<ConsumerOffsetResetDetail> resets = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          int idx = 1;
          String consumerGroup = spec.getConsumerGroup();
          if (consumerGroup != null) {
            ps.setString(idx++, consumerGroup);
            ps.setString(idx++, consumerGroup);
          } else {
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.VARCHAR);
          }
          String topicName = spec.getTopicName();
          if (topicName != null) {
            ps.setString(idx++, topicName);
            ps.setString(idx++, topicName);
          } else {
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.VARCHAR);
          }
          Integer partitionId = spec.getPartitionId();
          if (partitionId != null) {
            ps.setInt(idx++, partitionId);
            ps.setInt(idx++, partitionId);
          } else {
            ps.setNull(idx++, Types.INTEGER);
            ps.setNull(idx++, Types.INTEGER);
          }
          ps.setBoolean(idx, spec.isOnlyStuckAhead());
          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              resets.add(
                  ConsumerOffsetResetDetail.builder()
                      .consumerGroup(rs.getString(1))
                      .topicName(rs.getString(2))
                      .partitionId(rs.getInt(3))
                      .previousOffset(rs.getLong(4))
                      .newOffset(rs.getLong(5))
                      .maxSeq(rs.getLong(6))
                      .build());
            }
          }
        }
        tx.commit();
        return ConsumerOffsetResetReport.builder()
            .partitionsUpdated(resets.size())
            .resets(resets)
            .build();
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("resetConsumerOffsets failed", e);
      }
    }
  }

  @Override
  public void applyRetention() {
    try (Transaction tx = database.beginTransaction(TxScope.requiresNew())) {
      Connection conn = tx.connection();
      try {
        try (PreparedStatement ps =
            conn.prepareStatement("SELECT " + tableNames.qualifiedApplyRetention() + "()")) {
          ps.execute();
        }
        tx.commit();
      } catch (SQLException e) {
        tx.rollback();
        throw new IllegalStateException("applyRetention failed", e);
      }
    }
  }
}
