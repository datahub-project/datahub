package com.linkedin.metadata.config.postgres;

import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * Binds {@code postgres.*} from {@code application.yaml} for optional SqlSetup PostgreSQL DDL
 * (pgQueue).
 *
 * <p>Configuration defaults live in {@code application.yaml}, not on fields in this class.
 */
@ConfigurationProperties(prefix = "postgres")
@Getter
@Setter
public class PostgresSqlSetupProperties {

  /**
   * Allowlisted {@code postgres.pgQueue.retention.partmanPartitionInterval} values (pg_partman).
   */
  public static final Set<String> PGQUEUE_PARTMAN_PARTITION_INTERVALS =
      Set.of("1 hour", "6 hours", "12 hours", "1 day", "1 week", "1 month");

  /** Default same-JVM pgQueue consumer threads per topic when unset or non-positive in config. */
  public static final int PGQUEUE_TOPIC_DEFAULT_CONSUMER_CONCURRENCY = 1;

  public static final int PGQUEUE_TOPIC_CONSUMER_CONCURRENCY_MAX = 64;

  private static int normalizedPgQueueTopicConsumerConcurrency(int raw) {
    if (raw < PGQUEUE_TOPIC_DEFAULT_CONSUMER_CONCURRENCY) {
      return PGQUEUE_TOPIC_DEFAULT_CONSUMER_CONCURRENCY;
    }
    return Math.min(raw, PGQUEUE_TOPIC_CONSUMER_CONCURRENCY_MAX);
  }

  /** Same-JVM consumer threads cannot exceed this topic's {@code partitionCount}. */
  private static int capPgQueueConsumerConcurrencyToPartitions(
      int consumerConcurrency, int partitionCount) {
    return Math.min(consumerConcurrency, Math.max(1, partitionCount));
  }

  /**
   * PostgreSQL schema (namespace) for SqlSetup DDL and Ebean metadata tables. Defaults to {@code
   * public}; the JDBC URL supplies only the database name. Bound from {@code application.yaml} or
   * set by {@link #applySqlSetupSchemaFromJdbcUrl(String)} when {@code postgres.schema} is unset
   * (non-Spring callers).
   */
  private String schema;

  private PgQueue pgQueue = new PgQueue();
  private PgCron pgCron = new PgCron();

  /**
   * Optional Kafka topic catalog for {@link #buildPgQueueOptions()} when {@link
   * PgQueue#inheritKafkaTopics} is true. Set by Spring wiring ({@code ConfigurationProvider}) — not
   * bound from {@code postgres.*} YAML.
   */
  @Nullable private KafkaConfiguration kafkaConfiguration;

  /** Disables all optional SqlSetup PostgreSQL extension steps (for tests or non-Spring use). */
  public static PostgresSqlSetupProperties disabled() {
    PostgresSqlSetupProperties p = new PostgresSqlSetupProperties();
    p.getPgQueue().setEnabled(false);
    return p;
  }

  /**
   * Validates optional PostgreSQL SqlSetup extensions when the metadata store is PostgreSQL. No-op
   * for other database types (YAML may still carry {@code postgres.*} values).
   */
  public void validateForUse(DatabaseType dbType) {
    if (dbType != DatabaseType.POSTGRES) {
      return;
    }
    normalizedPostgresSchema();
    if (pgQueue.isEnabled()) {
      validatePgQueueConfig();
    }
    validatePgCronConfig(dbType);
  }

  private void validatePgCronConfig(DatabaseType dbType) {
    if (dbType != DatabaseType.POSTGRES) {
      return;
    }
    boolean cronNeeded = pgQueue.isEnabled() && pgQueue.getMaintenance().isCronEnabled();
    if (!cronNeeded) {
      return;
    }
    normalizedPgCronSchema();
    PgCron.Admin admin = pgCron.getAdmin();
    String jdbcUrl = admin != null ? admin.getJdbcUrl() : null;
    if (jdbcUrl == null || jdbcUrl.isBlank()) {
      throw new IllegalStateException(
          "postgres.pgCron.admin.jdbcUrl must be non-empty when pgQueue or pgTimeseries pg_cron "
              + "maintenance is enabled (configure under postgres.pgCron.admin in application.yaml).");
    }
  }

  /**
   * pg_cron extension schema (typically {@code cron}); SqlSetup registers jobs with fully qualified
   * {@code <cronSchema>.*}. pg_cron metadata and {@code CREATE EXTENSION pg_cron} use {@link
   * PgCron.Admin#getJdbcUrl()}; jobs execute in the application database via {@code
   * cron.schedule_in_database}.
   */
  public String normalizedPgCronSchema() {
    String raw = pgCron.getCronSchema();
    if (raw == null || raw.isBlank()) {
      return "cron";
    }
    return validateAndNormalizePostgresFeatureSchema(raw, "postgres.pgCron.cronSchema");
  }

  /** Built queue options, or null when {@code postgres.pgQueue.enabled} is false. */
  public PgQueueSetupOptions buildPgQueueOptions() {
    return buildPgQueueOptions(kafkaConfiguration);
  }

  /**
   * Same as {@link #buildPgQueueOptions()} but merges Kafka topic definitions when {@link
   * PgQueue#inheritKafkaTopics} is true (recommended). Pass {@link KafkaConfiguration} from Spring
   * so pgQueue catalog matches {@code kafka.topics.*}; pass null only in tests or when inheritance
   * is disabled.
   */
  public PgQueueSetupOptions buildPgQueueOptions(@Nullable KafkaConfiguration kafkaConfiguration) {
    if (!pgQueue.isEnabled()) {
      return null;
    }
    PgQueue.Retention retention = pgQueue.getRetention();
    int topicRetentionAge = pgQueue.getTopicDefaults().getRetentionMaxAgeSeconds();
    String partmanPartitionNormalized = retention.getPartmanPartitionInterval();
    if (partmanPartitionNormalized != null) {
      partmanPartitionNormalized = partmanPartitionNormalized.trim().toLowerCase();
    } else {
      partmanPartitionNormalized = "";
    }
    String contentTypeMime = pgQueue.getEffectiveDefaultContentTypeMime();
    if (contentTypeMime == null || contentTypeMime.isBlank()) {
      contentTypeMime = "application/avro";
    }
    return new PgQueueSetupOptions(
        normalizedPgQueueSchema(),
        normalizedPgQueueTablePrefix(),
        pgQueue.getTopicDefaults().getPartitionCount(),
        pgQueue.getTopicDefaults().getVisibilityTimeoutSeconds(),
        pgQueue.getTopicDefaults().getPriorityBands(),
        topicRetentionAge,
        pgQueue.getTopicDefaults().getMaxRowsPerTopic(),
        pgQueue.getTopicDefaults().getMaxTotalPayloadBytesPerTopic(),
        contentTypeMime,
        partmanPartitionNormalized,
        retention.getPartmanPremake(),
        pgQueue.getMaintenance().isCronEnabled(),
        pgQueue.getMaintenance().getIntervalSeconds(),
        pgQueue.getMaintenance().getBatchDeleteLimit(),
        pgQueue.getTopicDefaults().isAggressiveRetention(),
        capPgQueueConsumerConcurrencyToPartitions(
            normalizedPgQueueTopicConsumerConcurrency(
                pgQueue.getTopicDefaults().getConsumerConcurrency()),
            pgQueue.getTopicDefaults().getPartitionCount()),
        buildPgQueueResolvedTopicCatalog(kafkaConfiguration));
  }

  private List<PgQueueResolvedTopicCatalogEntry> buildPgQueueResolvedTopicCatalog(
      @Nullable KafkaConfiguration kafkaConfiguration) {
    Map<String, PgQueueTopicOverride> topicMap = mergeKafkaAndPgQueueTopicMaps(kafkaConfiguration);
    if (topicMap.isEmpty()) {
      return List.of();
    }
    PgQueue.TopicDefaults defaults = pgQueue.getTopicDefaults();
    List<PgQueueResolvedTopicCatalogEntry> out = new ArrayList<>();
    for (Map.Entry<String, PgQueueTopicOverride> e : topicMap.entrySet()) {
      PgQueueTopicOverride o = e.getValue();
      if (o == null) {
        continue;
      }
      if (o.getTopicName() == null || o.getTopicName().isBlank()) {
        throw new IllegalStateException(
            "postgres.pgQueue.topics."
                + e.getKey()
                + " must resolve to a non-empty topicName "
                + "(set topicName or enable postgres.pgQueue.inheritKafkaTopics with a matching kafka.topics entry).");
      }
      int partitionCount =
          o.getPartitionCount() != null ? o.getPartitionCount() : defaults.getPartitionCount();
      String priorityBands =
          o.getPriorityBands() != null ? o.getPriorityBands() : defaults.getPriorityBands();
      int retentionMaxAgeSeconds =
          o.getRetentionMaxAgeSeconds() != null
              ? o.getRetentionMaxAgeSeconds()
              : defaults.getRetentionMaxAgeSeconds();
      long maxRows =
          o.getMaxRowsPerTopic() != null ? o.getMaxRowsPerTopic() : defaults.getMaxRowsPerTopic();
      long maxBytes =
          o.getMaxTotalPayloadBytesPerTopic() != null
              ? o.getMaxTotalPayloadBytesPerTopic()
              : defaults.getMaxTotalPayloadBytesPerTopic();
      boolean aggressiveRetention =
          o.getAggressiveRetention() != null
              ? o.getAggressiveRetention()
              : defaults.isAggressiveRetention();
      int consumerConcurrency =
          capPgQueueConsumerConcurrencyToPartitions(
              normalizedPgQueueTopicConsumerConcurrency(
                  o.getConsumerConcurrency() != null
                      ? o.getConsumerConcurrency()
                      : defaults.getConsumerConcurrency()),
              partitionCount);
      out.add(
          new PgQueueResolvedTopicCatalogEntry(
              e.getKey(),
              o.getTopicName().trim(),
              partitionCount,
              priorityBands,
              retentionMaxAgeSeconds,
              maxRows,
              maxBytes,
              aggressiveRetention,
              consumerConcurrency));
    }
    return List.copyOf(out);
  }

  /**
   * Starts from enabled {@code kafka.topics.*} entries (topic name, partitions, retention from
   * {@code retention.ms} when set), then merges {@code postgres.pgQueue.topics.*} so pgQueue can
   * override any field or add pg-only topics.
   */
  private Map<String, PgQueueTopicOverride> mergeKafkaAndPgQueueTopicMaps(
      @Nullable KafkaConfiguration kafkaConfiguration) {
    LinkedHashMap<String, PgQueueTopicOverride> merged = new LinkedHashMap<>();
    if (pgQueue.isInheritKafkaTopics() && kafkaConfiguration != null) {
      TopicsConfiguration topicsConfiguration = kafkaConfiguration.getTopics();
      Map<String, TopicsConfiguration.TopicConfiguration> kafkaTopics =
          topicsConfiguration != null ? topicsConfiguration.getTopics() : null;
      if (kafkaTopics != null) {
        for (Map.Entry<String, TopicsConfiguration.TopicConfiguration> e : kafkaTopics.entrySet()) {
          TopicsConfiguration.TopicConfiguration kc = e.getValue();
          if (kc == null) {
            continue;
          }
          if (Boolean.FALSE.equals(kc.getEnabled())) {
            continue;
          }
          if (kc.getName() == null || kc.getName().isBlank()) {
            continue;
          }
          PgQueueTopicOverride o = new PgQueueTopicOverride();
          o.setTopicName(kc.getName().trim());
          if (kc.getPartitions() != null) {
            o.setPartitionCount(kc.getPartitions());
          }
          Integer retentionSec = retentionSecondsFromKafkaRetentionMs(kc.getConfigProperties());
          if (retentionSec != null) {
            o.setRetentionMaxAgeSeconds(retentionSec);
          }
          merged.put(e.getKey(), o);
        }
      }
    }
    Map<String, PgQueueTopicOverride> yamlTopics = pgQueue.getTopics();
    if (yamlTopics != null) {
      for (Map.Entry<String, PgQueueTopicOverride> e : yamlTopics.entrySet()) {
        String key = e.getKey();
        PgQueueTopicOverride overlay = e.getValue();
        if (overlay == null) {
          continue;
        }
        PgQueueTopicOverride base = merged.get(key);
        merged.put(key, mergePgQueueTopicLayers(base, overlay));
      }
    }
    return merged;
  }

  private static PgQueueTopicOverride mergePgQueueTopicLayers(
      @Nullable PgQueueTopicOverride base, @NonNull PgQueueTopicOverride overlay) {
    PgQueueTopicOverride out = new PgQueueTopicOverride();
    String topicName =
        overlay.getTopicName() != null && !overlay.getTopicName().isBlank()
            ? overlay.getTopicName().trim()
            : base != null ? base.getTopicName() : null;
    out.setTopicName(topicName);
    out.setPartitionCount(
        overlay.getPartitionCount() != null
            ? overlay.getPartitionCount()
            : base != null ? base.getPartitionCount() : null);
    out.setPriorityBands(
        overlay.getPriorityBands() != null
            ? overlay.getPriorityBands()
            : base != null ? base.getPriorityBands() : null);
    out.setRetentionMaxAgeSeconds(
        overlay.getRetentionMaxAgeSeconds() != null
            ? overlay.getRetentionMaxAgeSeconds()
            : base != null ? base.getRetentionMaxAgeSeconds() : null);
    out.setMaxRowsPerTopic(
        overlay.getMaxRowsPerTopic() != null
            ? overlay.getMaxRowsPerTopic()
            : base != null ? base.getMaxRowsPerTopic() : null);
    out.setMaxTotalPayloadBytesPerTopic(
        overlay.getMaxTotalPayloadBytesPerTopic() != null
            ? overlay.getMaxTotalPayloadBytesPerTopic()
            : base != null ? base.getMaxTotalPayloadBytesPerTopic() : null);
    out.setAggressiveRetention(
        overlay.getAggressiveRetention() != null
            ? overlay.getAggressiveRetention()
            : base != null ? base.getAggressiveRetention() : null);
    out.setConsumerConcurrency(
        overlay.getConsumerConcurrency() != null
            ? overlay.getConsumerConcurrency()
            : base != null ? base.getConsumerConcurrency() : null);
    return out;
  }

  /**
   * Maps Kafka {@code retention.ms} to pgQueue {@code retention_max_age_seconds}; {@code -1} or
   * invalid becomes {@code 0} (no age-based trimming).
   */
  static Integer retentionSecondsFromKafkaRetentionMs(
      @Nullable Map<String, String> configProperties) {
    if (configProperties == null) {
      return null;
    }
    String ms = configProperties.get("retention.ms");
    if (ms == null) {
      return null;
    }
    try {
      long msVal = Long.parseLong(ms.trim());
      if (msVal < 0) {
        return 0;
      }
      long sec = msVal / 1000;
      return sec > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) sec;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Largest merged {@code retention_max_age_seconds} across configured {@code
   * postgres.pgQueue.topics} entries (0 if none).
   */
  public int maxMergedPgQueueTopicRetentionMaxAgeSeconds() {
    if (!pgQueue.isEnabled()) {
      return 0;
    }
    return buildPgQueueResolvedTopicCatalog(null).stream()
        .mapToInt(PgQueueResolvedTopicCatalogEntry::getRetentionMaxAgeSeconds)
        .max()
        .orElse(0);
  }

  /**
   * Derives {@code partman.part_config.retention} from the longest age-based retention in effect:
   * {@code max(topicDefaults.retentionMaxAgeSeconds, max(topic.retention_max_age_seconds))}, plus a
   * buffer of two partition widths so partman never drops partitions before row-level retention can
   * delete completed rows.
   *
   * @param normalizedPartitionInterval lower-cased allowlisted interval, e.g. {@code "1 day"}
   */
  public static String resolvePartmanPartitionRetentionIntervalText(
      int topicDefaultRetentionMaxAgeSeconds,
      int maxRetentionMaxAgeSecondsFromTopicRows,
      String normalizedPartitionInterval) {
    int effective =
        Math.max(topicDefaultRetentionMaxAgeSeconds, maxRetentionMaxAgeSecondsFromTopicRows);
    if (effective <= 0) {
      return null;
    }
    return formatPartmanRetentionIntervalText(
        effective, 2 * approximatePartitionSeconds(normalizedPartitionInterval));
  }

  /**
   * Visible for tests; pg_partman {@code part_config.retention} text (PostgreSQL interval input).
   */
  public static String formatPartmanRetentionIntervalText(
      int retentionMaxAgeSeconds, long bufferSeconds) {
    long total = (long) retentionMaxAgeSeconds + bufferSeconds;
    if (total <= 0) {
      return "1 day";
    }
    if (total % 86400 == 0) {
      return (total / 86400) + " days";
    }
    if (total % 3600 == 0) {
      return (total / 3600) + " hours";
    }
    return total + " seconds";
  }

  public static long approximatePartitionSeconds(String normalizedPartmanInterval) {
    switch (normalizedPartmanInterval) {
      case "1 hour":
        return 3600;
      case "6 hours":
        return 6 * 3600L;
      case "12 hours":
        return 12 * 3600L;
      case "1 day":
        return 86400;
      case "1 week":
        return 7 * 86400L;
      case "1 month":
        return 31 * 86400L;
      default:
        return 86400;
    }
  }

  /**
   * When {@link #schema} is unset and {@code jdbcUrl} targets PostgreSQL, sets it to {@code
   * public}. The database name comes only from the JDBC URL path; application DDL does not use a
   * separate schema named after the database.
   */
  public void applySqlSetupSchemaFromJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
      return;
    }
    try {
      JdbcUrlParser.JdbcInfo info = JdbcUrlParser.parseJdbcUrl(jdbcUrl.trim());
      if (info.databaseType != DatabaseType.POSTGRES) {
        return;
      }
      if (schema == null || schema.isBlank()) {
        setSchema("public");
      }
    } catch (IllegalArgumentException ignored) {
      // Leave schema unchanged if URL cannot be parsed.
    }
  }

  /**
   * Returns {@code postgres.schema} normalized to a valid unquoted PostgreSQL identifier (throws
   * {@link IllegalStateException} if invalid).
   */
  public String normalizedPostgresSchema() {
    return validateAndNormalizePostgresFeatureSchema(schema, "postgres.schema");
  }

  /**
   * Normalized {@code postgres.pgQueue.schema}: PostgreSQL namespace for pgQueue SqlSetup DDL
   * (default {@code queue}), separate from {@link #normalizedPostgresSchema()}.
   */
  public String normalizedPgQueueSchema() {
    return validateAndNormalizePostgresFeatureSchema(
        pgQueue.getSchema(), "postgres.pgQueue.schema");
  }

  /** Normalized {@code postgres.pgQueue.tablePrefix}. */
  public String normalizedPgQueueTablePrefix() {
    return normalizeTablePrefix(pgQueue.getTablePrefix(), "postgres.pgQueue.tablePrefix");
  }

  /**
   * Table name prefix segment (letters, digits, underscore); combined with fixed suffixes in
   * SqlSetup SQL (e.g. {@code prefix + "_topic"}).
   */
  public static String normalizeTablePrefix(String raw, String yamlPropertyPath) {
    if (raw == null || raw.trim().isEmpty()) {
      throw new IllegalStateException(yamlPropertyPath + " must be non-empty.");
    }
    String s = raw.trim();
    if (!s.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
      throw new IllegalStateException(
          yamlPropertyPath
              + " must be a valid unquoted PostgreSQL identifier fragment "
              + "(letters, digits, underscore; must not start with a digit).");
    }
    return s.toLowerCase();
  }

  private static String validateAndNormalizePostgresFeatureSchema(
      String raw, String yamlPropertyPath) {
    if (raw == null || raw.trim().isEmpty()) {
      throw new IllegalStateException(yamlPropertyPath + " must be non-empty.");
    }
    String s = raw.trim();
    if (!s.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
      throw new IllegalStateException(
          yamlPropertyPath
              + " must be a valid unquoted PostgreSQL identifier "
              + "(letters, digits, underscore; must not start with a digit).");
    }
    return s.toLowerCase();
  }

  private void validatePgQueueConfig() {
    normalizedPgQueueSchema();
    normalizedPgQueueTablePrefix();
    PgQueue.TopicDefaults t = pgQueue.getTopicDefaults();
    if (t.getPartitionCount() < 1 || t.getPartitionCount() > 4096) {
      throw new IllegalStateException(
          "postgres.pgQueue.topicDefaults.partitionCount must be between 1 and 4096 inclusive.");
    }
    if (t.getVisibilityTimeoutSeconds() < 1 || t.getVisibilityTimeoutSeconds() > 86400 * 7) {
      throw new IllegalStateException(
          "postgres.pgQueue.topicDefaults.visibilityTimeoutSeconds must be between 1 and 604800 inclusive.");
    }
    if (t.getRetentionMaxAgeSeconds() < 0) {
      throw new IllegalStateException(
          "postgres.pgQueue.topicDefaults.retentionMaxAgeSeconds must be non-negative (0 disables).");
    }
    if (t.getRetentionMaxAgeSeconds() > 0 && t.getRetentionMaxAgeSeconds() < 60) {
      throw new IllegalStateException(
          "postgres.pgQueue.topicDefaults.retentionMaxAgeSeconds must be 0 or at least 60 when enabled.");
    }
    if (t.getMaxRowsPerTopic() < 0) {
      throw new IllegalStateException(
          "postgres.pgQueue.topicDefaults.maxRowsPerTopic must be non-negative (0 disables).");
    }
    if (t.getMaxTotalPayloadBytesPerTopic() < 0) {
      throw new IllegalStateException(
          "postgres.pgQueue.topicDefaults.maxTotalPayloadBytesPerTopic must be non-negative (0 disables).");
    }
    if (t.getConsumerConcurrency() < 0
        || t.getConsumerConcurrency() > PGQUEUE_TOPIC_CONSUMER_CONCURRENCY_MAX) {
      throw new IllegalStateException(
          "postgres.pgQueue.topicDefaults.consumerConcurrency must be between 0 and "
              + PGQUEUE_TOPIC_CONSUMER_CONCURRENCY_MAX
              + " inclusive (0 uses default "
              + PGQUEUE_TOPIC_DEFAULT_CONSUMER_CONCURRENCY
              + ").");
    }
    PgQueue.Retention r = pgQueue.getRetention();
    if (r.getPartmanPartitionInterval() == null
        || r.getPartmanPartitionInterval().trim().isEmpty()) {
      throw new IllegalStateException(
          "postgres.pgQueue.retention.partmanPartitionInterval must be non-empty.");
    }
    String piQueue = r.getPartmanPartitionInterval().trim().toLowerCase();
    if (!PGQUEUE_PARTMAN_PARTITION_INTERVALS.contains(piQueue)) {
      throw new IllegalStateException(
          "postgres.pgQueue.retention.partmanPartitionInterval must be one of "
              + PGQUEUE_PARTMAN_PARTITION_INTERVALS
              + " (got: "
              + r.getPartmanPartitionInterval()
              + ").");
    }
    if (r.getPartmanPremake() < 1 || r.getPartmanPremake() > 128) {
      throw new IllegalStateException(
          "postgres.pgQueue.retention.partmanPremake must be between 1 and 128 inclusive.");
    }
    PgQueue.Maintenance m = pgQueue.getMaintenance();
    if (m.isCronEnabled()) {
      if (m.getIntervalSeconds() < 60 || m.getIntervalSeconds() > 86400 * 30) {
        throw new IllegalStateException(
            "postgres.pgQueue.maintenance.intervalSeconds must be between 60 and 2592000 inclusive when cron is enabled.");
      }
    }
    if (m.getBatchDeleteLimit() < 1 || m.getBatchDeleteLimit() > 100_000) {
      throw new IllegalStateException(
          "postgres.pgQueue.maintenance.batchDeleteLimit must be between 1 and 100000 inclusive.");
    }
    String payloadCompression = pgQueue.getEffectivePayloadCompression();
    if (payloadCompression == null || payloadCompression.isBlank()) {
      throw new IllegalStateException(
          "postgres.pgQueue.producer.payloadCompression (or deprecated postgres.pgQueue.payloadCompression) must be non-empty.");
    }
    String pc = payloadCompression.trim().toUpperCase(Locale.ROOT);
    if (!Set.of("NONE", "SNAPPY").contains(pc)) {
      throw new IllegalStateException(
          "postgres.pgQueue.producer.payloadCompression must be NONE or SNAPPY (got: "
              + payloadCompression
              + ").");
    }
    Map<String, PgQueueTopicOverride> topicOverrides = pgQueue.getTopics();
    if (topicOverrides != null) {
      for (Map.Entry<String, PgQueueTopicOverride> e : topicOverrides.entrySet()) {
        PgQueueTopicOverride o = e.getValue();
        if (o == null) {
          continue;
        }
        String pfx = "postgres.pgQueue.topics." + e.getKey() + ".";
        if (o.getTopicName() != null && o.getTopicName().isBlank()) {
          throw new IllegalStateException(pfx + "topicName must not be blank.");
        }
        if (!pgQueue.isInheritKafkaTopics()
            && (o.getTopicName() == null || o.getTopicName().isBlank())) {
          throw new IllegalStateException(pfx + "topicName must be non-empty.");
        }
        if (o.getPartitionCount() != null
            && (o.getPartitionCount() < 1 || o.getPartitionCount() > 4096)) {
          throw new IllegalStateException(
              pfx + "partitionCount must be between 1 and 4096 inclusive.");
        }
        if (o.getRetentionMaxAgeSeconds() != null) {
          if (o.getRetentionMaxAgeSeconds() < 0) {
            throw new IllegalStateException(pfx + "retentionMaxAgeSeconds must be non-negative.");
          }
          if (o.getRetentionMaxAgeSeconds() > 0 && o.getRetentionMaxAgeSeconds() < 60) {
            throw new IllegalStateException(
                pfx + "retentionMaxAgeSeconds must be 0 or at least 60 when enabled.");
          }
        }
        if (o.getMaxRowsPerTopic() != null && o.getMaxRowsPerTopic() < 0) {
          throw new IllegalStateException(pfx + "maxRowsPerTopic must be non-negative.");
        }
        if (o.getMaxTotalPayloadBytesPerTopic() != null
            && o.getMaxTotalPayloadBytesPerTopic() < 0) {
          throw new IllegalStateException(
              pfx + "maxTotalPayloadBytesPerTopic must be non-negative.");
        }
        if (o.getConsumerConcurrency() != null
            && (o.getConsumerConcurrency() < 0
                || o.getConsumerConcurrency() > PGQUEUE_TOPIC_CONSUMER_CONCURRENCY_MAX)) {
          throw new IllegalStateException(
              pfx
                  + "consumerConcurrency must be between 0 and "
                  + PGQUEUE_TOPIC_CONSUMER_CONCURRENCY_MAX
                  + " inclusive (0 uses default "
                  + PGQUEUE_TOPIC_DEFAULT_CONSUMER_CONCURRENCY
                  + ").");
        }
      }
    }
  }

  @Getter
  @Setter
  public static class PgQueue {
    private boolean enabled;

    /**
     * PostgreSQL schema for pgQueue SqlSetup objects (tables, functions). Default in {@code
     * application.yaml} is {@code queue} ({@code DATAHUB_PGQUEUE_SCHEMA}); this is separate from
     * {@link PostgresSqlSetupProperties#schema} (metadata/Ebean, typically {@code public}).
     */
    private String schema;

    /** Prefix for SqlSetup queue tables, e.g. {@code metadata_queue_topic}. */
    private String tablePrefix;

    /**
     * Application-layer compression for new message payloads ({@code message.payload_compression}).
     * Allowlisted: {@code NONE}, {@code SNAPPY}. Separate from PostgreSQL TOAST storage
     * compression. Default is defined in {@code application.yaml} ({@code
     * postgres.pgQueue.payloadCompression}), not on this field.
     *
     * @deprecated Use {@link Producer#payloadCompression} via {@code
     *     postgres.pgQueue.producer.payloadCompression} instead. This field is kept for backward
     *     compatibility and will be removed in a future release. When the new path is set it takes
     *     precedence.
     */
    @Deprecated private String payloadCompression;

    /**
     * When true (default), SqlSetup builds the pgQueue topic catalog from {@code kafka.topics.*}
     * (enabled topics with a non-empty name), then merges {@link #topics} for overrides or extra
     * pg-only topics. When false, only {@link #topics} is used (legacy).
     */
    private boolean inheritKafkaTopics = true;

    private Producer producer = new Producer();

    /**
     * Returns the effective payload compression setting, preferring the new {@link
     * Producer#payloadCompression} path when set, falling back to the deprecated top-level field.
     */
    public String getEffectivePayloadCompression() {
      if (producer.getPayloadCompression() != null && !producer.getPayloadCompression().isBlank()) {
        return producer.getPayloadCompression();
      }
      return payloadCompression;
    }

    /**
     * Returns the effective default content type MIME, preferring the new {@link
     * Producer#defaultContentTypeMime} path when set, falling back to the deprecated {@link
     * TopicDefaults#defaultContentTypeMime} field.
     */
    public String getEffectiveDefaultContentTypeMime() {
      if (producer.getDefaultContentTypeMime() != null
          && !producer.getDefaultContentTypeMime().isBlank()) {
        return producer.getDefaultContentTypeMime();
      }
      return topicDefaults.getDefaultContentTypeMime();
    }

    private TopicDefaults topicDefaults = new TopicDefaults();

    /**
     * Optional overrides/additions; logical keys align with {@code kafka.topics.*}. When {@link
     * #inheritKafkaTopics} is true, omitted fields fall through to Kafka-derived values; {@code
     * topicName} may be omitted when Kafka defines the same key.
     */
    private Map<String, PgQueueTopicOverride> topics = new HashMap<>();

    private Retention retention = new Retention();
    private Maintenance maintenance = new Maintenance();
    private ConsumerPoll consumerPoll;

    @Getter
    @Setter
    public static class ConsumerPoll {
      /** Sleep when a poll returns no messages (most pipelines). */
      private Long emptyPollSleepMillis;

      /**
       * Shorter empty-poll sleep for MCL hook pollers; falls back to {@link #emptyPollSleepMillis}.
       */
      private Long mclEmptyPollSleepMillis;

      /** Sleep when the logical topic is not registered in pgQueue. */
      private Long missingTopicSleepMillis;

      /** Sleep after an unexpected poll/processing error before retrying. */
      private Long errorRecoverySleepMillis;
    }

    @Getter
    @Setter
    public static class Retention {
      /**
       * Allowlisted values: see {@link
       * PostgresSqlSetupProperties#PGQUEUE_PARTMAN_PARTITION_INTERVALS}.
       */
      private String partmanPartitionInterval;

      private int partmanPremake;
    }

    @Getter
    @Setter
    public static class TopicDefaults {
      private int partitionCount;
      private int visibilityTimeoutSeconds;
      private String priorityBands;
      private int retentionMaxAgeSeconds;
      private long maxRowsPerTopic;
      private long maxTotalPayloadBytesPerTopic;

      /**
       * Stored as {@code topic.default_content_type_id} via {@code *_content_type.mime}.
       *
       * @deprecated Use {@link Producer#defaultContentTypeMime} via {@code
       *     postgres.pgQueue.producer.defaultContentTypeMime} instead. This field is kept for
       *     backward compatibility and will be removed in a future release.
       */
      @Deprecated private String defaultContentTypeMime;

      /**
       * When true, topics default to aggressive retention: messages are purged as soon as all
       * registered consumers have advanced their offsets past them.
       */
      private boolean aggressiveRetention;

      /**
       * Same-JVM pgQueue consumer threads per topic when not overridden under {@link
       * PgQueue#topics} (1 = one poll thread per registration per topic set). Non-positive values
       * are normalized to 1 when building {@link PgQueueSetupOptions}. The resolved value is also
       * capped to {@link #partitionCount}.
       */
      private int consumerConcurrency = PGQUEUE_TOPIC_DEFAULT_CONSUMER_CONCURRENCY;
    }

    /**
     * Producer-specific settings for pgQueue message publishing. These control how the producer
     * encodes and compresses messages before enqueuing.
     */
    @Getter
    @Setter
    public static class Producer {
      /**
       * Application-layer compression for new message payloads ({@code
       * message.payload_compression}). Allowlisted: {@code NONE}, {@code SNAPPY}. When set, takes
       * precedence over the deprecated top-level {@link PgQueue#payloadCompression}.
       */
      private String payloadCompression;

      /**
       * MIME type for topic default content type. When set, takes precedence over the deprecated
       * {@link TopicDefaults#defaultContentTypeMime}. Stored as {@code
       * topic.default_content_type_id} via {@code *_content_type.mime}.
       */
      private String defaultContentTypeMime;
    }

    @Getter
    @Setter
    public static class Maintenance {
      private boolean cronEnabled;
      private int intervalSeconds;
      private int batchDeleteLimit;
    }
  }

  /**
   * pg_cron registry configuration. Extension DDL and {@code cron.*} metadata use {@link
   * PgCron.Admin}; scheduled commands run in the application database (from {@code ebean.url}) via
   * {@code cron.schedule_in_database}.
   */
  @Getter
  @Setter
  public static class PgCron {
    /**
     * Schema for pg_cron system objects (default {@code cron}). SqlSetup registers jobs with fully
     * qualified references ({@code cron.schedule_in_database}, {@code cron.job}, …); application
     * DDL uses {@link PostgresSqlSetupProperties#schema} separately.
     */
    private String cronSchema;

    /**
     * Connection to the DB named by {@code postgresql.conf} {@code cron.database_name} (pg_cron).
     */
    private Admin admin = new Admin();

    /**
     * Cross-cloud IAM for the pg_cron admin connection; mirrors {@code ebean.*} IAM flags and cloud
     * env defaults (see application.yaml).
     */
    private Iam iam = new Iam();

    @Getter
    @Setter
    public static class Admin {
      /**
       * JDBC URL for pg_cron ({@code CREATE EXTENSION} + job registry); set in application.yaml.
       */
      private String jdbcUrl;

      /**
       * Credentials for {@link #jdbcUrl}; defaults match {@code EBEAN_DATASOURCE_*} in
       * application.yaml.
       */
      private String username;

      private String password;

      /** JDBC driver class for IAM transformation (defaults to {@code ebean.driver}). */
      private String driver;
    }

    @Getter
    @Setter
    public static class Iam {
      private boolean useIamAuth;
      private boolean postgresUseIamAuth;
      private String cloudProvider;
      private String awsRegion;
      private String awsAccessKeyId;
      private String awsSecretAccessKey;
      private String awsSessionToken;
      private String googleApplicationCredentials;
      private String gcpProject;
      private String instanceConnectionName;
    }
  }
}
