package com.linkedin.metadata.queue;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Qualified pgQueue SqlSetup table names ({@code schema}.{prefix}_topic}, etc.). Identifiers come
 * from validated/normalized {@link PostgresSqlSetupProperties} and are safe for concatenation into
 * SQL (allowlisted charset: letters, digits, underscore).
 */
@Immutable
public final class QueueTableNames {

  /**
   * Default SqlSetup layout matching {@link com.linkedin.metadata.queue.ebean.EbeanPgQueueTopic}.
   */
  public static final String DEFAULT_SCHEMA = "queue";

  public static final String DEFAULT_TABLE_PREFIX = "metadata_queue";

  private final String schema;
  private final String prefix;

  public QueueTableNames(@Nonnull String normalizedSchema, @Nonnull String normalizedTablePrefix) {
    this.schema = normalizedSchema;
    this.prefix = normalizedTablePrefix;
  }

  @Nonnull
  public static QueueTableNames fromPostgresProperties(
      @Nonnull PostgresSqlSetupProperties properties) {
    return new QueueTableNames(
        properties.normalizedPgQueueSchema(), properties.normalizedPgQueueTablePrefix());
  }

  @Nonnull
  public String schema() {
    return schema;
  }

  @Nonnull
  public String tablePrefix() {
    return prefix;
  }

  @Nonnull
  public String qualifiedTopic() {
    return schema + "." + prefix + "_topic";
  }

  @Nonnull
  public String qualifiedMessage() {
    return schema + "." + prefix + "_message";
  }

  @Nonnull
  public String qualifiedConsumerOffset() {
    return schema + "." + prefix + "_consumer_offset";
  }

  /**
   * Per-consumer-group leases ({@code prefix_message_group_lease}) for parallel Kafka-style groups
   * on one message payload row.
   */
  @Nonnull
  public String qualifiedMessageGroupLease() {
    return schema + "." + prefix + "_message_group_lease";
  }

  @Nonnull
  public String qualifiedContentType() {
    return schema + "." + prefix + "_content_type";
  }

  @Nonnull
  public String qualifiedConsumerRegistration() {
    return schema + "." + prefix + "_consumer_registration";
  }

  @Nonnull
  public String qualifiedApplyRetention() {
    return schema + "." + prefix + "_apply_retention";
  }

  /**
   * When true, physical tables match {@link com.linkedin.metadata.queue.ebean.EbeanPgQueueTopic} /
   * {@link com.linkedin.metadata.queue.ebean.EbeanPgQueueConsumerOffset} mapping and Ebean entity
   * finders may be used for reads. Non-default schema or prefix requires native SQL only.
   */
  public boolean matchesDefaultEntityPhysicalMapping() {
    return DEFAULT_SCHEMA.equals(schema) && DEFAULT_TABLE_PREFIX.equals(prefix);
  }
}
