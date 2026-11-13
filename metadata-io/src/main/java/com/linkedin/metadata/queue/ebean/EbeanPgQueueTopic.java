package com.linkedin.metadata.queue.ebean;

import com.linkedin.metadata.queue.QueueTableNames;
import io.ebean.Model;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.sql.Timestamp;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Maps the pgQueue topic catalog table when physical layout matches SqlSetup defaults ({@link
 * QueueTableNames#DEFAULT_SCHEMA} + {@link QueueTableNames#DEFAULT_TABLE_PREFIX}). For non-default
 * schema/prefix, callers should use native SQL via {@link
 * com.linkedin.metadata.queue.postgres.EbeanPostgresMetadataQueueStore}.
 */
@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name = EbeanPgQueueTopic.TABLE_NAME, schema = QueueTableNames.DEFAULT_SCHEMA)
public class EbeanPgQueueTopic extends Model {

  static final String TABLE_NAME = QueueTableNames.DEFAULT_TABLE_PREFIX + "_topic";

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "topic_name", nullable = false, unique = true, columnDefinition = "text")
  private String topicName;

  @Column(name = "partition_count", nullable = false)
  private Integer partitionCount;

  @Column(name = "retention_max_age_seconds", nullable = false)
  private Integer retentionMaxAgeSeconds;

  @Column(name = "max_rows_per_topic", nullable = false)
  private Long maxRowsPerTopic;

  @Column(name = "max_total_payload_bytes", nullable = false)
  private Long maxTotalPayloadBytesPerTopic;

  @Nullable
  @Column(name = "default_content_type_id")
  private Integer defaultContentTypeId;

  @Nullable
  @Column(name = "created_at")
  private Timestamp createdAt;
}
