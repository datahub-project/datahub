package com.linkedin.metadata.queue.ebean;

import com.linkedin.metadata.queue.QueueTableNames;
import io.ebean.Model;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Maps the pgQueue consumer_offset table for default SqlSetup physical layout; see {@link
 * EbeanPgQueueTopic}.
 */
@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(
    name = EbeanPgQueueConsumerOffset.TABLE_NAME,
    schema = QueueTableNames.DEFAULT_SCHEMA,
    uniqueConstraints =
        @UniqueConstraint(columnNames = {"consumer_group", "topic_id", "partition_id"}))
public class EbeanPgQueueConsumerOffset extends Model {

  static final String TABLE_NAME = QueueTableNames.DEFAULT_TABLE_PREFIX + "_consumer_offset";

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "consumer_group", nullable = false, columnDefinition = "text")
  private String consumerGroup;

  @Column(name = "topic_id", nullable = false)
  private Long topicId;

  @Column(name = "partition_id", nullable = false)
  private Integer partitionId;

  @Column(name = "offset_value", nullable = false)
  private Long offsetValue;

  @Column(name = "epoch", nullable = false)
  private Long epoch;
}
