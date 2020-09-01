package com.linkedin.mxe;

import com.linkedin.common.Urn;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;
import org.apache.avro.specific.SpecificRecord;


/**
 * The convention for naming kafka topics.
 *
 * <p>Different companies may have different naming conventions or styles for their kafka topics. Namely, companies
 * should pick _ or . as a delimiter, but not both, as they collide in metric names.
 */
public interface TopicConvention {
  /**
   * The name of the metadata change event (v4) kafka topic.
   */
  @Nonnull
  String getMetadataChangeEventTopicName();

  /**
   * The name of the metadata audit event (v4) kafka topic.
   */
  @Nonnull
  String getMetadataAuditEventTopicName();

  /**
   * The name of the failed metadata change event (v4) kafka topic.
   */
  @Nonnull
  String getFailedMetadataChangeEventTopicName();

  /**
   * Returns the name of the metadata audit event (v5) kafka topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Nonnull
  String getMetadataAuditEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the avro class that defines the given MAE v5 topic.
   */
  Class<? extends SpecificRecord> getMetadataAuditEventType(@Nonnull String topicName);
}
