package com.linkedin.mxe;

import com.linkedin.common.urn.Urn;
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
   * Note that MetadataChangeEvents are deprecated, replaced by {@link MetadataChangeProposal}.
   */
  @Nonnull
  @Deprecated
  String getMetadataChangeEventTopicName();

  /**
   * The name of the metadata audit event (v4) kafka topic.
   * Note that MetadataAuditEvents are deprecated, replaced by {@link MetadataChangeLog}.
   */
  @Nonnull
  @Deprecated
  String getMetadataAuditEventTopicName();

  /**
   * The name of the failed metadata change event (v4) kafka topic.
   * Note that FailedMetadataChangeEvents are deprecated, replaced by {@link FailedMetadataChangeProposal}.
   */
  @Nonnull
  @Deprecated
  String getFailedMetadataChangeEventTopicName();

  /**
   * The name of the metadata change proposal kafka topic.
   */
  @Nonnull
  String getMetadataChangeProposalTopicName();

  /**
   * The name of the metadata change log kafka topic.
   */
  @Nonnull
  String getMetadataChangeLogVersionedTopicName();

  /**
   * The name of the metadata change log kafka topic with limited retention.
   */
  @Nonnull
  String getMetadataChangeLogTimeseriesTopicName();

  /**
   * The name of the failed metadata change proposal kafka topic.
   */
  @Nonnull
  String getFailedMetadataChangeProposalTopicName();

  /**
   * The name of the platform event topic.
   */
  @Nonnull
  String getPlatformEventTopicName();

  /**
   * Returns the name of the metadata change event (v5) kafka topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Nonnull
  @Deprecated
  String getMetadataChangeEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the avro class that defines the given MCE v5 topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Deprecated
  Class<? extends SpecificRecord> getMetadataChangeEventType(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the name of the metadata audit event (v5) kafka topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Nonnull
  @Deprecated
  String getMetadataAuditEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the avro class that defines the given MAE v5 topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Deprecated
  Class<? extends SpecificRecord> getMetadataAuditEventType(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);


  /**
   * Returns the name of the failed metadata change event (v5) kafka topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Nonnull
  @Deprecated
  String getFailedMetadataChangeEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);

  /**
   * Returns the avro class that defines the given FMCE v5 topic.
   *
   * @param urn the urn of the entity being updated
   * @param aspect the aspect name being updated
   */
  @Deprecated
  Class<? extends SpecificRecord> getFailedMetadataChangeEventType(@Nonnull Urn urn, @Nonnull RecordTemplate aspect);
}
