package com.linkedin.mxe;

import com.linkedin.common.Urn;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


/**
 * Convention for standard Kafka topics, and is configurable to use different delimiters.
 *
 * <p>By default, uses an underscore (_) as the delimiter.
 */
public final class StandardTopicConvention implements TopicConvention {
  private static final String DEFAULT_DELIMiTER = "_";
  private static final String METADATA_CHANGE_EVENT = "MetadataChangeEvent";
  private static final String METADATA_AUDIT_EVENT = "MetadataAuditEvent";
  private static final String FAILED_METADATA_CHANGE_EVENT = "FailedMetadataChangeEvent";
  private static final String VERSION_4 = "v4";

  private final String _delimiter;

  /**
   * Constructs a convention with the given delimiter.
   *
   * @param delimiter the delimiter between words in a topic
   */
  public StandardTopicConvention(@Nonnull String delimiter) {
    _delimiter = delimiter;
  }

  /**
   * Constructs a convention with the default delimiter of underscore.
   */
  public StandardTopicConvention() {
    this(DEFAULT_DELIMiTER);
  }

  @Nonnull
  private String getTopicName(@Nonnull String... parts) {
    return String.join(_delimiter, parts);
  }

  @Nonnull
  @Override
  public String getMetadataChangeEventTopicName() {
    return getTopicName(METADATA_CHANGE_EVENT, VERSION_4);
  }

  @Nonnull
  @Override
  public String getMetadataAuditEventTopicName() {
    return getTopicName(METADATA_AUDIT_EVENT, VERSION_4);
  }

  @Nonnull
  @Override
  public String getFailedMetadataChangeEventTopicName() {
    return getTopicName(FAILED_METADATA_CHANGE_EVENT, VERSION_4);
  }

  @Nonnull
  @Override
  public String getMetadataAuditEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    // v5 is still in development.
    throw new UnsupportedOperationException("TODO - implement once versions are in annotations.");
  }

  @Override
  public String getMetadataAuditEventType(@Nonnull String topicName) {
    // v5 is still in development.
    throw new UnsupportedOperationException("TODO - implement once versions are in annotations.");
  }
}
