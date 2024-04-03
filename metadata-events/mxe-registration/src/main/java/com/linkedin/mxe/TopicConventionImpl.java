package com.linkedin.mxe;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;
import org.apache.avro.specific.SpecificRecord;

/**
 * Default implementation of a {@link TopicConvention}, which is fully customizable for event names.
 *
 * <p>The newer aspect-entity specific event names are based on a pattern that can also be
 * configured. The pattern is a string, which can use {@link #EVENT_TYPE_PLACEHOLDER}, {@link
 * #VERSION_PLACEHOLDER}, {@link #ENTITY_PLACEHOLDER}, and {@link #ASPECT_PLACEHOLDER} as
 * placeholders for the event type (MCE, MAE, FMCE, etc), event version, entity name, and aspect
 * name, respectively.
 */
public final class TopicConventionImpl implements TopicConvention {
  // Placeholders
  public static final String EVENT_TYPE_PLACEHOLDER = "%EVENT%";
  public static final String VERSION_PLACEHOLDER = "%VERSION%";
  public static final String ENTITY_PLACEHOLDER = "%ENTITY%";
  public static final String ASPECT_PLACEHOLDER = "%ASPECT%";

  // v5 defaults
  public static final String DEFAULT_EVENT_PATTERN = "%EVENT%_%ENTITY%_%ASPECT%_v%VERSION%";

  // V5 event name placeholder replacements
  private static final String METADATA_CHANGE_EVENT_TYPE = "MCE";
  private static final String METADATA_AUDIT_EVENT_TYPE = "MAE";
  private static final String FAILED_METADATA_CHANGE_EVENT_TYPE = "FMCE";

  // v4 names
  private final String _metadataChangeEventTopicName;
  private final String _metadataAuditEventTopicName;
  private final String _failedMetadataChangeEventTopicName;

  // Generic event topic names
  private final String _metadataChangeProposalTopicName;
  private final String _metadataChangeLogVersionedTopicName;
  private final String _metadataChangeLogTimeseriesTopicName;
  private final String _failedMetadataChangeProposalTopicName;
  private final String _platformEventTopicName;
  private final String _dataHubUpgradeHistoryTopicName;

  // v5 patterns
  private final String _eventPattern;

  public TopicConventionImpl(
      @Nonnull String metadataChangeEventTopicName,
      @Nonnull String metadataAuditEventTopicName,
      @Nonnull String failedMetadataChangeEventTopicName,
      @Nonnull String metadataChangeProposalTopicName,
      @Nonnull String metadataChangeLogVersionedTopicName,
      @Nonnull String metadataChangeLogTimeseriesTopicName,
      @Nonnull String failedMetadataChangeProposalTopicName,
      @Nonnull String platformEventTopicName,
      @Nonnull String eventPattern,
      @Nonnull String dataHubUpgradeHistoryTopicName) {
    _metadataChangeEventTopicName = metadataChangeEventTopicName;
    _metadataAuditEventTopicName = metadataAuditEventTopicName;
    _failedMetadataChangeEventTopicName = failedMetadataChangeEventTopicName;
    _metadataChangeProposalTopicName = metadataChangeProposalTopicName;
    _metadataChangeLogVersionedTopicName = metadataChangeLogVersionedTopicName;
    _metadataChangeLogTimeseriesTopicName = metadataChangeLogTimeseriesTopicName;
    _failedMetadataChangeProposalTopicName = failedMetadataChangeProposalTopicName;
    _platformEventTopicName = platformEventTopicName;
    _eventPattern = eventPattern;
    _dataHubUpgradeHistoryTopicName = dataHubUpgradeHistoryTopicName;
  }

  public TopicConventionImpl() {
    this(
        Topics.METADATA_CHANGE_EVENT,
        Topics.METADATA_AUDIT_EVENT,
        Topics.FAILED_METADATA_CHANGE_EVENT,
        Topics.METADATA_CHANGE_PROPOSAL,
        Topics.METADATA_CHANGE_LOG_VERSIONED,
        Topics.METADATA_CHANGE_LOG_TIMESERIES,
        Topics.FAILED_METADATA_CHANGE_PROPOSAL,
        Topics.PLATFORM_EVENT,
        DEFAULT_EVENT_PATTERN,
        Topics.DATAHUB_UPGRADE_HISTORY_TOPIC_NAME);
  }

  @Nonnull
  @Override
  public String getMetadataChangeEventTopicName() {
    return _metadataChangeEventTopicName;
  }

  @Nonnull
  @Override
  public String getMetadataAuditEventTopicName() {
    return _metadataAuditEventTopicName;
  }

  @Nonnull
  @Override
  public String getFailedMetadataChangeEventTopicName() {
    return _failedMetadataChangeEventTopicName;
  }

  @Nonnull
  @Override
  public String getMetadataChangeProposalTopicName() {
    return _metadataChangeProposalTopicName;
  }

  @Nonnull
  @Override
  public String getMetadataChangeLogVersionedTopicName() {
    return _metadataChangeLogVersionedTopicName;
  }

  @Nonnull
  @Override
  public String getMetadataChangeLogTimeseriesTopicName() {
    return _metadataChangeLogTimeseriesTopicName;
  }

  @Nonnull
  @Override
  public String getFailedMetadataChangeProposalTopicName() {
    return _failedMetadataChangeProposalTopicName;
  }

  @Nonnull
  @Override
  public String getPlatformEventTopicName() {
    return _platformEventTopicName;
  }

  @Nonnull
  private String buildEventName(
      @Nonnull String eventType,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      int version) {
    return _eventPattern
        .replace(EVENT_TYPE_PLACEHOLDER, eventType)
        .replace(ENTITY_PLACEHOLDER, entityName)
        .replace(ASPECT_PLACEHOLDER, aspectName)
        .replace(VERSION_PLACEHOLDER, Integer.toString(version));
  }

  private String buildEventName(
      @Nonnull String eventType, @Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    final String urnName = urn.getClass().getSimpleName();
    // Expect URN name to relate to the entity name. (EntityName) + "Urn" == (UrnName)
    final String entityType = urnName.substring(0, urnName.length() - "Urn".length());
    final String aspectName = aspect.getClass().getSimpleName();

    // TODO support versions beyond v1
    return buildEventName(eventType, entityType, aspectName, 1);
  }

  @Nonnull
  @Override
  public String getMetadataChangeEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    return buildEventName(METADATA_CHANGE_EVENT_TYPE, urn, aspect);
  }

  @Override
  public String getDataHubUpgradeHistoryTopicName() {
    return _dataHubUpgradeHistoryTopicName;
  }

  @Override
  public Class<? extends SpecificRecord> getMetadataChangeEventType(
      @Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    // v5 is still in development.
    throw new UnsupportedOperationException("TODO - implement once versions are in annotations.");
  }

  @Nonnull
  @Override
  public String getMetadataAuditEventTopicName(@Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    return buildEventName(METADATA_AUDIT_EVENT_TYPE, urn, aspect);
  }

  @Override
  public Class<? extends SpecificRecord> getMetadataAuditEventType(
      @Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    // v5 is still in development.
    throw new UnsupportedOperationException("TODO - implement once versions are in annotations.");
  }

  @Nonnull
  @Override
  public String getFailedMetadataChangeEventTopicName(
      @Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    return buildEventName(FAILED_METADATA_CHANGE_EVENT_TYPE, urn, aspect);
  }

  @Override
  public Class<? extends SpecificRecord> getFailedMetadataChangeEventType(
      @Nonnull Urn urn, @Nonnull RecordTemplate aspect) {
    // v5 is still in development.
    throw new UnsupportedOperationException("TODO - implement once versions are in annotations.");
  }
}
