package com.linkedin.metadata.kafka.mcl;

import static com.linkedin.metadata.Constants.MDC_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MDC_CHANGE_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_TYPE;
import static com.linkedin.metadata.Constants.MDC_ENTITY_URN;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.generic.AbstractKafkaListener;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.MDC;

@Slf4j
public class MCLKafkaListener
    extends AbstractKafkaListener<MetadataChangeLog, MetadataChangeLogHook> {

  private static final String WILDCARD = "*";

  @Override
  @Nonnull
  public MetadataChangeLog convertRecord(@Nonnull GenericRecord record) throws IOException {
    return EventUtils.avroToPegasusMCL(record);
  }

  @Override
  protected void setMDCContext(MetadataChangeLog event) {
    Urn entityUrn = event.getEntityUrn();
    String aspectName = event.hasAspectName() ? event.getAspectName() : null;
    String entityType = event.hasEntityType() ? event.getEntityType() : null;
    ChangeType changeType = event.hasChangeType() ? event.getChangeType() : null;

    MDC.put(MDC_ENTITY_URN, Optional.ofNullable(entityUrn).map(Urn::toString).orElse(""));
    MDC.put(MDC_ASPECT_NAME, aspectName);
    MDC.put(MDC_ENTITY_TYPE, entityType);
    MDC.put(MDC_CHANGE_TYPE, Optional.ofNullable(changeType).map(ChangeType::toString).orElse(""));
  }

  @Override
  protected boolean shouldSkipProcessing(MetadataChangeLog event) {
    String entityType = event.hasEntityType() ? event.getEntityType() : null;
    String aspectName = event.hasAspectName() ? event.getAspectName() : null;

    return aspectsToDrop.getOrDefault(entityType, Collections.emptySet()).contains(aspectName)
        || aspectsToDrop.getOrDefault(WILDCARD, Collections.emptySet()).contains(aspectName);
  }

  @Override
  protected List<String> getFineGrainedLoggingAttributes(MetadataChangeLog event) {
    List<String> attributes = new ArrayList<>();

    if (!fineGrainedLoggingEnabled) {
      return attributes;
    }

    String aspectName = event.hasAspectName() ? event.getAspectName() : null;
    String entityType = event.hasEntityType() ? event.getEntityType() : null;
    ChangeType changeType = event.hasChangeType() ? event.getChangeType() : null;

    if (aspectName != null) {
      attributes.add(MetricUtils.ASPECT_NAME);
      attributes.add(aspectName);
    }

    if (entityType != null) {
      attributes.add(MetricUtils.ENTITY_TYPE);
      attributes.add(entityType);
    }

    if (changeType != null) {
      attributes.add(MetricUtils.CHANGE_TYPE);
      attributes.add(changeType.name());
    }

    return attributes;
  }

  @Override
  protected SystemMetadata getSystemMetadata(MetadataChangeLog event) {
    return event.getSystemMetadata();
  }

  @Override
  protected String getEventDisplayString(MetadataChangeLog event) {
    return String.format(
        "urn: %s, aspect name: %s, entity type: %s, change type: %s",
        event.getEntityUrn(),
        event.hasAspectName() ? event.getAspectName() : null,
        event.hasEntityType() ? event.getEntityType() : null,
        event.hasChangeType() ? event.getChangeType() : null);
  }
}
