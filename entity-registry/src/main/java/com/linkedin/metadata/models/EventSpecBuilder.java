package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.EventAnnotation;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventSpecBuilder {

  public EventSpecBuilder() {
  }

  public EventSpec buildEventSpec(
      @Nonnull final String eventName,
      @Nonnull final DataSchema eventDataSchema) {

    final RecordDataSchema eventRecordSchema = validateEvent(eventDataSchema);
    final Object eventAnnotationObj = eventDataSchema.getProperties().get(EventAnnotation.ANNOTATION_NAME);

    if (eventAnnotationObj != null) {

      final EventAnnotation eventAnnotation =
          EventAnnotation.fromPegasusAnnotationObject(eventAnnotationObj, eventRecordSchema.getFullName());

      return new DefaultEventSpec(
          eventName,
          eventAnnotation,
          eventRecordSchema);
    }
    return null;
  }

  private RecordDataSchema validateEvent(@Nonnull final DataSchema eventSchema) {
    if (eventSchema.getType() != DataSchema.Type.RECORD) {
      failValidation(String.format("Failed to validate event schema of type %s. Schema must be of record type.",
          eventSchema.getType().toString()));
    }
    return (RecordDataSchema) eventSchema;
  }

  private void failValidation(@Nonnull final String message) {
    throw new ModelValidationException(message);
  }
}