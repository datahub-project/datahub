package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.EventAnnotation;
import javax.annotation.Nonnull;


public class DefaultEventSpec implements EventSpec {

  private final String _name;
  private final EventAnnotation _eventAnnotation;
  private final RecordDataSchema _pegasusSchema;

  public DefaultEventSpec(
      @Nonnull final String name,
      @Nonnull final EventAnnotation eventAnnotation,
      @Nonnull final RecordDataSchema pegasusSchema) {
    _name = name;
    _eventAnnotation = eventAnnotation;
    _pegasusSchema = pegasusSchema;
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public EventAnnotation getEventAnnotation() {
    return _eventAnnotation;
  }

  @Override
  public RecordDataSchema getPegasusSchema() {
    return _pegasusSchema;
  }
}
