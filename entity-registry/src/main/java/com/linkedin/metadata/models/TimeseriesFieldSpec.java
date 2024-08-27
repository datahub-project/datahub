package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.TimeseriesFieldAnnotation;
import lombok.NonNull;
import lombok.Value;

@Value
public class TimeseriesFieldSpec implements FieldSpec {
  @NonNull PathSpec path;
  @NonNull TimeseriesFieldAnnotation timeseriesFieldAnnotation;
  @NonNull DataSchema pegasusSchema;

  public String getName() {
    return timeseriesFieldAnnotation.getStatName();
  }
}
