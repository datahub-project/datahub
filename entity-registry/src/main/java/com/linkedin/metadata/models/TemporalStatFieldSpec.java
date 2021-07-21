package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.TemporalStatAnnotation;
import lombok.NonNull;
import lombok.Value;


@Value
public class TemporalStatFieldSpec implements FieldSpec {
  @NonNull PathSpec path;
  @NonNull TemporalStatAnnotation temporalStatAnnotation;
  @NonNull DataSchema pegasusSchema;

  public String getName() {
    return temporalStatAnnotation.getStatName();
  }
}
