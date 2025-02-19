package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.UrnValidationAnnotation;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class UrnValidationFieldSpec {
  @Nonnull PathSpec path;
  @Nonnull UrnValidationAnnotation urnValidationAnnotation;
  @Nonnull DataSchema pegasusSchema;
}
