package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import lombok.NonNull;
import lombok.Value;

@Value
public class SearchableFieldSpec implements FieldSpec {

  @NonNull PathSpec path;
  @NonNull SearchableAnnotation searchableAnnotation;
  @NonNull DataSchema pegasusSchema;

  public boolean isArray() {
    return path.getPathComponents().contains("*");
  }
}
