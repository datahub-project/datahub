package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchableRefAnnotation;
import lombok.NonNull;
import lombok.Value;

@Value
public class SearchableRefFieldSpec implements FieldSpec {

  @NonNull PathSpec path;
  @NonNull SearchableRefAnnotation searchableRefAnnotation;
  @NonNull DataSchema pegasusSchema;

  public boolean isArray() {
    return path.getPathComponents().contains("*");
  }
}
