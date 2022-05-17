package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import lombok.NonNull;
import lombok.Value;


@Value
public class SearchScoreFieldSpec implements FieldSpec {
  @NonNull PathSpec path;
  @NonNull SearchScoreAnnotation searchScoreAnnotation;
  @NonNull DataSchema pegasusSchema;
}