package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.TimeseriesFieldCollectionAnnotation;
import java.util.Map;
import lombok.Data;
import lombok.NonNull;

@Data
public class TimeseriesFieldCollectionSpec implements FieldSpec {
  @NonNull PathSpec path;
  @NonNull TimeseriesFieldCollectionAnnotation timeseriesFieldCollectionAnnotation;
  @NonNull Map<String, TimeseriesFieldSpec> timeseriesFieldSpecMap;
  @NonNull DataSchema pegasusSchema;

  private PathSpec keyPath;

  public String getName() {
    return timeseriesFieldCollectionAnnotation.getCollectionName();
  }

  public String getKeyPathFromAnnotation() {
    return path + "/" + timeseriesFieldCollectionAnnotation.getKey();
  }
}
