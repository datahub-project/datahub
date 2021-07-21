package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.TemporalStatCollectionAnnotation;
import java.util.List;
import lombok.Data;
import lombok.NonNull;


@Data
public class TemporalStatCollectionFieldSpec implements FieldSpec {
  @NonNull PathSpec path;
  @NonNull TemporalStatCollectionAnnotation temporalStatCollectionAnnotation;
  @NonNull List<TemporalStatFieldSpec> temporalStats;
  @NonNull DataSchema pegasusSchema;

  private PathSpec keyPath;

  public String getName() {
    return temporalStatCollectionAnnotation.getCollectionName();
  }

  public String getKeyPathFromAnnotation() {
    return path.toString() + "/" + temporalStatCollectionAnnotation.getKey();
  }
}
