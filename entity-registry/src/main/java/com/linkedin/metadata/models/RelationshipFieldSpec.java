package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.Value;

@Value
public class RelationshipFieldSpec implements FieldSpec {

  @NonNull PathSpec path;
  @NonNull RelationshipAnnotation relationshipAnnotation;
  @NonNull DataSchema pegasusSchema;

  /** Returns the name of the outbound relationship extending from the field. */
  @Nonnull
  public String getRelationshipName() {
    return relationshipAnnotation.getName();
  }

  /** Returns a list of entity names representing the destination node type of the relationship. */
  @Nonnull
  public List<String> getValidDestinationTypes() {
    return relationshipAnnotation.getValidDestinationTypes();
  }

  public boolean isLineageRelationship() {
    return relationshipAnnotation.isLineage();
  }
}
