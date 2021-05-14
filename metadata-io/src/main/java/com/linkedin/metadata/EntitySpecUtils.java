package com.linkedin.metadata;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;


public class EntitySpecUtils {

  private EntitySpecUtils() {
  }

  public static String getEntityNameFromSchema(final RecordDataSchema entitySnapshotSchema) {
    final Object entityAnnotationObj = entitySnapshotSchema.getProperties().get("Entity");
    if (entityAnnotationObj != null) {
      return EntityAnnotation.fromSchemaProperty(entityAnnotationObj).getName();
    }
    throw new IllegalArgumentException(
        String.format("Failed to extract entity name from provided schema %s", entitySnapshotSchema.getName()));
  }

  public static String getAspectNameFromSchema(final RecordDataSchema aspectSchema) {
    final Object aspectAnnotationObj = aspectSchema.getProperties().get("Aspect");
    if (aspectAnnotationObj != null) {
      return EntityAnnotation.fromSchemaProperty(aspectAnnotationObj).getName();
    }
    throw new IllegalArgumentException(
        String.format("Failed to extract aspect name from provided schema %s", aspectSchema.getName()));
  }

  public static String urnToEntityName(final Urn urn) {
    return urn.getEntityType();
  }
}
