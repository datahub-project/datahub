package com.linkedin.metadata.utils;

import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ModelConversionException;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import lombok.extern.slf4j.Slf4j;


/**
 * Static utility class providing methods for extracting entity metadata from Pegasus models.
 */
@Slf4j
public class PegasusUtils {

  private PegasusUtils() {
  }

  public static String getEntityNameFromSchema(final RecordDataSchema entitySnapshotSchema) {
    final Object entityAnnotationObj = entitySnapshotSchema.getProperties().get(EntityAnnotation.ANNOTATION_NAME);
    if (entityAnnotationObj != null) {
      return EntityAnnotation.fromSchemaProperty(entityAnnotationObj, entitySnapshotSchema.getFullName()).getName();
    }
    log.error(String.format("Failed to extract entity name from provided schema %s", entitySnapshotSchema.getName()));
    throw new IllegalArgumentException(
        String.format("Failed to extract entity name from provided schema %s", entitySnapshotSchema.getName()));
  }

  // TODO: Figure out a better iteration strategy.
  public static String getAspectNameFromFullyQualifiedName(final String fullyQualifiedRecordTemplateName) {
    final RecordTemplate template = RecordUtils.toRecordTemplate(fullyQualifiedRecordTemplateName, new DataMap());
    final RecordDataSchema aspectSchema = template.schema();
    return getAspectNameFromSchema(aspectSchema);
  }

  public static String getAspectNameFromSchema(final RecordDataSchema aspectSchema) {
    final Object aspectAnnotationObj = aspectSchema.getProperties().get(AspectAnnotation.ANNOTATION_NAME);
    if (aspectAnnotationObj != null) {
      return AspectAnnotation.fromSchemaProperty(aspectAnnotationObj, aspectSchema.getFullName()).getName();
    }
    log.error(String.format("Failed to extract aspect name from provided schema %s", aspectSchema.getName()));
    throw new IllegalArgumentException(
        String.format("Failed to extract aspect name from provided schema %s", aspectSchema.getName()));
  }

  public static <T> Class<? extends T> getDataTemplateClassFromSchema(final NamedDataSchema schema, final Class<T> clazz) {
    try {
        return Class.forName(schema.getFullName()).asSubclass(clazz);
    } catch (ClassNotFoundException e) {
      log.error("Unable to find class for RecordDataSchema named " + schema.getFullName() + " " + e.getMessage());
      throw new ModelConversionException("Unable to find class for RecordDataSchema named " + schema.getFullName(), e);
    }
  }

  public static String urnToEntityName(final Urn urn) {
    return urn.getEntityType();
  }
}
