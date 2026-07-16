package com.linkedin.metadata.models.annotation;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

@Value
public class TimeseriesFieldAnnotation {

  public static final String ANNOTATION_NAME = "TimeseriesField";

  String statName;
  AggregationType aggregationType;
  // Type of the field. Defines how the field is indexed and matched
  FieldType fieldType;

  public enum FieldType {
    DATETIME,
    KEYWORD,
    DOUBLE,
    INT,
    FLOAT,
    LONG
  }

  @Nonnull
  public static TimeseriesFieldAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName,
      @Nonnull final DataSchema.Type schemaDataType,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> statName = AnnotationUtils.getField(map, "name", String.class);
    final Optional<String> aggregationType =
        AnnotationUtils.getField(map, "aggregationType", String.class);
    final Optional<String> fieldType = AnnotationUtils.getField(map, "fieldType", String.class);
    if (fieldType.isPresent() && !EnumUtils.isValidEnum(FieldType.class, fieldType.get())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid field 'fieldType'. Invalid fieldType provided. Valid types are %s",
              ANNOTATION_NAME, context, Arrays.toString(FieldType.values())));
    }
    final FieldType resolvedFieldType = getFieldType(fieldType, schemaDataType);

    return new TimeseriesFieldAnnotation(
        statName.orElse(schemaFieldName),
        aggregationType.map(AggregationType::valueOf).orElse(AggregationType.LATEST),
        resolvedFieldType);
  }

  public enum AggregationType {
    LATEST,
    SUM
  }

  private static FieldType getFieldType(
      Optional<String> maybeFieldType, DataSchema.Type schemaDataType) {
    if (!maybeFieldType.isPresent()) {
      return getDefaultFieldType(schemaDataType);
    }
    return FieldType.valueOf(maybeFieldType.get());
  }

  private static FieldType getDefaultFieldType(DataSchema.Type schemaDataType) {
    switch (schemaDataType) {
      case INT:
        return FieldType.INT;
      case LONG:
        return FieldType.LONG;
      case FLOAT:
        return FieldType.FLOAT;
      case DOUBLE:
        return FieldType.DOUBLE;
      default:
        return FieldType.KEYWORD;
    }
  }
}
