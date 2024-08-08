package com.datahub.util.validator;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

public class SnapshotValidator {

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED =
      ConcurrentHashMap.newKeySet();

  private SnapshotValidator() {
    // Util class
  }

  /**
   * Validates a snapshot model defined in com.linkedin.metadata.snapshot.
   *
   * @param schema schema for the model
   */
  public static void validateSnapshotSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, ValidationUtils::isValidUrnField)) {
      ValidationUtils.invalidSchema(
          "Snapshot '%s' must contain an non-optional 'urn' field of URN type", className);
    }

    if (!ValidationUtils.schemaHasExactlyOneSuchField(
        schema, SnapshotValidator::isValidAspectsField)) {
      ValidationUtils.invalidSchema(
          "Snapshot '%s' must contain an non-optional 'aspects' field of ARRAY type", className);
    }

    validateAspectsItemType(schema.getField("aspects"), className);
  }

  /**
   * Similar to {@link #validateSnapshotSchema(RecordDataSchema)} but take a {@link Class} instead
   * and caches results.
   */
  public static void validateSnapshotSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateSnapshotSchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }

  /**
   * Validates that the URN class is unique across all snapshots.
   *
   * @param snapshotClasses a collection of snapshot classes.
   */
  public static void validateUniqueUrn(
      @Nonnull Collection<Class<? extends RecordTemplate>> snapshotClasses) {
    final Set<Class<Urn>> urnClasses = new HashSet<>();
    snapshotClasses.forEach(
        snapshotClass -> {
          final Class<Urn> urnClass =
              ValidationUtils.getUrnClass(
                  ValidationUtils.getRecordSchema(snapshotClass).getField("urn"));
          if (urnClasses.contains(urnClass)) {
            ValidationUtils.invalidSchema(
                "URN class %s in %s has already been claimed by another snapshot.",
                urnClass, snapshotClass);
          }
          urnClasses.add(urnClass);
        });
  }

  private static boolean isValidAspectsField(@Nonnull RecordDataSchema.Field field) {
    return field.getName().equals("aspects")
        && !field.getOptional()
        && field.getType().getType() == DataSchema.Type.ARRAY;
  }

  private static void validateAspectsItemType(
      @Nonnull RecordDataSchema.Field aspectsField, @Nonnull String className) {
    DataSchema itemSchema = ((ArrayDataSchema) aspectsField.getType()).getItems();

    if (itemSchema.getType() != DataSchema.Type.TYPEREF) {
      ValidationUtils.invalidSchema(
          "Snapshot %s' 'aspects' field must be an array of aspect typeref", className);
    }

    TyperefDataSchema typerefSchema = (TyperefDataSchema) itemSchema;
    DataSchema unionSchema = typerefSchema.getDereferencedDataSchema();

    if (unionSchema.getType() != DataSchema.Type.UNION) {
      ValidationUtils.invalidSchema(
          "Snapshot '%s' 'aspects' field must be an array of union typeref", className);
    }

    AspectValidator.validateAspectUnionSchema(
        (UnionDataSchema) unionSchema, typerefSchema.getBindingName());
  }
}
