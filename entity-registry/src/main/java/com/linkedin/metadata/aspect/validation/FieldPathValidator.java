package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * 1. Validates the Schema Field Path specification, specifically that all field IDs must be unique
 * across all fields within a schema. 2. Validates that the field path id is not empty.
 *
 * @see <a href="https://docs.datahub.com/docs/advanced/field-path-spec-v2/#requirements">Field Path
 *     V2 docs</a>
 */
@Setter
@Getter
@Accessors(chain = true)
public class FieldPathValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  /**
   * Validate the merged aspect at pre-commit so the field-id/field-path constraints apply to
   * PATCH-applied writes too. A PATCH item is dropped by the proposed-hook change-type gate, but
   * the patch is merged into an UPSERT {@link ChangeMCP} that reaches this hook, so validating here
   * covers both upsert and patch without adding PATCH to the bean's supportedOperations.
   */
  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return validateFieldPathUpserts(changeMCPs);
  }

  /** Prevent duplicate field ids or empty field paths in the merged schema aspect. */
  public static Stream<AspectValidationException> validateFieldPathUpserts(
      @Nonnull Collection<ChangeMCP> changeMCPs) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (ChangeMCP i : changeMCPs) {
      if (SCHEMA_METADATA_ASPECT_NAME.equals(i.getAspectName())) {
        processSchemaMetadataAspect(i, exceptions);
      } else if (EDITABLE_SCHEMA_METADATA_ASPECT_NAME.equals(i.getAspectName())) {
        processEditableSchemaMetadataAspect(i, exceptions);
      }
    }

    return exceptions.streamAllExceptions();
  }

  private static void processEditableSchemaMetadataAspect(
      BatchItem i, ValidationExceptionCollection exceptions) {
    final EditableSchemaMetadata schemaMetadata = i.getAspect(EditableSchemaMetadata.class);
    final long uniquePaths =
        validateAndCount(
            i,
            schemaMetadata.getEditableSchemaFieldInfo().stream()
                .map(EditableSchemaFieldInfo::getFieldPath),
            exceptions);

    if (uniquePaths != schemaMetadata.getEditableSchemaFieldInfo().size()) {
      exceptions.addException(
          i,
          String.format(
              "Cannot perform %s action on proposal. EditableSchemaMetadata aspect has duplicated field paths",
              i.getChangeType()));
    }
  }

  private static void processSchemaMetadataAspect(
      BatchItem i, ValidationExceptionCollection exceptions) {
    final SchemaMetadata schemaMetadata = i.getAspect(SchemaMetadata.class);
    final long uniquePaths =
        validateAndCount(
            i, schemaMetadata.getFields().stream().map(SchemaField::getFieldPath), exceptions);

    if (uniquePaths != schemaMetadata.getFields().size()) {
      exceptions.addException(
          i,
          String.format(
              "Cannot perform %s action on proposal. SchemaMetadata aspect has duplicated field paths",
              i.getChangeType()));
    }
  }

  private static long validateAndCount(
      BatchItem i, Stream<String> fieldPaths, ValidationExceptionCollection exceptions) {
    return fieldPaths
        .distinct()
        // inspect the stream of fieldPath validation errors since we're already iterating
        .peek(
            fieldPath ->
                validateFieldPath(fieldPath)
                    .ifPresent(message -> exceptions.addException(i, message)))
        .count();
  }

  private static Optional<String> validateFieldPath(String fieldPath) {
    if (fieldPath == null || fieldPath.isEmpty()) {
      return Optional.of("SchemaMetadata aspect has empty field path.");
    }
    return Optional.empty();
  }
}
