package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;

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
 * @see <a href="https://datahubproject.io/docs/advanced/field-path-spec-v2/#requirements">Field
 *     Path V2 docs</a>
 */
@Setter
@Getter
@Accessors(chain = true)
public class FieldPathValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  /** Prevent any MCP for SchemaMetadata where field ids are duplicated. */
  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        i -> {
          if (i.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)) {
            processSchemaMetadataAspect(i, exceptions);
          } else {
            processEditableSchemaMetadataAspect(i, exceptions);
          }
        });

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
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
