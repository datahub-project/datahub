package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Validates the Schema Field Path specification, specifically that all field IDs must be unique
 * across all fields within a schema.
 *
 * @see <a href="https://datahubproject.io/docs/advanced/field-path-spec-v2/#requirements">Field
 *     Path V2 docs</a>
 */
@Setter
@Getter
@Accessors(chain = true)
public class FieldPathValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  /**
   * Prevent any MCP for SchemaMetadata where field ids are duplicated (except for MCPs with {@link
   * ChangeType#DELETE}).
   */
  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return mcpItems.stream()
        .filter(i -> !ChangeType.DELETE.equals(i.getChangeType()))
        .filter(
            i ->
                i.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)
                    || i.getAspectName().equals(EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
        .map(
            i -> {
              if (i.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)) {
                return processSchemaMetadataAspect(i);
              } else {
                return processEditableSchemaMetadataAspect(i);
              }
            })
        .filter(Objects::nonNull);
  }

  private static AspectValidationException processEditableSchemaMetadataAspect(BatchItem i) {
    final EditableSchemaMetadata schemaMetadata = i.getAspect(EditableSchemaMetadata.class);
    final long uniquePaths =
        schemaMetadata.getEditableSchemaFieldInfo().stream()
            .map(EditableSchemaFieldInfo::getFieldPath)
            .distinct()
            .count();
    if (uniquePaths != schemaMetadata.getEditableSchemaFieldInfo().size()) {
      return AspectValidationException.forItem(
          i,
          String.format(
              "Cannot perform %s action on proposal. EditableSchemaMetadata aspect has duplicated field paths",
              i.getChangeType()));
    }
    return null;
  }

  private static AspectValidationException processSchemaMetadataAspect(BatchItem i) {
    final SchemaMetadata schemaMetadata = i.getAspect(SchemaMetadata.class);
    final long uniquePaths =
        schemaMetadata.getFields().stream().map(SchemaField::getFieldPath).distinct().count();
    if (uniquePaths != schemaMetadata.getFields().size()) {
      return AspectValidationException.forItem(
          i,
          String.format(
              "Cannot perform %s action on proposal. SchemaMetadata aspect has duplicated field paths",
              i.getChangeType()));
    }
    return null;
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
