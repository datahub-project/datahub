package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.*;

import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.PatchOperationUtils;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

  private static final String FIELD_PATH_FIELD = "fieldPath";
  private static final String SCHEMA_FIELDS_PATH = "/fields";
  private static final String EDITABLE_FIELDS_PATH = "/editableSchemaFieldInfo";

  /** Prevent any MCP for SchemaMetadata where field ids are duplicated. */
  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        i -> {
          if (ChangeType.PATCH.equals(i.getChangeType()) && i instanceof MCPItem) {
            processPatchItem((MCPItem) i, exceptions);
          } else if (i.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)) {
            processSchemaMetadata(i, i.getAspect(SchemaMetadata.class), exceptions);
          } else {
            processEditableSchemaMetadata(i, i.getAspect(EditableSchemaMetadata.class), exceptions);
          }
        });

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    // A PATCH carries only its delta at proposal time, and a schemaMetadata patch can append or
    // rewrite a field by array index, so the patched result is checked here. Only violations the
    // patch introduced are reported: stored bad data must not block unrelated patches.
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    changeMCPs.stream()
        .filter(PatchOperationUtils::isAppliedPatch)
        .forEach(
            item -> {
              Set<String> before =
                  violations(item.getAspectName(), item.getPreviousRecordTemplate());
              violations(item.getAspectName(), item.getRecordTemplate()).stream()
                  .filter(violation -> !before.contains(violation))
                  .forEach(violation -> exceptions.addException(item, violation));
            });
    return exceptions.streamAllExceptions();
  }

  /** Empty and duplicated field paths of a stored or patched aspect, as messages. */
  private static Set<String> violations(String aspectName, @Nullable RecordTemplate aspect) {
    if (aspect == null) {
      return Set.of();
    }
    final boolean isSchemaMetadata = SCHEMA_METADATA_ASPECT_NAME.equals(aspectName);
    final List<String> fieldPaths =
        isSchemaMetadata
            ? new SchemaMetadata(aspect.data())
                .getFields().stream().map(SchemaField::getFieldPath).collect(Collectors.toList())
            : new EditableSchemaMetadata(aspect.data())
                .getEditableSchemaFieldInfo().stream()
                    .map(EditableSchemaFieldInfo::getFieldPath)
                    .collect(Collectors.toList());
    final Set<String> seen = new HashSet<>();
    final Set<String> violations = new HashSet<>();
    for (String fieldPath : fieldPaths) {
      validateFieldPath(fieldPath).ifPresent(violations::add);
      if (!seen.add(fieldPath)) {
        violations.add(
            String.format(
                "%s aspect has duplicated field path %s",
                isSchemaMetadata ? "SchemaMetadata" : "EditableSchemaMetadata", fieldPath));
      }
    }
    return violations;
  }

  /**
   * A patch item carries only its delta, so validate the field paths the patch itself writes: a
   * root or whole-array value is checked like an upsert (duplicates and empty paths), a single
   * keyed-array element only for an empty path — keyed merging already collapses duplicate keys —
   * and deeper sub-field operations (e.g. a tag added under an existing field) do not modify the
   * field path at all. Unparseable values are left to schema validation at merge time.
   */
  private static void processPatchItem(MCPItem item, ValidationExceptionCollection exceptions) {
    final boolean isSchemaMetadata = SCHEMA_METADATA_ASPECT_NAME.equals(item.getAspectName());
    final String arrayPath = isSchemaMetadata ? SCHEMA_FIELDS_PATH : EDITABLE_FIELDS_PATH;

    PatchOperationUtils.addAndReplaceValues(item)
        .forEach(
            op -> {
              final String path = op.getFirst();
              final JsonValue value = op.getSecond();
              try {
                if (path.isEmpty() || "/".equals(path)) {
                  processParsedAspect(item, isSchemaMetadata, value.toString(), exceptions);
                } else if (path.equals(arrayPath)) {
                  processParsedAspect(
                      item,
                      isSchemaMetadata,
                      "{\"" + arrayPath.substring(1) + "\":" + value + "}",
                      exceptions);
                } else if (isArrayElementPath(path, arrayPath)
                    && value.getValueType() == JsonValue.ValueType.OBJECT) {
                  JsonObject element = value.asJsonObject();
                  if (element.containsKey(FIELD_PATH_FIELD)) {
                    validateFieldPath(element.getString(FIELD_PATH_FIELD, null))
                        .ifPresent(message -> exceptions.addException(item, message));
                  }
                }
              } catch (RuntimeException e) {
                // unparseable delta — schema validation rejects it at merge time
              }
            });
  }

  private static void processParsedAspect(
      MCPItem item,
      boolean isSchemaMetadata,
      String aspectJson,
      ValidationExceptionCollection exceptions) {
    if (isSchemaMetadata) {
      processSchemaMetadata(
          item, RecordUtils.toRecordTemplate(SchemaMetadata.class, aspectJson), exceptions);
    } else {
      processEditableSchemaMetadata(
          item, RecordUtils.toRecordTemplate(EditableSchemaMetadata.class, aspectJson), exceptions);
    }
  }

  /** True for {@code <arrayPath>/<key>} exactly — deeper sub-field paths are not elements. */
  private static boolean isArrayElementPath(String path, String arrayPath) {
    if (!path.startsWith(arrayPath + "/")) {
      return false;
    }
    String remainder = path.substring(arrayPath.length() + 1);
    if (remainder.endsWith("/")) {
      remainder = remainder.substring(0, remainder.length() - 1);
    }
    return !remainder.isEmpty() && !remainder.contains("/");
  }

  private static void processEditableSchemaMetadata(
      BatchItem i,
      EditableSchemaMetadata schemaMetadata,
      ValidationExceptionCollection exceptions) {
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

  private static void processSchemaMetadata(
      BatchItem i, SchemaMetadata schemaMetadata, ValidationExceptionCollection exceptions) {
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
