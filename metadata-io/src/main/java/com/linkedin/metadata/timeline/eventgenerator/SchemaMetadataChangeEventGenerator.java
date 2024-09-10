package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.timeline.eventgenerator.ChangeEventGeneratorUtils.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.dataset.DatasetSchemaFieldChangeEvent;
import com.linkedin.metadata.timeline.data.dataset.SchemaFieldModificationCategory;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import jakarta.json.JsonPatch;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class SchemaMetadataChangeEventGenerator extends EntityChangeEventGenerator<SchemaMetadata> {
  private static final String SCHEMA_METADATA_ASPECT_NAME = "schemaMetadata";
  private static final String BACKWARDS_INCOMPATIBLE_DESC =
      "A backwards incompatible change due to";
  private static final String BACK_AND_FORWARD_COMPATIBLE_DESC =
      "A forwards & backwards compatible change due to ";
  private static final String FIELD_DESCRIPTION_ADDED_FORMAT =
      "The description '%s' for the field '%s' has been added.";
  private static final String FIELD_DESCRIPTION_REMOVED_FORMAT =
      "The description '%s' for the field '%s' has been removed.";
  private static final String FIELD_DESCRIPTION_MODIFIED_FORMAT =
      "The description for the field '%s' has been changed from '%s' to '%s'.";

  private static ChangeEvent getDescriptionChange(
      @Nullable SchemaField baseField,
      @Nullable SchemaField targetField,
      String datasetFieldUrn,
      AuditStamp auditStamp) {
    String baseDescription = (baseField != null) ? baseField.getDescription() : null;
    String targetDescription = (targetField != null) ? targetField.getDescription() : null;
    if (baseDescription == null && targetDescription != null) {
      // Description got added.
      return ChangeEvent.builder()
          .operation(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .category(ChangeCategory.DOCUMENTATION)
          .entityUrn(datasetFieldUrn)
          .description(
              String.format(
                  FIELD_DESCRIPTION_ADDED_FORMAT, targetDescription, targetField.getFieldPath()))
          .parameters(ImmutableMap.of("description", targetDescription))
          .auditStamp(auditStamp)
          .build();
    }
    if (baseDescription != null && targetDescription == null) {
      // Description removed.
      return ChangeEvent.builder()
          .operation(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .category(ChangeCategory.DOCUMENTATION)
          .entityUrn(datasetFieldUrn)
          .description(
              String.format(
                  FIELD_DESCRIPTION_REMOVED_FORMAT, baseDescription, baseField.getFieldPath()))
          .parameters(ImmutableMap.of("description", baseDescription))
          .auditStamp(auditStamp)
          .build();
    }
    if (baseDescription != null && !baseDescription.equals(targetDescription)) {
      // Description Change
      return ChangeEvent.builder()
          .operation(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.PATCH)
          .category(ChangeCategory.DOCUMENTATION)
          .entityUrn(datasetFieldUrn)
          .description(
              String.format(
                  FIELD_DESCRIPTION_MODIFIED_FORMAT,
                  baseField.getFieldPath(),
                  baseDescription,
                  targetDescription))
          .parameters(ImmutableMap.of("description", targetDescription))
          .auditStamp(auditStamp)
          .build();
    }
    return null;
  }

  private static List<ChangeEvent> getGlobalTagChangeEvents(
      SchemaField baseField,
      SchemaField targetField,
      String parentUrnStr,
      String datasetFieldUrn,
      AuditStamp auditStamp) {

    // 1. Get EntityTagChangeEvent, then rebind into a SchemaFieldTagChangeEvent.
    List<ChangeEvent> entityTagChangeEvents =
        GlobalTagsChangeEventGenerator.computeDiffs(
            baseField != null ? baseField.getGlobalTags() : null,
            targetField != null ? targetField.getGlobalTags() : null,
            datasetFieldUrn,
            auditStamp);

    if (baseField != null || targetField != null) {
      String fieldPath =
          targetField != null ? targetField.getFieldPath() : baseField.getFieldPath();
      // 2. Convert EntityTagChangeEvent into a SchemaFieldTagChangeEvent.
      final Urn parentUrn;
      try {
        parentUrn = UrnUtils.getUrn(parentUrnStr);
      } catch (Exception e) {
        log.error("Failed to parse parentUrnStr: {}", parentUrnStr, e);
        return Collections.emptyList();
      }

      return convertEntityTagChangeEvents(fieldPath, parentUrn, entityTagChangeEvents);
    }

    return Collections.emptyList();
  }

  private static List<ChangeEvent> getGlossaryTermsChangeEvents(
      SchemaField baseField,
      SchemaField targetField,
      String parentUrnStr,
      String datasetFieldUrn,
      AuditStamp auditStamp) {

    // 1. Get EntityGlossaryTermChangeEvent, then rebind into a SchemaFieldGlossaryTermChangeEvent.
    List<ChangeEvent> entityGlossaryTermsChangeEvents =
        GlossaryTermsChangeEventGenerator.computeDiffs(
            baseField != null ? baseField.getGlossaryTerms() : null,
            targetField != null ? targetField.getGlossaryTerms() : null,
            datasetFieldUrn,
            auditStamp);

    if (targetField != null || baseField != null) {
      String fieldPath =
          targetField != null ? targetField.getFieldPath() : baseField.getFieldPath();
      // 2. Convert EntityGlossaryTermChangeEvent into a SchemaFieldGlossaryTermChangeEvent.
      final Urn parentUrn;
      try {
        parentUrn = UrnUtils.getUrn(parentUrnStr);
      } catch (Exception e) {
        log.error("Failed to parse parentUrnStr: {}", parentUrnStr, e);
        return Collections.emptyList();
      }

      return convertEntityGlossaryTermChangeEvents(
          fieldPath, parentUrn, entityGlossaryTermsChangeEvents);
    }

    return Collections.emptyList();
  }

  private static List<ChangeEvent> getFieldPropertyChangeEvents(
      SchemaField baseField,
      SchemaField targetField,
      Urn datasetUrn,
      ChangeCategory changeCategory,
      AuditStamp auditStamp) {
    List<ChangeEvent> propChangeEvents = new ArrayList<>();
    String datasetFieldUrn;
    if (targetField != null) {
      datasetFieldUrn = getSchemaFieldUrn(datasetUrn, targetField).toString();
    } else {
      datasetFieldUrn = getSchemaFieldUrn(datasetUrn, baseField).toString();
    }

    // Description Change.
    if (ChangeCategory.DOCUMENTATION.equals(changeCategory)) {
      ChangeEvent descriptionChangeEvent =
          getDescriptionChange(baseField, targetField, datasetFieldUrn, auditStamp);
      if (descriptionChangeEvent != null) {
        propChangeEvents.add(descriptionChangeEvent);
      }
    }

    // Global Tags
    if (ChangeCategory.TAG.equals(changeCategory)) {
      propChangeEvents.addAll(
          getGlobalTagChangeEvents(
              baseField, targetField, datasetUrn.toString(), datasetFieldUrn, auditStamp));
    }

    // Glossary terms.
    if (ChangeCategory.GLOSSARY_TERM.equals(changeCategory)) {
      propChangeEvents.addAll(
          getGlossaryTermsChangeEvents(
              baseField, targetField, datasetUrn.toString(), datasetFieldUrn, auditStamp));
    }

    return propChangeEvents;
  }

  private static Map<String, SchemaField> getSchemaFieldMap(SchemaFieldArray fieldArray) {
    Map<String, SchemaField> fieldMap = new HashMap<>();
    fieldArray.forEach(schemaField -> fieldMap.put(schemaField.getFieldPath(), schemaField));
    return fieldMap;
  }

  private static void processFieldPathDataTypeChange(
      String baseFieldPath,
      Urn datasetUrn,
      ChangeCategory changeCategory,
      AuditStamp auditStamp,
      Map<String, SchemaField> baseFieldMap,
      Map<String, SchemaField> targetFieldMap,
      Set<String> processedBaseFields,
      Set<String> processedTargetFields,
      List<ChangeEvent> changeEvents) {
    SchemaField curBaseField = baseFieldMap.get(baseFieldPath);
    if (!targetFieldMap.containsKey(baseFieldPath)) {
      return;
    }
    processedBaseFields.add(baseFieldPath);
    processedTargetFields.add(baseFieldPath);
    SchemaField curTargetField = targetFieldMap.get(baseFieldPath);
    if (!curBaseField.getNativeDataType().equals(curTargetField.getNativeDataType())) {
      // Non-backward compatible change + Major version bump
      if (ChangeCategory.TECHNICAL_SCHEMA.equals(changeCategory)) {
        changeEvents.add(
            DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
                .category(ChangeCategory.TECHNICAL_SCHEMA)
                .modifier(getSchemaFieldUrn(datasetUrn, curBaseField).toString())
                .entityUrn(datasetUrn.toString())
                .operation(ChangeOperation.MODIFY)
                .semVerChange(SemanticChangeType.MAJOR)
                .description(
                    String.format(
                        "%s native datatype of the field '%s' changed from '%s' to '%s'.",
                        BACKWARDS_INCOMPATIBLE_DESC,
                        getFieldPathV1(curTargetField),
                        curBaseField.getNativeDataType(),
                        curTargetField.getNativeDataType()))
                .fieldPath(curBaseField.getFieldPath())
                .fieldUrn(getSchemaFieldUrn(datasetUrn, curBaseField))
                .nullable(curBaseField.isNullable())
                .modificationCategory(SchemaFieldModificationCategory.TYPE_CHANGE)
                .auditStamp(auditStamp)
                .build());
      }
      List<ChangeEvent> propChangeEvents =
          getFieldPropertyChangeEvents(
              curBaseField, curTargetField, datasetUrn, changeCategory, auditStamp);
      changeEvents.addAll(propChangeEvents);
    }
    List<ChangeEvent> propChangeEvents =
        getFieldPropertyChangeEvents(
            curBaseField, curTargetField, datasetUrn, changeCategory, auditStamp);
    changeEvents.addAll(propChangeEvents);
  }

  private static void processFieldPathRename(
      String baseFieldPath,
      Urn datasetUrn,
      ChangeCategory changeCategory,
      AuditStamp auditStamp,
      Map<String, SchemaField> baseFieldMap,
      Map<String, SchemaField> targetFieldMap,
      Set<String> processedBaseFields,
      Set<String> processedTargetFields,
      List<ChangeEvent> changeEvents,
      Set<SchemaField> renamedFields) {

    List<SchemaField> nonProcessedTargetSchemaFields = new ArrayList<>();
    targetFieldMap.forEach(
        (s, schemaField) -> {
          if (!processedTargetFields.contains(s)) {
            nonProcessedTargetSchemaFields.add(schemaField);
          }
        });

    SchemaField curBaseField = baseFieldMap.get(baseFieldPath);
    SchemaField renamedField =
        findRenamedField(curBaseField, nonProcessedTargetSchemaFields, renamedFields);
    processedBaseFields.add(baseFieldPath);
    if (renamedField == null) {
      processRemoval(changeCategory, changeEvents, datasetUrn, curBaseField, auditStamp);
    } else {
      if (!ChangeCategory.TECHNICAL_SCHEMA.equals(changeCategory)) {
        return;
      }
      processedTargetFields.add(renamedField.getFieldPath());
      changeEvents.add(generateRenameEvent(datasetUrn, curBaseField, renamedField, auditStamp));
      List<ChangeEvent> propChangeEvents =
          getFieldPropertyChangeEvents(
              curBaseField, renamedField, datasetUrn, changeCategory, auditStamp);
      changeEvents.addAll(propChangeEvents);
    }
  }

  private static Set<String> getNonProcessedFields(
      Map<String, SchemaField> fieldMap, Set<String> processedFields) {
    Set<String> nonProcessedFields = new HashSet<>(fieldMap.keySet());
    nonProcessedFields.removeAll(processedFields);
    return nonProcessedFields;
  }

  private static List<ChangeEvent> computeDiffs(
      SchemaMetadata baseSchema,
      SchemaMetadata targetSchema,
      Urn datasetUrn,
      ChangeCategory changeCategory,
      AuditStamp auditStamp) {
    SchemaFieldArray baseFields =
        (baseSchema != null ? baseSchema.getFields() : new SchemaFieldArray());
    SchemaFieldArray targetFields =
        targetSchema != null ? targetSchema.getFields() : new SchemaFieldArray();

    Map<String, SchemaField> baseFieldMap = getSchemaFieldMap(baseFields);
    Map<String, SchemaField> targetFieldMap = getSchemaFieldMap(targetFields);

    Set<String> processedBaseFields = new HashSet<>();
    Set<String> processedTargetFields = new HashSet<>();

    List<ChangeEvent> changeEvents = new ArrayList<>();
    Set<SchemaField> renamedFields = new HashSet<>();

    for (String baseFieldPath : baseFieldMap.keySet()) {
      processFieldPathDataTypeChange(
          baseFieldPath,
          datasetUrn,
          changeCategory,
          auditStamp,
          baseFieldMap,
          targetFieldMap,
          processedBaseFields,
          processedTargetFields,
          changeEvents);
    }
    Set<String> nonProcessedBaseFields = getNonProcessedFields(baseFieldMap, processedBaseFields);
    for (String baseFieldPath : nonProcessedBaseFields) {
      processFieldPathRename(
          baseFieldPath,
          datasetUrn,
          changeCategory,
          auditStamp,
          baseFieldMap,
          targetFieldMap,
          processedBaseFields,
          processedTargetFields,
          changeEvents,
          renamedFields);
    }

    Set<String> nonProcessedTargetFields =
        getNonProcessedFields(targetFieldMap, processedTargetFields);

    nonProcessedTargetFields.forEach(
        fieldPath -> {
          SchemaField curTargetField = targetFieldMap.get(fieldPath);
          processAdd(changeCategory, changeEvents, datasetUrn, curTargetField, auditStamp);
        });

    return changeEvents;
  }

  private static SchemaField findRenamedField(
      SchemaField curField, List<SchemaField> targetFields, Set<SchemaField> renamedFields) {
    return targetFields.stream()
        .filter(schemaField -> isRenamed(curField, schemaField))
        .filter(field -> !renamedFields.contains(field))
        .findFirst()
        .orElse(null);
  }

  private static boolean isRenamed(SchemaField curField, SchemaField schemaField) {
    return curField.getNativeDataType().equals(schemaField.getNativeDataType())
        && parentFieldsMatch(curField, schemaField)
        && descriptionsMatch(curField, schemaField);
  }

  private static boolean parentFieldsMatch(SchemaField curField, SchemaField schemaField) {
    int curFieldIndex = curField.getFieldPath().lastIndexOf(".");
    int schemaFieldIndex = schemaField.getFieldPath().lastIndexOf(".");
    if (curFieldIndex > 0 && schemaFieldIndex > 0) {
      String curFieldParentPath = curField.getFieldPath().substring(0, curFieldIndex);
      String schemaFieldParentPath = schemaField.getFieldPath().substring(0, schemaFieldIndex);
      return StringUtils.isNotBlank(curFieldParentPath)
          && curFieldParentPath.equals(schemaFieldParentPath);
    }
    // No parent field
    return curFieldIndex < 0 && schemaFieldIndex < 0;
  }

  private static boolean descriptionsMatch(SchemaField curField, SchemaField schemaField) {
    if (StringUtils.isBlank(curField.getDescription())
        && StringUtils.isBlank(schemaField.getDescription())) {
      return true;
    }
    return !(StringUtils.isBlank(curField.getDescription())
            && StringUtils.isNotBlank(schemaField.getDescription()))
        && !(StringUtils.isNotBlank(curField.getDescription())
            && StringUtils.isBlank(schemaField.getDescription()))
        && curField.getDescription().equals(schemaField.getDescription());
  }

  private static void processRemoval(
      ChangeCategory changeCategory,
      List<ChangeEvent> changeEvents,
      Urn datasetUrn,
      SchemaField baseField,
      AuditStamp auditStamp) {
    if (ChangeCategory.TECHNICAL_SCHEMA.equals(changeCategory)) {
      changeEvents.add(
          DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
              .modifier(getSchemaFieldUrn(datasetUrn, baseField).toString())
              .entityUrn(datasetUrn.toString())
              .category(ChangeCategory.TECHNICAL_SCHEMA)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MAJOR)
              .description(
                  BACKWARDS_INCOMPATIBLE_DESC
                      + " removal of field: '"
                      + getFieldPathV1(baseField)
                      + "'.")
              .fieldPath(baseField.getFieldPath())
              .fieldUrn(getSchemaFieldUrn(datasetUrn, baseField))
              .nullable(baseField.isNullable())
              .auditStamp(auditStamp)
              .build());
    }
    List<ChangeEvent> propChangeEvents =
        getFieldPropertyChangeEvents(baseField, null, datasetUrn, changeCategory, auditStamp);
    changeEvents.addAll(propChangeEvents);
  }

  private static void processAdd(
      ChangeCategory changeCategory,
      List<ChangeEvent> changeEvents,
      Urn datasetUrn,
      SchemaField targetField,
      AuditStamp auditStamp) {
    if (ChangeCategory.TECHNICAL_SCHEMA.equals(changeCategory)) {
      changeEvents.add(
          DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
              .modifier(getSchemaFieldUrn(datasetUrn, targetField).toString())
              .entityUrn(datasetUrn.toString())
              .category(ChangeCategory.TECHNICAL_SCHEMA)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  BACK_AND_FORWARD_COMPATIBLE_DESC
                      + "the newly added field '"
                      + getFieldPathV1(targetField)
                      + "'.")
              .fieldPath(targetField.getFieldPath())
              .fieldUrn(getSchemaFieldUrn(datasetUrn, targetField))
              .nullable(targetField.isNullable())
              .auditStamp(auditStamp)
              .build());
    }
    List<ChangeEvent> propChangeEvents =
        getFieldPropertyChangeEvents(null, targetField, datasetUrn, changeCategory, auditStamp);
    changeEvents.addAll(propChangeEvents);
  }

  private static ChangeEvent generateRenameEvent(
      Urn datasetUrn, SchemaField curBaseField, SchemaField curTargetField, AuditStamp auditStamp) {
    return DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
        .category(ChangeCategory.TECHNICAL_SCHEMA)
        .modifier(getSchemaFieldUrn(datasetUrn, curBaseField).toString())
        .entityUrn(datasetUrn.toString())
        .operation(ChangeOperation.MODIFY)
        .semVerChange(SemanticChangeType.MINOR)
        .description(
            BACK_AND_FORWARD_COMPATIBLE_DESC
                + "renaming of the field '"
                + getFieldPathV1(curBaseField)
                + " to "
                + getFieldPathV1(curTargetField)
                + "'.")
        .fieldPath(curBaseField.getFieldPath())
        .fieldUrn(getSchemaFieldUrn(datasetUrn, curBaseField))
        .nullable(curBaseField.isNullable())
        .modificationCategory(SchemaFieldModificationCategory.RENAME)
        .auditStamp(auditStamp)
        .build();
  }

  private static SchemaMetadata getSchemaMetadataFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(SchemaMetadata.class, entityAspect.getMetadata());
    }
    return null;
  }

  @SuppressWarnings("UnnecessaryLocalVariable")
  private static List<ChangeEvent> getForeignKeyChangeEvents() {
    List<ChangeEvent> foreignKeyChangeEvents = new ArrayList<>();
    // TODO: Implement the diffing logic.
    return foreignKeyChangeEvents;
  }

  private static List<ChangeEvent> getPrimaryKeyChangeEvents(
      SchemaMetadata baseSchema,
      SchemaMetadata targetSchema,
      Urn datasetUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> primaryKeyChangeEvents = new ArrayList<>();
    Set<String> basePrimaryKeys =
        (baseSchema != null && baseSchema.getPrimaryKeys() != null)
            ? new HashSet<>(baseSchema.getPrimaryKeys())
            : new HashSet<>();
    Set<String> targetPrimaryKeys =
        (targetSchema != null && targetSchema.getPrimaryKeys() != null)
            ? new HashSet<>(targetSchema.getPrimaryKeys())
            : new HashSet<>();
    Set<String> removedBaseKeys =
        basePrimaryKeys.stream()
            .filter(key -> !targetPrimaryKeys.contains(key))
            .collect(Collectors.toSet());
    for (String removedBaseKeyField : removedBaseKeys) {
      primaryKeyChangeEvents.add(
          ChangeEvent.builder()
              .category(ChangeCategory.TECHNICAL_SCHEMA)
              .modifier(getSchemaFieldUrn(datasetUrn.toString(), removedBaseKeyField).toString())
              .entityUrn(datasetUrn.toString())
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MAJOR)
              .description(
                  BACKWARDS_INCOMPATIBLE_DESC
                      + " removal of the primary key field '"
                      + removedBaseKeyField
                      + "'")
              .auditStamp(auditStamp)
              .build());
    }

    Set<String> addedTargetKeys =
        targetPrimaryKeys.stream()
            .filter(key -> !basePrimaryKeys.contains(key))
            .collect(Collectors.toSet());
    for (String addedTargetKeyField : addedTargetKeys) {
      primaryKeyChangeEvents.add(
          ChangeEvent.builder()
              .category(ChangeCategory.TECHNICAL_SCHEMA)
              .modifier(getSchemaFieldUrn(datasetUrn, addedTargetKeyField).toString())
              .entityUrn(datasetUrn.toString())
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MAJOR)
              .description(
                  BACKWARDS_INCOMPATIBLE_DESC
                      + " addition of the primary key field '"
                      + addedTargetKeyField
                      + "'")
              .auditStamp(auditStamp)
              .build());
    }
    return primaryKeyChangeEvents;
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory changeCategory,
      JsonPatch rawDiff,
      boolean rawDiffRequested) {
    if (!previousValue.getAspect().equals(SCHEMA_METADATA_ASPECT_NAME)
        || !currentValue.getAspect().equals(SCHEMA_METADATA_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + SCHEMA_METADATA_ASPECT_NAME);
    }

    SchemaMetadata baseSchema = getSchemaMetadataFromAspect(previousValue);
    SchemaMetadata targetSchema = getSchemaMetadataFromAspect(currentValue);

    if (targetSchema == null) {
      throw new IllegalStateException("SchemaMetadata targetSchema should not be null");
    }

    List<ChangeEvent> changeEvents;
    try {
      changeEvents =
          new ArrayList<>(
              computeDiffs(
                  baseSchema,
                  targetSchema,
                  DatasetUrn.createFromString(currentValue.getUrn()),
                  changeCategory,
                  null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Malformed DatasetUrn " + currentValue.getUrn());
    }

    // Assess the highest change at the transaction(schema) level.
    SemanticChangeType highestSematicChange = SemanticChangeType.NONE;
    changeEvents =
        changeEvents.stream()
            .filter(changeEvent -> changeEvent.getCategory() == changeCategory)
            .collect(Collectors.toList());
    ChangeEvent highestChangeEvent =
        changeEvents.stream().max(Comparator.comparing(ChangeEvent::getSemVerChange)).orElse(null);
    if (highestChangeEvent != null) {
      highestSematicChange = highestChangeEvent.getSemVerChange();
    }
    return ChangeTransaction.builder()
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffRequested ? rawDiff : null)
        .semVerChange(highestSematicChange)
        .actor(currentValue.getCreatedBy())
        .build();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<SchemaMetadata> from,
      @Nonnull Aspect<SchemaMetadata> to,
      @Nonnull AuditStamp auditStamp) {
    final List<ChangeEvent> changeEvents = new ArrayList<>();
    changeEvents.addAll(
        computeDiffs(
            from.getValue(), to.getValue(), urn, ChangeCategory.DOCUMENTATION, auditStamp));
    changeEvents.addAll(
        computeDiffs(from.getValue(), to.getValue(), urn, ChangeCategory.TAG, auditStamp));
    changeEvents.addAll(
        computeDiffs(
            from.getValue(), to.getValue(), urn, ChangeCategory.TECHNICAL_SCHEMA, auditStamp));
    changeEvents.addAll(
        computeDiffs(
            from.getValue(), to.getValue(), urn, ChangeCategory.GLOSSARY_TERM, auditStamp));
    return changeEvents;
  }
}
