package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.timeline.eventgenerator.ChangeEventGeneratorUtils.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
      Set<ChangeCategory> changeCategories,
      AuditStamp auditStamp) {
    List<ChangeEvent> propChangeEvents = new ArrayList<>();
    String datasetFieldUrn;
    if (targetField != null) {
      datasetFieldUrn = getSchemaFieldUrn(datasetUrn, targetField).toString();
    } else {
      datasetFieldUrn = getSchemaFieldUrn(datasetUrn, baseField).toString();
    }

    // Description Change.
    if (changeCategories != null && changeCategories.contains(ChangeCategory.DOCUMENTATION)) {
      ChangeEvent descriptionChangeEvent =
          getDescriptionChange(baseField, targetField, datasetFieldUrn, auditStamp);
      if (descriptionChangeEvent != null) {
        propChangeEvents.add(descriptionChangeEvent);
      }
    }

    // Global Tags
    if (changeCategories != null && changeCategories.contains(ChangeCategory.TAG)) {
      propChangeEvents.addAll(
          getGlobalTagChangeEvents(
              baseField, targetField, datasetUrn.toString(), datasetFieldUrn, auditStamp));
    }

    // Glossary terms.
    if (changeCategories != null && changeCategories.contains(ChangeCategory.GLOSSARY_TERM)) {
      propChangeEvents.addAll(
          getGlossaryTermsChangeEvents(
              baseField, targetField, datasetUrn.toString(), datasetFieldUrn, auditStamp));
    }

    return propChangeEvents;
  }

  private static List<ChangeEvent> computeDiffs(
      SchemaMetadata baseSchema,
      SchemaMetadata targetSchema,
      Urn datasetUrn,
      Set<ChangeCategory> changeCategories,
      AuditStamp auditStamp) {
    // Sort the fields by their field path. This aligns both sets of fields based on field paths for
    // comparisons.
    if (baseSchema != null) {
      sortFieldsByPath(baseSchema);
    }
    if (targetSchema != null) {
      sortFieldsByPath(targetSchema);
    }

    SchemaFieldArray baseFields =
        (baseSchema != null ? baseSchema.getFields() : new SchemaFieldArray());
    SchemaFieldArray targetFields =
        targetSchema != null ? targetSchema.getFields() : new SchemaFieldArray();
    int baseFieldIdx = 0;
    int targetFieldIdx = 0;
    List<ChangeEvent> changeEvents = new ArrayList<>();
    Set<SchemaField> renamedFields = new HashSet<>();

    // Compares each sorted base field with the target field, tries to reconcile name changes by
    // matching field properties
    while (baseFieldIdx < baseFields.size() && targetFieldIdx < targetFields.size()) {
      SchemaField curBaseField = baseFields.get(baseFieldIdx);
      SchemaField curTargetField = targetFields.get(targetFieldIdx);
      int comparison = curBaseField.getFieldPath().compareTo(curTargetField.getFieldPath());
      if (renamedFields.contains(curBaseField)) {
        baseFieldIdx++;
      } else if (renamedFields.contains(curTargetField)) {
        targetFieldIdx++;
      } else if (comparison == 0) {
        // This is the same field. Check for change events from property changes.
        if (!curBaseField.getNativeDataType().equals(curTargetField.getNativeDataType())) {
          processNativeTypeChange(
              changeCategories, changeEvents, datasetUrn, curBaseField, curTargetField, auditStamp);
        }
        List<ChangeEvent> propChangeEvents =
            getFieldPropertyChangeEvents(
                curBaseField, curTargetField, datasetUrn, changeCategories, auditStamp);
        changeEvents.addAll(propChangeEvents);
        ++baseFieldIdx;
        ++targetFieldIdx;
      } else if (comparison < 0) {
        // Base Field was removed or was renamed. Non-backward compatible change + Major version
        // bump for removal
        // Forwards/Backwards compatible change and Minor version bump for rename
        // Check for rename, if rename coincides with other modifications we assume drop/add.
        // Assumes that two different fields on the same schema would not have the same description,
        // terms, and tags and share the same type
        SchemaField renamedField =
            findRenamedField(
                curBaseField,
                new HashSet<>(baseFields.subList(baseFieldIdx, baseFields.size())),
                targetFields.subList(targetFieldIdx, targetFields.size()),
                renamedFields);
        if (renamedField == null) {
          processRemoval(changeCategories, changeEvents, datasetUrn, curBaseField, auditStamp);
          ++baseFieldIdx;
        } else {
          if (changeCategories != null
              && changeCategories.contains(ChangeCategory.TECHNICAL_SCHEMA)) {
            changeEvents.add(
                generateRenameEvent(datasetUrn, curBaseField, renamedField, auditStamp));
          }
          List<ChangeEvent> propChangeEvents =
              getFieldPropertyChangeEvents(
                  curBaseField, renamedField, datasetUrn, changeCategories, auditStamp);
          changeEvents.addAll(propChangeEvents);
          ++baseFieldIdx;
          renamedFields.add(renamedField);
        }
      } else {
        // The targetField got added or a renaming occurred. Forward & backwards compatible change +
        // minor version bump for both.
        SchemaField renamedField =
            findRenamedField(
                curTargetField,
                new HashSet<>(targetFields.subList(targetFieldIdx, targetFields.size())),
                baseFields.subList(baseFieldIdx, baseFields.size()),
                renamedFields);
        if (renamedField == null) {
          processAdd(changeCategories, changeEvents, datasetUrn, curTargetField, auditStamp);
          ++targetFieldIdx;
        } else {
          if (changeCategories != null
              && changeCategories.contains(ChangeCategory.TECHNICAL_SCHEMA)) {
            changeEvents.add(
                generateRenameEvent(datasetUrn, renamedField, curTargetField, auditStamp));
          }
          List<ChangeEvent> propChangeEvents =
              getFieldPropertyChangeEvents(
                  renamedField, curTargetField, datasetUrn, changeCategories, auditStamp);
          changeEvents.addAll(propChangeEvents);
          ++targetFieldIdx;
          renamedFields.add(renamedField);
        }
      }
    }
    while (baseFieldIdx < baseFields.size()) {
      // Handle removed fields. Non-backward compatible change + major version bump
      SchemaField baseField = baseFields.get(baseFieldIdx);
      if (!renamedFields.contains(baseField)) {
        processRemoval(changeCategories, changeEvents, datasetUrn, baseField, auditStamp);
      }
      ++baseFieldIdx;
    }
    while (targetFieldIdx < targetFields.size()) {
      // Newly added fields. Forwards & backwards compatible change + minor version bump.
      SchemaField targetField = targetFields.get(targetFieldIdx);
      if (!renamedFields.contains(targetField)) {
        processAdd(changeCategories, changeEvents, datasetUrn, targetField, auditStamp);
      }
      ++targetFieldIdx;
    }

    // Handle primary key constraint change events.
    List<ChangeEvent> primaryKeyChangeEvents =
        getPrimaryKeyChangeEvents(
            changeCategories, baseSchema, targetSchema, datasetUrn, auditStamp);
    changeEvents.addAll(primaryKeyChangeEvents);

    // Handle foreign key constraint change events, currently no-op due to field not being utilized.
    List<ChangeEvent> foreignKeyChangeEvents = getForeignKeyChangeEvents();
    changeEvents.addAll(foreignKeyChangeEvents);

    return changeEvents;
  }

  private static void sortFieldsByPath(SchemaMetadata schemaMetadata) {
    if (schemaMetadata == null) {
      throw new IllegalArgumentException("SchemaMetadata should not be null");
    }
    List<SchemaField> schemaFields = new ArrayList<>(schemaMetadata.getFields());
    schemaFields.sort(Comparator.comparing(SchemaField::getFieldPath));
    schemaMetadata.setFields(new SchemaFieldArray(schemaFields));
  }

  private static SchemaField findRenamedField(
      SchemaField curField,
      Set<SchemaField> baseFields,
      List<SchemaField> targetFields,
      Set<SchemaField> renamedFields) {
    return targetFields.stream()
        .filter(schemaField -> isRenamed(curField, schemaField))
        .filter(field -> !renamedFields.contains(field))
        .filter(field -> !baseFields.contains(field)) // Filter out fields that will match later
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
      return StringUtils.equals(curFieldParentPath, schemaFieldParentPath);
    }
    // No parent field
    return curFieldIndex < 0 && schemaFieldIndex < 0;
  }

  private static boolean descriptionsMatch(SchemaField curField, SchemaField schemaField) {
    return StringUtils.equals(curField.getDescription(), schemaField.getDescription());
  }

  private static void processRemoval(
      Set<ChangeCategory> changeCategories,
      List<ChangeEvent> changeEvents,
      Urn datasetUrn,
      SchemaField baseField,
      AuditStamp auditStamp) {
    if (changeCategories != null && changeCategories.contains(ChangeCategory.TECHNICAL_SCHEMA)) {
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
              .modificationCategory(SchemaFieldModificationCategory.OTHER)
              .auditStamp(auditStamp)
              .build());
    }
    List<ChangeEvent> propChangeEvents =
        getFieldPropertyChangeEvents(baseField, null, datasetUrn, changeCategories, auditStamp);
    changeEvents.addAll(propChangeEvents);
  }

  private static void processAdd(
      Set<ChangeCategory> changeCategories,
      List<ChangeEvent> changeEvents,
      Urn datasetUrn,
      SchemaField targetField,
      AuditStamp auditStamp) {
    if (changeCategories != null && changeCategories.contains(ChangeCategory.TECHNICAL_SCHEMA)) {
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
              .modificationCategory(SchemaFieldModificationCategory.OTHER)
              .build());
    }
    List<ChangeEvent> propChangeEvents =
        getFieldPropertyChangeEvents(null, targetField, datasetUrn, changeCategories, auditStamp);
    changeEvents.addAll(propChangeEvents);
  }

  private static void processNativeTypeChange(
      Set<ChangeCategory> changeCategories,
      List<ChangeEvent> changeEvents,
      Urn datasetUrn,
      SchemaField curBaseField,
      SchemaField curTargetField,
      AuditStamp auditStamp) {
    // Non-backward compatible change + Major version bump
    if (changeCategories != null && changeCategories.contains(ChangeCategory.TECHNICAL_SCHEMA)) {
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
      Set<ChangeCategory> changeCategories,
      SchemaMetadata baseSchema,
      SchemaMetadata targetSchema,
      Urn datasetUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> primaryKeyChangeEvents = new ArrayList<>();
    if (changeCategories != null && changeCategories.contains(ChangeCategory.TECHNICAL_SCHEMA)) {
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
        Urn schemaFieldUrn = getSchemaFieldUrn(datasetUrn.toString(), removedBaseKeyField);
        primaryKeyChangeEvents.add(
            DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
                .category(ChangeCategory.TECHNICAL_SCHEMA)
                .modifier(schemaFieldUrn.toString())
                .fieldUrn(schemaFieldUrn)
                .fieldPath(removedBaseKeyField)
                .entityUrn(datasetUrn.toString())
                .operation(ChangeOperation.MODIFY)
                .semVerChange(SemanticChangeType.MAJOR)
                .description(
                    BACKWARDS_INCOMPATIBLE_DESC
                        + " removal of the primary key field '"
                        + removedBaseKeyField
                        + "'")
                .auditStamp(auditStamp)
                .modificationCategory(SchemaFieldModificationCategory.OTHER)
                .build());
      }

      Set<String> addedTargetKeys =
          targetPrimaryKeys.stream()
              .filter(key -> !basePrimaryKeys.contains(key))
              .collect(Collectors.toSet());
      for (String addedTargetKeyField : addedTargetKeys) {
        Urn schemaFieldUrn = getSchemaFieldUrn(datasetUrn.toString(), addedTargetKeyField);
        primaryKeyChangeEvents.add(
            DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
                .category(ChangeCategory.TECHNICAL_SCHEMA)
                .modifier(getSchemaFieldUrn(datasetUrn, addedTargetKeyField).toString())
                .fieldUrn(schemaFieldUrn)
                .fieldPath(addedTargetKeyField)
                .entityUrn(datasetUrn.toString())
                .operation(ChangeOperation.MODIFY)
                .semVerChange(SemanticChangeType.MAJOR)
                .description(
                    BACKWARDS_INCOMPATIBLE_DESC
                        + " addition of the primary key field '"
                        + addedTargetKeyField
                        + "'")
                .auditStamp(auditStamp)
                .modificationCategory(SchemaFieldModificationCategory.OTHER)
                .build());
      }
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
                  Collections.singleton(changeCategory),
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
    return new ArrayList<>(
        computeDiffs(
            from.getValue(),
            to.getValue(),
            urn,
            ImmutableSet.of(
                ChangeCategory.DOCUMENTATION,
                ChangeCategory.TAG,
                ChangeCategory.TECHNICAL_SCHEMA,
                ChangeCategory.GLOSSARY_TERM),
            auditStamp));
  }
}
