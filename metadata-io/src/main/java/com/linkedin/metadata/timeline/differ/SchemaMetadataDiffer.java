package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.dataset.DatasetSchemaFieldChangeEvent;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;

import static com.linkedin.metadata.timeline.differ.DifferUtils.*;


public class SchemaMetadataDiffer implements AspectDiffer<SchemaMetadata> {
  private static final String SCHEMA_METADATA_ASPECT_NAME = "schemaMetadata";
  private static final String BACKWARDS_INCOMPATIBLE_DESC = "A backwards incompatible change due to";
  private static final String FORWARDS_COMPATIBLE_DESC = "A forwards compatible change due to ";
  private static final String BACK_AND_FORWARD_COMPATIBLE_DESC = "A forwards & backwards compatible change due to ";
  private static final String FIELD_DESCRIPTION_ADDED_FORMAT =
      "The description '%s' for the field '%s' has been added.";
  private static final String FIELD_DESCRIPTION_REMOVED_FORMAT =
      "The description '%s' for the field '%s' has been removed.";
  private static final String FIELD_DESCRIPTION_MODIFIED_FORMAT =
      "The description for the field '%s' has been changed from '%s' to '%s'.";

  private static ChangeEvent getDescriptionChange(SchemaField baseField, SchemaField targetField,
      String datasetFieldUrn, AuditStamp auditStamp) {
    String baseDesciption = (baseField != null) ? baseField.getDescription() : null;
    String targetDescription = (targetField != null) ? targetField.getDescription() : null;
    if (baseDesciption == null && targetDescription != null) {
      // Description got added.
      return ChangeEvent.builder()
          .operation(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .category(ChangeCategory.DOCUMENTATION)
          .entityUrn(datasetFieldUrn)
          .description(String.format(FIELD_DESCRIPTION_ADDED_FORMAT, targetDescription, targetField.getFieldPath()))
          .auditStamp(auditStamp)
          .build();
    }
    if (baseDesciption != null && targetDescription == null) {
      // Description removed.
      return ChangeEvent.builder()
          .operation(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .category(ChangeCategory.DOCUMENTATION)
          .entityUrn(datasetFieldUrn)
          .description(String.format(FIELD_DESCRIPTION_REMOVED_FORMAT, baseDesciption, baseField.getFieldPath()))
          .auditStamp(auditStamp)
          .build();
    }
    if (baseDesciption != null && !baseDesciption.equals(targetDescription)) {
      // Description Change
      return ChangeEvent.builder()
          .operation(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.PATCH)
          .category(ChangeCategory.DOCUMENTATION)
          .entityUrn(datasetFieldUrn)
          .description(String.format(FIELD_DESCRIPTION_MODIFIED_FORMAT, baseField.getFieldPath(), baseDesciption,
              targetDescription))
          .auditStamp(auditStamp)
          .build();
    }
    return null;
  }

  private static List<ChangeEvent> getGlobalTagChangeEvents(SchemaField baseField, SchemaField targetField,
      String parentUrn,
      String datasetFieldUrn,
      AuditStamp auditStamp) {

    // 1. Get EntityTagChangeEvent, then rebind into a SchemaFieldTagChangeEvent.
    List<ChangeEvent> entityTagChangeEvents = GlobalTagsDiffer.computeDiffs(baseField != null ? baseField.getGlobalTags() : null,
        targetField != null ? targetField.getGlobalTags() : null, datasetFieldUrn, auditStamp);

    if (baseField != null || targetField != null) {
      String fieldPath = targetField != null ? targetField.getFieldPath() : baseField.getFieldPath();
      // 2. Convert EntityTagChangeEvent into a SchemaFieldTagChangeEvent.
      return convertEntityTagChangeEvents(
          fieldPath,
          UrnUtils.getUrn(parentUrn),
          entityTagChangeEvents);
    }

    return Collections.emptyList();
  }

  private static List<ChangeEvent> getGlossaryTermsChangeEvents(SchemaField baseField, SchemaField targetField,
      String parentUrn,
      String datasetFieldUrn,
      AuditStamp auditStamp) {

    // 1. Get EntityGlossaryTermChangeEvent, then rebind into a SchemaFieldGlossaryTermChangeEvent.
    List<ChangeEvent> entityGlossaryTermsChangeEvents = GlossaryTermsDiffer.computeDiffs(baseField != null ? baseField.getGlossaryTerms() : null,
        targetField != null ? targetField.getGlossaryTerms() : null, datasetFieldUrn, auditStamp);

    if (targetField != null || baseField != null) {
      String fieldPath = targetField != null ? targetField.getFieldPath() : baseField.getFieldPath();
      // 2. Convert EntityGlossaryTermChangeEvent into a SchemaFieldGlossaryTermChangeEvent.
      return convertEntityGlossaryTermChangeEvents(
          fieldPath,
          UrnUtils.getUrn(parentUrn),
          entityGlossaryTermsChangeEvents);
    }

    return Collections.emptyList();
  }

  private static List<ChangeEvent> getFieldPropertyChangeEvents(SchemaField baseField, SchemaField targetField,
      Urn datasetUrn, ChangeCategory changeCategory, AuditStamp auditStamp) {
    List<ChangeEvent> propChangeEvents = new ArrayList<>();
    String datasetFieldUrn;
    if (targetField != null) {
      datasetFieldUrn = getSchemaFieldUrn(datasetUrn, targetField).toString();
    } else {
      datasetFieldUrn = getSchemaFieldUrn(datasetUrn, baseField).toString();
    }

    // Description Change.
    if (ChangeCategory.DOCUMENTATION.equals(changeCategory)) {
      ChangeEvent descriptionChangeEvent = getDescriptionChange(baseField, targetField, datasetFieldUrn, auditStamp);
      if (descriptionChangeEvent != null) {
        propChangeEvents.add(descriptionChangeEvent);
      }
    }

    // Global Tags
    if (ChangeCategory.TAG.equals(changeCategory)) {
      propChangeEvents.addAll(getGlobalTagChangeEvents(baseField, targetField, datasetUrn.toString(), datasetFieldUrn, auditStamp));
    }

    // Glossary terms.
    if (ChangeCategory.GLOSSARY_TERM.equals(changeCategory)) {
      propChangeEvents.addAll(getGlossaryTermsChangeEvents(baseField, targetField, datasetUrn.toString(), datasetFieldUrn, auditStamp));
    }

    return propChangeEvents;
  }

  // TODO: This could use some cleanup, lots of repeated logic and tenuous conditionals
  private static List<ChangeEvent> computeDiffs(SchemaMetadata baseSchema, SchemaMetadata targetSchema,
      Urn datasetUrn, ChangeCategory changeCategory, AuditStamp auditStamp) {
    // Sort the fields by their field path.
    if (baseSchema != null) {
      sortFieldsByPath(baseSchema);
    }
    sortFieldsByPath(targetSchema);

    // Performs ordinal based diff, primarily based on fixed field ordinals and their types.
    SchemaFieldArray baseFields = (baseSchema != null ? baseSchema.getFields() : new SchemaFieldArray());
    SchemaFieldArray targetFields = targetSchema.getFields();
    int baseFieldIdx = 0;
    int targetFieldIdx = 0;
    List<ChangeEvent> changeEvents = new ArrayList<>();
    Set<SchemaField> renamedFields = new HashSet<>();
    while (baseFieldIdx < baseFields.size() && targetFieldIdx < targetFields.size()) {
      SchemaField curBaseField = baseFields.get(baseFieldIdx);
      SchemaField curTargetField = targetFields.get(targetFieldIdx);
      //TODO: Re-evaluate ordinal processing?
      int comparison = curBaseField.getFieldPath().compareTo(curTargetField.getFieldPath());
      if (renamedFields.contains(curBaseField)) {
        baseFieldIdx++;
      } else if (renamedFields.contains(curTargetField)) {
        targetFieldIdx++;
      } else if (comparison == 0) {
        // This is the same field. Check for change events from property changes.
        if (!curBaseField.getNativeDataType().equals(curTargetField.getNativeDataType())) {
          // Non-backward compatible change + Major version bump
          if (ChangeCategory.TECHNICAL_SCHEMA.equals(changeCategory)) {
            changeEvents.add(DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
                .category(ChangeCategory.TECHNICAL_SCHEMA)
                .modifier(getSchemaFieldUrn(datasetUrn, curBaseField).toString())
                .entityUrn(datasetUrn.toString())
                .operation(ChangeOperation.MODIFY)
                .semVerChange(SemanticChangeType.MAJOR)
                .description(String.format("%s native datatype of the field '%s' changed from '%s' to '%s'.",
                    BACKWARDS_INCOMPATIBLE_DESC, getFieldPathV1(curTargetField), curBaseField.getNativeDataType(),
                    curTargetField.getNativeDataType()))
                .fieldPath(curBaseField.getFieldPath())
                .fieldUrn(getSchemaFieldUrn(datasetUrn, curBaseField))
                .nullable(curBaseField.isNullable())
                .auditStamp(auditStamp)
                .build());
          }
          List<ChangeEvent> propChangeEvents = getFieldPropertyChangeEvents(curBaseField, curTargetField, datasetUrn,
              changeCategory, auditStamp);
          changeEvents.addAll(propChangeEvents);
          ++baseFieldIdx;
          ++targetFieldIdx;
        }
        List<ChangeEvent> propChangeEvents =
            getFieldPropertyChangeEvents(curBaseField, curTargetField, datasetUrn, changeCategory, auditStamp);
        changeEvents.addAll(propChangeEvents);
        ++baseFieldIdx;
        ++targetFieldIdx;
      } else if (comparison < 0) {
        // Base Field was removed or was renamed. Non-backward compatible change + Major version bump
        // Check for rename, if rename coincides with other modifications we assume drop/add.
        // Assumes that two different fields on the same schema would not have the same description, terms,
        // or tags and share the same type
        SchemaField renamedField = findRenamedField(curBaseField,
            targetFields.subList(targetFieldIdx, targetFields.size()), renamedFields);
        if (renamedField == null) {
          processRemoval(changeCategory, changeEvents, datasetUrn, curBaseField, auditStamp);
          ++baseFieldIdx;
        } else {
          changeEvents.add(generateRenameEvent(datasetUrn, curBaseField, renamedField, auditStamp));
          List<ChangeEvent> propChangeEvents = getFieldPropertyChangeEvents(curBaseField, curTargetField, datasetUrn,
              changeCategory, auditStamp);
          changeEvents.addAll(propChangeEvents);
          ++baseFieldIdx;
          renamedFields.add(renamedField);
        }
      } else {
        // The targetField got added or a rename occurred. Forward & backwards compatible change + minor version bump.
        SchemaField renamedField = findRenamedField(curTargetField,
            baseFields.subList(baseFieldIdx, baseFields.size()), renamedFields);
        if (renamedField == null) {
          processAdd(changeCategory, changeEvents, datasetUrn, curTargetField, auditStamp);
          ++targetFieldIdx;
        } else {
          changeEvents.add(generateRenameEvent(datasetUrn, renamedField, curTargetField, auditStamp));
          List<ChangeEvent> propChangeEvents = getFieldPropertyChangeEvents(curBaseField, curTargetField, datasetUrn,
              changeCategory, auditStamp);
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
        processRemoval(changeCategory, changeEvents, datasetUrn, baseField, auditStamp);
      }
      ++baseFieldIdx;
    }
    while (targetFieldIdx < targetFields.size()) {
      // Newly added fields. Forwards & backwards compatible change + minor version bump.
      SchemaField targetField = targetFields.get(targetFieldIdx);
      if (!renamedFields.contains(targetField)) {
        processAdd(changeCategory, changeEvents, datasetUrn, targetField, auditStamp);
      }
      targetFieldIdx++;
    }

    // Handle primary key constraint change events.
    List<ChangeEvent> primaryKeyChangeEvents = getPrimaryKeyChangeEvents(baseSchema, targetSchema, datasetUrn, auditStamp);
    changeEvents.addAll(primaryKeyChangeEvents);

    // Handle foreign key constraint change events.
    List<ChangeEvent> foreignKeyChangeEvents = getForeignKeyChangeEvents(baseSchema, targetSchema);
    changeEvents.addAll(foreignKeyChangeEvents);

    return changeEvents;
  }

  private static void sortFieldsByPath(SchemaMetadata schemaMetadata) {
    assert (schemaMetadata != null);
    List<SchemaField> schemaFields = new ArrayList<>(schemaMetadata.getFields());
    schemaFields.sort(Comparator.comparing(SchemaField::getFieldPath));
    schemaMetadata.setFields(new SchemaFieldArray(schemaFields));
  }

  private static SchemaField findRenamedField(SchemaField curField, List<SchemaField> targetFields, Set<SchemaField> renamedFields) {
    return targetFields.stream()
        .filter(schemaField -> isRenamed(curField, schemaField))
        .filter(field -> !renamedFields.contains(field))
        .findFirst().orElse(null);
  }

  private static boolean isRenamed(SchemaField curField, SchemaField schemaField) {
    return curField.getNativeDataType().equals(schemaField.getNativeDataType())
        && parentFieldsMatch(curField, schemaField) && descriptionsMatch(curField, schemaField);
  }

  private static boolean parentFieldsMatch(SchemaField curField, SchemaField schemaField) {
    int curFieldIndex = curField.getFieldPath().lastIndexOf(".");
    int schemaFieldIndex = schemaField.getFieldPath().lastIndexOf(".");
    if (curFieldIndex > 0 && schemaFieldIndex > 0) {
      String curFieldParentPath = curField.getFieldPath().substring(0, curFieldIndex);
      String schemaFieldParentPath = schemaField.getFieldPath().substring(0, schemaFieldIndex);
      return StringUtils.isNotBlank(curFieldParentPath) && curFieldParentPath.equals(schemaFieldParentPath);
    }
    // No parent field
    return curFieldIndex < 0 && schemaFieldIndex < 0;
  }

  private static boolean descriptionsMatch(SchemaField curField, SchemaField schemaField) {
    return StringUtils.isNotBlank(curField.getDescription()) && curField.getDescription().equals(schemaField.getDescription());
  }

  private static void processRemoval(ChangeCategory changeCategory, List<ChangeEvent> changeEvents, Urn datasetUrn,
      SchemaField baseField, AuditStamp auditStamp) {
    if (ChangeCategory.TECHNICAL_SCHEMA.equals(changeCategory)) {
      changeEvents.add(DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
          .modifier(getSchemaFieldUrn(datasetUrn, baseField).toString())
          .entityUrn(datasetUrn.toString())
          .category(ChangeCategory.TECHNICAL_SCHEMA)
          .operation(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MAJOR)
          .description(BACKWARDS_INCOMPATIBLE_DESC + " removal of field: '" + getFieldPathV1(baseField) + "'.")
          .fieldPath(baseField.getFieldPath())
          .fieldUrn(getSchemaFieldUrn(datasetUrn, baseField))
          .nullable(baseField.isNullable())
          .auditStamp(auditStamp)
          .build());
    }
    List<ChangeEvent> propChangeEvents = getFieldPropertyChangeEvents(baseField, null, datasetUrn,
        changeCategory, auditStamp);
    changeEvents.addAll(propChangeEvents);
  }

  private static void processAdd(ChangeCategory changeCategory, List<ChangeEvent> changeEvents, Urn datasetUrn,
      SchemaField targetField, AuditStamp auditStamp) {
    if (ChangeCategory.TECHNICAL_SCHEMA.equals(changeCategory)) {
      changeEvents.add(DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
          .modifier(getSchemaFieldUrn(datasetUrn, targetField).toString())
          .entityUrn(datasetUrn.toString())
          .category(ChangeCategory.TECHNICAL_SCHEMA)
          .operation(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(BACK_AND_FORWARD_COMPATIBLE_DESC + "the newly added field '" + getFieldPathV1(targetField) + "'.")
          .fieldPath(targetField.getFieldPath())
          .fieldUrn(getSchemaFieldUrn(datasetUrn, targetField))
          .nullable(targetField.isNullable())
          .auditStamp(auditStamp)
          .build());
    }
    List<ChangeEvent> propChangeEvents = getFieldPropertyChangeEvents(null, targetField, datasetUrn,
        changeCategory, auditStamp);
    changeEvents.addAll(propChangeEvents);
  }

  private static ChangeEvent generateRenameEvent(Urn datasetUrn, SchemaField curBaseField, SchemaField curTargetField,
      AuditStamp auditStamp) {
      return DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
          .category(ChangeCategory.TECHNICAL_SCHEMA)
          .modifier(getSchemaFieldUrn(datasetUrn, curBaseField).toString())
          .entityUrn(datasetUrn.toString())
          .operation(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.MINOR)
          .description(BACK_AND_FORWARD_COMPATIBLE_DESC + "renaming of the field '" + getFieldPathV1(curBaseField)
              + " to " + getFieldPathV1(curTargetField) +  "'.")
          .fieldPath(curBaseField.getFieldPath())
          .fieldUrn(getSchemaFieldUrn(datasetUrn, curBaseField))
          .nullable(curBaseField.isNullable())
          .auditStamp(auditStamp)
          .build();
  }

  @SuppressWarnings("ConstantConditions")
  private static SchemaMetadata getSchemaMetadataFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(SchemaMetadata.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }

  @SuppressWarnings("UnnecessaryLocalVariable")
  private static List<ChangeEvent> getForeignKeyChangeEvents(SchemaMetadata baseSchema, SchemaMetadata targetSchema) {
    List<ChangeEvent> foreignKeyChangeEvents = new ArrayList<>();
    // TODO: Implement the diffing logic.
    return foreignKeyChangeEvents;
  }

  private static List<ChangeEvent> getPrimaryKeyChangeEvents(SchemaMetadata baseSchema, SchemaMetadata targetSchema,
      Urn datasetUrn, AuditStamp auditStamp) {
    List<ChangeEvent> primaryKeyChangeEvents = new ArrayList<>();
    Set<String> basePrimaryKeys =
        (baseSchema != null && baseSchema.getPrimaryKeys() != null) ? new HashSet<>(baseSchema.getPrimaryKeys())
            : new HashSet<>();
    Set<String> targetPrimaryKeys =
        (targetSchema.getPrimaryKeys() != null) ? new HashSet<>(targetSchema.getPrimaryKeys()) : new HashSet<>();
    Set<String> removedBaseKeys =
        basePrimaryKeys.stream().filter(key -> !targetPrimaryKeys.contains(key)).collect(Collectors.toSet());
    for (String removedBaseKeyField : removedBaseKeys) {
      primaryKeyChangeEvents.add(ChangeEvent.builder()
          .category(ChangeCategory.TECHNICAL_SCHEMA)
          .modifier(getSchemaFieldUrn(datasetUrn.toString(), removedBaseKeyField).toString())
          .entityUrn(datasetUrn.toString())
          .operation(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.MAJOR)
          .description(BACKWARDS_INCOMPATIBLE_DESC + " removal of the primary key field '" + removedBaseKeyField + "'")
          .auditStamp(auditStamp)
          .build());
    }

    Set<String> addedTargetKeys =
        targetPrimaryKeys.stream().filter(key -> !basePrimaryKeys.contains(key)).collect(Collectors.toSet());
    for (String addedTargetKeyField : addedTargetKeys) {
      primaryKeyChangeEvents.add(ChangeEvent.builder()
          .category(ChangeCategory.TECHNICAL_SCHEMA)
          .modifier(getSchemaFieldUrn(datasetUrn, addedTargetKeyField).toString())
          .entityUrn(datasetUrn.toString())
          .operation(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.MAJOR)
          .description(BACKWARDS_INCOMPATIBLE_DESC + " addition of the primary key field '" + addedTargetKeyField + "'")
          .auditStamp(auditStamp)
          .build());
    }
    return primaryKeyChangeEvents;
  }

  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory changeCategory, JsonPatch rawDiff, boolean rawDiffRequested) {
    if (!previousValue.getAspect().equals(SCHEMA_METADATA_ASPECT_NAME) || !currentValue.getAspect()
        .equals(SCHEMA_METADATA_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + SCHEMA_METADATA_ASPECT_NAME);
    }

    SchemaMetadata baseSchema = getSchemaMetadataFromAspect(previousValue);
    SchemaMetadata targetSchema = getSchemaMetadataFromAspect(currentValue);
    assert (targetSchema != null);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    try {
      changeEvents.addAll(
          computeDiffs(baseSchema, targetSchema, DatasetUrn.createFromString(currentValue.getUrn()), changeCategory, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Malformed DatasetUrn " + currentValue.getUrn());
    }

    // Assess the highest change at the transaction(schema) level.
    SemanticChangeType highestSematicChange = SemanticChangeType.NONE;
    changeEvents =
        changeEvents.stream().filter(changeEvent -> changeEvent.getCategory() == changeCategory).collect(Collectors.toList());
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
    changeEvents.addAll(computeDiffs(from.getValue(), to.getValue(), urn, ChangeCategory.DOCUMENTATION, auditStamp));
    changeEvents.addAll(computeDiffs(from.getValue(), to.getValue(), urn, ChangeCategory.TAG, auditStamp));
    changeEvents.addAll(computeDiffs(from.getValue(), to.getValue(), urn, ChangeCategory.TECHNICAL_SCHEMA, auditStamp));
    changeEvents.addAll(computeDiffs(from.getValue(), to.getValue(), urn, ChangeCategory.GLOSSARY_TERM, auditStamp));
    return changeEvents;
  }

}
