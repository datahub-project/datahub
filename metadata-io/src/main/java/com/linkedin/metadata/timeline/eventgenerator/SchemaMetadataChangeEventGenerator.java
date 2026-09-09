package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.timeline.eventgenerator.ChangeEventGeneratorUtils.*;
import static com.linkedin.metadata.utils.SchemaFieldUtils.downgradeFieldPath;
import static com.linkedin.metadata.utils.SchemaFieldUtils.generateSchemaFieldUrn;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.dataset.DatasetSchemaFieldChangeEvent;
import com.linkedin.metadata.timeline.data.dataset.SchemaFieldModificationCategory;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import jakarta.json.JsonPatch;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

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
          // The schema history UI diffs the two values, so it cannot render the change without the
          // previous one. Both are non-null here: the branches above handle either being absent.
          .parameters(
              ImmutableMap.of(
                  "description", targetDescription, "previousDescription", baseDescription))
          .auditStamp(auditStamp)
          .build();
    }
    return null;
  }

  private static List<ChangeEvent> getGlobalTagChangeEvents(
      SchemaField baseField,
      SchemaField targetField,
      Urn parentUrn,
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
      return convertEntityTagChangeEvents(fieldPath, parentUrn, entityTagChangeEvents);
    }

    return Collections.emptyList();
  }

  private static List<ChangeEvent> getGlossaryTermsChangeEvents(
      SchemaField baseField,
      SchemaField targetField,
      Urn parentUrn,
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
      datasetFieldUrn = generateSchemaFieldUrn(datasetUrn, targetField).toString();
    } else {
      datasetFieldUrn = generateSchemaFieldUrn(datasetUrn, baseField).toString();
    }

    // Description Change.
    if (changeCategories != null && changeCategories.contains(ChangeCategory.DOCUMENTATION)) {
      ChangeEvent descriptionChangeEvent =
          getDescriptionChange(baseField, targetField, datasetFieldUrn, auditStamp);
      if (descriptionChangeEvent != null) {
        String fieldPath =
            targetField != null ? targetField.getFieldPath() : baseField.getFieldPath();
        descriptionChangeEvent =
            convertEntityDocumentationChangeEvent(fieldPath, datasetUrn, descriptionChangeEvent);
        propChangeEvents.add(descriptionChangeEvent);
      }
    }

    // Global Tags
    if (changeCategories != null && changeCategories.contains(ChangeCategory.TAG)) {
      propChangeEvents.addAll(
          getGlobalTagChangeEvents(
              baseField, targetField, datasetUrn, datasetFieldUrn, auditStamp));
    }

    // Glossary terms.
    if (changeCategories != null && changeCategories.contains(ChangeCategory.GLOSSARY_TERM)) {
      propChangeEvents.addAll(
          getGlossaryTermsChangeEvents(
              baseField, targetField, datasetUrn, datasetFieldUrn, auditStamp));
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

    // Connectors that build field paths through the Avro conversion pipeline (Glue, Hive, Trino,
    // Iceberg, Kafka, ...) encode the column type into the v2 field path, so changing a column's
    // type also changes its field path and the merge-join below can no longer align the two
    // versions of the same column. Pair those up ahead of time so they are reported as a single
    // MODIFY, matching what connectors emitting plain column names (Snowflake, BigQuery, ...) get.
    Map<SchemaField, MovedFieldGroup> movedFieldGroups =
        findMovedFieldGroups(baseFields, targetFields);
    Set<SchemaField> movedFieldCandidates = movedFieldGroups.keySet();
    Set<SchemaField> processedMovedFields = new HashSet<>();

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
      } else if (processedMovedFields.contains(curBaseField)) {
        ++baseFieldIdx;
      } else if (processedMovedFields.contains(curTargetField)) {
        ++targetFieldIdx;
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
        MovedFieldGroup movedGroup = movedFieldGroups.get(curBaseField);
        if (movedGroup != null) {
          processMovedFields(changeCategories, changeEvents, datasetUrn, movedGroup, auditStamp);
          movedGroup.markProcessed(processedMovedFields);
          ++baseFieldIdx;
          continue;
        }
        SchemaField renamedField =
            findRenamedField(
                curBaseField,
                new HashSet<>(baseFields.subList(baseFieldIdx, baseFields.size())),
                targetFields.subList(targetFieldIdx, targetFields.size()),
                renamedFields,
                movedFieldCandidates);
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
        MovedFieldGroup movedGroup = movedFieldGroups.get(curTargetField);
        if (movedGroup != null) {
          processMovedFields(changeCategories, changeEvents, datasetUrn, movedGroup, auditStamp);
          movedGroup.markProcessed(processedMovedFields);
          ++targetFieldIdx;
          continue;
        }
        SchemaField renamedField =
            findRenamedField(
                curTargetField,
                new HashSet<>(targetFields.subList(targetFieldIdx, targetFields.size())),
                baseFields.subList(baseFieldIdx, baseFields.size()),
                renamedFields,
                movedFieldCandidates);
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
      if (!renamedFields.contains(baseField) && !processedMovedFields.contains(baseField)) {
        processRemoval(changeCategories, changeEvents, datasetUrn, baseField, auditStamp);
      }
      ++baseFieldIdx;
    }
    while (targetFieldIdx < targetFields.size()) {
      // Newly added fields. Forwards & backwards compatible change + minor version bump.
      SchemaField targetField = targetFields.get(targetFieldIdx);
      if (!renamedFields.contains(targetField) && !processedMovedFields.contains(targetField)) {
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
      Set<SchemaField> renamedFields,
      Set<SchemaField> retypeCandidates) {
    return targetFields.stream()
        .filter(schemaField -> isRenamed(curField, schemaField))
        .filter(field -> !renamedFields.contains(field))
        .filter(field -> !retypeCandidates.contains(field)) // Already paired up as a type change
        .filter(field -> !baseFields.contains(field)) // Filter out fields that will match later
        .findFirst()
        .orElse(null);
  }

  /**
   * Pairs up fields that represent the same column before and after their field path moved, which
   * happens for schemas whose field paths embed the column type (v2 field paths). Changing such a
   * column's type moves its path, so the field path based merge-join in {@link #computeDiffs} sees
   * an unrelated removal and addition instead of a modification.
   *
   * <p>Fields whose path is unchanged are excluded first, since the merge-join already aligns
   * those. What remains is grouped by type-stripped path: a group is the same named field on both
   * sides, and it is a type change only when the two sides declare different native types. A group
   * whose types match is a field that merely moved - a connector switching between plain column
   * names and v2 field paths moves every path at once without changing the schema - and reporting
   * that as a removal plus an addition would be wrong.
   *
   * <p>A group usually holds a single field per side. Union types are the reason it can hold more:
   * a union is stored as one field per member, all sharing the member's name, so several fields
   * collapse onto the same type-stripped path. When more than one member changes at once there is
   * no fact about which member became which - neither the field paths nor the source schema can say
   * - so the whole group is reported as one modification naming every type involved rather than as
   * a guessed pairing. The collision does not have to involve the union member itself: a union of
   * structs collides on the struct's children as well, for example {@code
   * [type=union].[type=struct0].u.[type=int].a} and {@code
   * [type=union].[type=struct1].u.[type=string].a} both reduce to {@code u.a}.
   *
   * @return a map from each participating field, on either side, to the group it belongs to
   */
  private static Map<SchemaField, MovedFieldGroup> findMovedFieldGroups(
      SchemaFieldArray baseFields, SchemaFieldArray targetFields) {
    Set<String> basePaths =
        baseFields.stream().map(SchemaField::getFieldPath).collect(Collectors.toSet());
    Set<String> targetPaths =
        targetFields.stream().map(SchemaField::getFieldPath).collect(Collectors.toSet());

    // Fields whose path exists on both sides are already aligned by the merge-join.
    Map<String, List<SchemaField>> baseByLogicalPath =
        baseFields.stream()
            .filter(field -> !targetPaths.contains(field.getFieldPath()))
            .collect(Collectors.groupingBy(SchemaFieldUtils::downgradeFieldPath));
    Map<String, List<SchemaField>> targetByLogicalPath =
        targetFields.stream()
            .filter(field -> !basePaths.contains(field.getFieldPath()))
            .collect(Collectors.groupingBy(SchemaFieldUtils::downgradeFieldPath));

    // Logical paths that also have a field whose path did not move. A union whose members change
    // keeps its union node in place, for instance, and the merge-join diffs that node's
    // documentation, tags and terms already.
    Set<String> logicalPathsWithStableField =
        baseFields.stream()
            .filter(field -> targetPaths.contains(field.getFieldPath()))
            .map(SchemaFieldUtils::downgradeFieldPath)
            .collect(Collectors.toSet());

    Map<SchemaField, MovedFieldGroup> movedFieldGroups = new HashMap<>();
    baseByLogicalPath.forEach(
        (logicalPath, candidateBaseFields) -> {
          List<SchemaField> candidateTargetFields = targetByLogicalPath.get(logicalPath);
          if (candidateTargetFields == null) {
            return;
          }
          MovedFieldGroup group =
              new MovedFieldGroup(
                  candidateBaseFields,
                  candidateTargetFields,
                  logicalPathsWithStableField.contains(logicalPath));
          candidateBaseFields.forEach(field -> movedFieldGroups.put(field, group));
          candidateTargetFields.forEach(field -> movedFieldGroups.put(field, group));
        });
    return movedFieldGroups;
  }

  /** The fields on either side of a field path move that share one type-stripped field path. */
  private static final class MovedFieldGroup {
    // The least qualified path in a group denotes the column itself rather than one of its type
    // branches: a union node is [type=union].u while its members add a type annotation on top. The
    // path breaks ties so that the representative, and therefore the URN reported for the change,
    // does not depend on the order the fields happened to arrive in.
    private static final Comparator<SchemaField> COLUMN_FIRST =
        Comparator.comparingInt(
                (SchemaField field) -> StringUtils.countMatches(field.getFieldPath(), '.'))
            .thenComparing(SchemaField::getFieldPath, Comparator.naturalOrder());

    private final List<SchemaField> baseFields;
    private final List<SchemaField> targetFields;
    private final boolean hasStableField;

    private MovedFieldGroup(
        List<SchemaField> baseFields, List<SchemaField> targetFields, boolean hasStableField) {
      this.baseFields = sortByPath(baseFields);
      this.targetFields = sortByPath(targetFields);
      this.hasStableField = hasStableField;
    }

    private static List<SchemaField> sortByPath(List<SchemaField> fields) {
      return fields.stream().sorted(COLUMN_FIRST).collect(Collectors.toList());
    }

    private static String describeNativeTypes(List<SchemaField> fields) {
      return fields.stream()
          .map(SchemaField::getNativeDataType)
          .distinct()
          .sorted()
          .collect(Collectors.joining(", "));
    }

    SchemaField getRepresentativeBaseField() {
      return baseFields.get(0);
    }

    SchemaField getRepresentativeTargetField() {
      return targetFields.get(0);
    }

    String getBaseNativeTypes() {
      return describeNativeTypes(baseFields);
    }

    String getTargetNativeTypes() {
      return describeNativeTypes(targetFields);
    }

    /**
     * Whether a field sharing this logical path kept its path, in which case the merge-join already
     * diffs that field's documentation, tags and terms and this group must not do so again.
     */
    boolean hasStableField() {
      return hasStableField;
    }

    /** Whether the path moved because the declared types changed, rather than moving on its own. */
    boolean isTypeChange() {
      return !getBaseNativeTypes().equals(getTargetNativeTypes());
    }

    void markProcessed(Set<SchemaField> processedRetypes) {
      processedRetypes.addAll(baseFields);
      processedRetypes.addAll(targetFields);
    }
  }

  private static void processMovedFields(
      Set<ChangeCategory> changeCategories,
      List<ChangeEvent> changeEvents,
      Urn datasetUrn,
      MovedFieldGroup movedGroup,
      AuditStamp auditStamp) {
    SchemaField baseField = movedGroup.getRepresentativeBaseField();
    SchemaField targetField = movedGroup.getRepresentativeTargetField();
    if (movedGroup.isTypeChange()) {
      processNativeTypeChange(
          changeCategories,
          changeEvents,
          datasetUrn,
          baseField,
          targetField,
          movedGroup.getBaseNativeTypes(),
          movedGroup.getTargetNativeTypes(),
          auditStamp);
    }
    // Documentation, tags and terms describe the column, so they are diffed between the fields
    // denoting it on each side - unless a field sharing this path stayed put, in which case the
    // merge-join has already diffed them and doing it here would report the change twice.
    if (!movedGroup.hasStableField()) {
      changeEvents.addAll(
          getFieldPropertyChangeEvents(
              baseField, targetField, datasetUrn, changeCategories, auditStamp));
    }
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
              .modifier(generateSchemaFieldUrn(datasetUrn, baseField).toString())
              .entityUrn(datasetUrn.toString())
              .category(ChangeCategory.TECHNICAL_SCHEMA)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MAJOR)
              .description(
                  BACKWARDS_INCOMPATIBLE_DESC
                      + " removal of field: '"
                      + downgradeFieldPath(baseField)
                      + "'.")
              .fieldPath(baseField.getFieldPath())
              .fieldUrn(generateSchemaFieldUrn(datasetUrn, baseField))
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
              .modifier(generateSchemaFieldUrn(datasetUrn, targetField).toString())
              .entityUrn(datasetUrn.toString())
              .category(ChangeCategory.TECHNICAL_SCHEMA)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  BACK_AND_FORWARD_COMPATIBLE_DESC
                      + "the newly added field '"
                      + downgradeFieldPath(targetField)
                      + "'.")
              .fieldPath(targetField.getFieldPath())
              .fieldUrn(generateSchemaFieldUrn(datasetUrn, targetField))
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
    processNativeTypeChange(
        changeCategories,
        changeEvents,
        datasetUrn,
        curBaseField,
        curTargetField,
        curBaseField.getNativeDataType(),
        curTargetField.getNativeDataType(),
        auditStamp);
  }

  private static void processNativeTypeChange(
      Set<ChangeCategory> changeCategories,
      List<ChangeEvent> changeEvents,
      Urn datasetUrn,
      SchemaField curBaseField,
      SchemaField curTargetField,
      String baseNativeType,
      String targetNativeType,
      AuditStamp auditStamp) {
    // Non-backward compatible change + Major version bump
    if (changeCategories != null && changeCategories.contains(ChangeCategory.TECHNICAL_SCHEMA)) {
      changeEvents.add(
          DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
              .category(ChangeCategory.TECHNICAL_SCHEMA)
              .modifier(generateSchemaFieldUrn(datasetUrn, curBaseField).toString())
              .entityUrn(datasetUrn.toString())
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MAJOR)
              .description(
                  String.format(
                      "%s native datatype of the field '%s' changed from '%s' to '%s'.",
                      BACKWARDS_INCOMPATIBLE_DESC,
                      downgradeFieldPath(curTargetField),
                      baseNativeType,
                      targetNativeType))
              .fieldPath(curBaseField.getFieldPath())
              .fieldUrn(generateSchemaFieldUrn(datasetUrn, curBaseField))
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
        .modifier(generateSchemaFieldUrn(datasetUrn, curBaseField).toString())
        .entityUrn(datasetUrn.toString())
        .operation(ChangeOperation.MODIFY)
        .semVerChange(SemanticChangeType.MINOR)
        .description(
            BACK_AND_FORWARD_COMPATIBLE_DESC
                + "renaming of the field '"
                + downgradeFieldPath(curBaseField)
                + " to "
                + downgradeFieldPath(curTargetField)
                + "'.")
        .fieldPath(curBaseField.getFieldPath())
        .fieldUrn(generateSchemaFieldUrn(datasetUrn, curBaseField))
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
        Urn schemaFieldUrn = generateSchemaFieldUrn(datasetUrn, removedBaseKeyField);
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
        Urn schemaFieldUrn = generateSchemaFieldUrn(datasetUrn, addedTargetKeyField);
        primaryKeyChangeEvents.add(
            DatasetSchemaFieldChangeEvent.schemaFieldChangeEventBuilder()
                .category(ChangeCategory.TECHNICAL_SCHEMA)
                .modifier(generateSchemaFieldUrn(datasetUrn, addedTargetKeyField).toString())
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
    if (!currentValue.getAspect().equals(SCHEMA_METADATA_ASPECT_NAME)
        || (previousValue != null
            && !previousValue.getAspect().equals(SCHEMA_METADATA_ASPECT_NAME))) {
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
