package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.metadata.Constants.*;


public class EditableSchemaMetadataDiffer implements Differ {
  public static final String FIELD_DOCUMENTATION_ADDED_FORMAT =
      "Documentation for the field '%s' of '%s' has been added: '%s'";
  public static final String FIELD_DOCUMENTATION_REMOVED_FORMAT =
      "Documentation for the field '%s' of '%s' has been removed: '%s'";
  public static final String FIELD_DOCUMENTATION_UPDATED_FORMAT =
      "Documentation for the field '%s' of '%s' has been updated from '%s' to '%s'.";
  private static final Set<ChangeCategory> SUPPORTED_CATEGORIES =
      Stream.of(ChangeCategory.DOCUMENTATION, ChangeCategory.TAG, ChangeCategory.GLOSSARY_TERM)
          .collect(Collectors.toSet());

  private static void sortEditableSchemaMetadataByFieldPath(EditableSchemaMetadata editableSchemaMetadata) {
    if (editableSchemaMetadata == null) {
      return;
    }
    List<EditableSchemaFieldInfo> editableSchemaFieldInfos =
        new ArrayList<>(editableSchemaMetadata.getEditableSchemaFieldInfo());
    editableSchemaFieldInfos.sort(Comparator.comparing(EditableSchemaFieldInfo::getFieldPath));
    editableSchemaMetadata.setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray(editableSchemaFieldInfos));
  }

  private static List<ChangeEvent> getAllChangeEvents(EditableSchemaFieldInfo baseFieldInfo,
      EditableSchemaFieldInfo targetFieldInfo, String entityUrn, ChangeCategory changeCategory) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (changeCategory == ChangeCategory.DOCUMENTATION) {
      ChangeEvent documentationChangeEvent = getDocumentationChangeEvent(baseFieldInfo, targetFieldInfo, entityUrn);
      if (documentationChangeEvent != null) {
        changeEvents.add(documentationChangeEvent);
      }
    }
    if (changeCategory == ChangeCategory.TAG) {
      changeEvents.addAll(getTagChangeEvents(baseFieldInfo, targetFieldInfo, entityUrn));
    }
    if (changeCategory == ChangeCategory.GLOSSARY_TERM) {
      changeEvents.addAll(getGlossaryTermChangeEvents(baseFieldInfo, targetFieldInfo, entityUrn));
    }
    return changeEvents;
  }

  private static List<ChangeEvent> computeDiffs(EditableSchemaMetadata baseEditableSchemaMetadata,
      EditableSchemaMetadata targetEditableSchemaMetadata, String entityUrn, ChangeCategory changeCategory) {
    sortEditableSchemaMetadataByFieldPath(baseEditableSchemaMetadata);
    sortEditableSchemaMetadataByFieldPath(targetEditableSchemaMetadata);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    EditableSchemaFieldInfoArray baseFieldInfos =
        (baseEditableSchemaMetadata != null) ? baseEditableSchemaMetadata.getEditableSchemaFieldInfo()
            : new EditableSchemaFieldInfoArray();
    EditableSchemaFieldInfoArray targetFieldInfos = targetEditableSchemaMetadata.getEditableSchemaFieldInfo();
    int baseIdx = 0;
    int targetIdx = 0;
    while (baseIdx < baseFieldInfos.size() && targetIdx < targetFieldInfos.size()) {
      EditableSchemaFieldInfo baseFieldInfo = baseFieldInfos.get(baseIdx);
      EditableSchemaFieldInfo targetFieldInfo = targetFieldInfos.get(targetIdx);
      int comparison = baseFieldInfo.getFieldPath().compareTo(targetFieldInfo.getFieldPath());
      if (comparison == 0) {
        changeEvents.addAll(getAllChangeEvents(baseFieldInfo, targetFieldInfo, entityUrn, changeCategory));
        ++baseIdx;
        ++targetIdx;
      } else if (comparison < 0) {
        // EditableFieldInfo got removed.
        changeEvents.addAll(getAllChangeEvents(baseFieldInfo, null, entityUrn, changeCategory));
        ++baseIdx;
      } else {
        // EditableFieldInfo got added.
        changeEvents.addAll(getAllChangeEvents(null, targetFieldInfo, entityUrn, changeCategory));
        ++targetIdx;
      }
    }

    while (baseIdx < baseFieldInfos.size()) {
      // Handle removed baseFieldInfo
      EditableSchemaFieldInfo baseFieldInfo = baseFieldInfos.get(baseIdx);
      changeEvents.addAll(getAllChangeEvents(baseFieldInfo, null, entityUrn, changeCategory));
      ++baseIdx;
    }
    while (targetIdx < targetFieldInfos.size()) {
      // Handle newly added targetFieldInfo
      EditableSchemaFieldInfo targetFieldInfo = targetFieldInfos.get(targetIdx);
      changeEvents.addAll(getAllChangeEvents(null, targetFieldInfo, entityUrn, changeCategory));
      ++targetIdx;
    }
    return changeEvents;
  }

  private static EditableSchemaMetadata getEditableSchemaMetadataFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(EditableSchemaMetadata.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }

  private static ChangeEvent getDocumentationChangeEvent(EditableSchemaFieldInfo baseFieldInfo,
      EditableSchemaFieldInfo targetFieldInfo, String entityUrn) {
    String baseFieldDescription = (baseFieldInfo != null) ? baseFieldInfo.getDescription() : null;
    String targetFieldDescription = (targetFieldInfo != null) ? targetFieldInfo.getDescription() : null;

    if (baseFieldDescription == null && targetFieldDescription != null) {
      return ChangeEvent.builder()
          .elementId(targetFieldInfo.getFieldPath())
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(FIELD_DOCUMENTATION_ADDED_FORMAT, targetFieldInfo.getFieldPath(), entityUrn,
              targetFieldDescription))
          .build();
    }

    if (baseFieldDescription != null && targetFieldDescription == null) {
      return ChangeEvent.builder()
          .elementId(baseFieldInfo.getFieldPath())
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(FIELD_DOCUMENTATION_REMOVED_FORMAT, targetFieldInfo.getFieldPath(), entityUrn,
              baseFieldDescription))
          .build();
    }

    if (baseFieldDescription != null && targetFieldDescription != null && !baseFieldDescription.equals(
        targetFieldDescription)) {
      return ChangeEvent.builder()
          .elementId(targetFieldInfo.getFieldPath())
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.PATCH)
          .description(String.format(FIELD_DOCUMENTATION_UPDATED_FORMAT, targetFieldInfo.getFieldPath(), entityUrn,
              baseFieldDescription, targetFieldDescription))
          .build();
    }

    return null;
  }

  private static List<ChangeEvent> getGlossaryTermChangeEvents(EditableSchemaFieldInfo baseFieldInfo,
      EditableSchemaFieldInfo targetFieldInfo, String entityUrn) {
    GlossaryTerms baseGlossaryTerms = (baseFieldInfo != null) ? baseFieldInfo.getGlossaryTerms() : null;
    GlossaryTerms targetGlossaryTerms = (targetFieldInfo != null) ? targetFieldInfo.getGlossaryTerms() : null;
    return GlossaryTermsDiffer.computeDiffs(baseGlossaryTerms, targetGlossaryTerms, entityUrn);
  }

  private static List<ChangeEvent> getTagChangeEvents(EditableSchemaFieldInfo baseFieldInfo,
      EditableSchemaFieldInfo targetFieldInfo, String entityUrn) {
    GlobalTags baseGlobalTags = (baseFieldInfo != null) ? baseFieldInfo.getGlobalTags() : null;
    GlobalTags targetGlobalTags = (targetFieldInfo != null) ? targetFieldInfo.getGlobalTags() : null;
    return GlobalTagsDiffer.computeDiffs(baseGlobalTags, targetGlobalTags, entityUrn);
  }

  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(EDITABLE_SCHEMA_METADATA_ASPECT_NAME) || !currentValue.getAspect()
        .equals(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + EDITABLE_SCHEMA_METADATA_ASPECT_NAME);
    }
    assert (currentValue != null);
    EditableSchemaMetadata baseEditableSchemaMetadata = getEditableSchemaMetadataFromAspect(previousValue);
    EditableSchemaMetadata targetEditableSchemaMetadata = getEditableSchemaMetadataFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (SUPPORTED_CATEGORIES.contains(element)) {
      changeEvents.addAll(
          computeDiffs(baseEditableSchemaMetadata, targetEditableSchemaMetadata, currentValue.getUrn(), element));
    }

    // Assess the highest change at the transaction(schema) level.
    SemanticChangeType highestSemanticChange = SemanticChangeType.NONE;
    ChangeEvent highestChangeEvent =
        changeEvents.stream().max(Comparator.comparing(ChangeEvent::getSemVerChange)).orElse(null);
    if (highestChangeEvent != null) {
      highestSemanticChange = highestChangeEvent.getSemVerChange();
    }

    return ChangeTransaction.builder()
        .semVerChange(highestSemanticChange)
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffsRequested ? rawDiff : null)
        .actor(currentValue.getCreatedBy())
        .build();
  }
}
