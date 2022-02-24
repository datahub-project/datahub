package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.linkedin.metadata.Constants.*;


public class EditableDatasetPropertiesDiffer implements Differ {
  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME) || !currentValue.getAspect()
        .equals(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + EDITABLE_DATASET_PROPERTIES_ASPECT_NAME);
    }
    assert (currentValue != null);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      EditableDatasetProperties baseDatasetProperties = getEditableDatasetPropertiesFromAspect(previousValue);
      EditableDatasetProperties targetDatasetProperties = getEditableDatasetPropertiesFromAspect(currentValue);
      changeEvents.addAll(computeDiffs(baseDatasetProperties, targetDatasetProperties, currentValue.getUrn()));
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

  private List<ChangeEvent> computeDiffs(EditableDatasetProperties baseDatasetProperties,
      EditableDatasetProperties targetDatasetProperties, String entityUrn) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    String baseDescription = baseDatasetProperties.getDescription();
    String targetDescription = targetDatasetProperties.getDescription();
    if (baseDescription == null && targetDescription != null) {
      // Description added
      changeEvents.add(ChangeEvent.builder()
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format("The editable description '%s' has been added for the dataset '%s'.", targetDescription,
                  entityUrn))
          .build());
    } else if (baseDescription != null && targetDescription == null) {
      // Description removed.
      changeEvents.add(ChangeEvent.builder()
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format("The editable description '%s' has been removed for the dataset '%s'.", baseDescription,
                  entityUrn))
          .build());
    } else if (!baseDescription.equals(targetDescription)) {
      // Description has been modified.
      changeEvents.add(ChangeEvent.builder()
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format("The editable description of the dataset '%s' has been changed from '%s' to '%s'.",
              entityUrn, baseDescription, targetDescription))
          .build());
    }
    return changeEvents;
  }

  private EditableDatasetProperties getEditableDatasetPropertiesFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(EditableDatasetProperties.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }
}
