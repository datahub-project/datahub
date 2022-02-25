package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.timeline.differ.EditableDatasetPropertiesDiffer.*;


public class DatasetPropertiesDiffer implements Differ {
  private static List<ChangeEvent> computeDiffs(DatasetProperties baseDatasetProperties,
      @Nonnull DatasetProperties targetDatasetProperties, @Nonnull String entityUrn) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    String baseDescription = (baseDatasetProperties != null) ? baseDatasetProperties.getDescription() : null;
    String targetDescription = (targetDatasetProperties != null) ? targetDatasetProperties.getDescription() : null;

    if (baseDescription == null && targetDescription != null) {
      // Description added
      changeEvents.add(ChangeEvent.builder()
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(DESCRIPTION_ADDED, targetDescription, entityUrn))
          .build());
    } else if (baseDescription != null && targetDescription == null) {
      // Description removed.
      changeEvents.add(ChangeEvent.builder()
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(DESCRIPTION_REMOVED, baseDescription, entityUrn))
          .build());
    } else if (baseDescription != null && targetDescription != null && !baseDescription.equals(targetDescription)) {
      // Description has been modified.
      changeEvents.add(ChangeEvent.builder()
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(DESCRIPTION_CHANGED, entityUrn, baseDescription, targetDescription))
          .build());
    }
    return changeEvents;
  }

  @Nullable
  private static DatasetProperties getDatasetPropertiesFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(DatasetProperties.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }

  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(DATASET_PROPERTIES_ASPECT_NAME) || !currentValue.getAspect()
        .equals(DATASET_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + DATASET_PROPERTIES_ASPECT_NAME);
    }
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      DatasetProperties baseDatasetProperties = getDatasetPropertiesFromAspect(previousValue);
      DatasetProperties targetDatasetProperties = getDatasetPropertiesFromAspect(currentValue);
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
}
