package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;

public class EditableDatasetPropertiesChangeEventGenerator
    extends EntityChangeEventGenerator<EditableDatasetProperties> {

  public static final String DESCRIPTION_ADDED = "Documentation for '%s' has been added: '%s'.";
  public static final String DESCRIPTION_REMOVED = "Documentation for '%s' has been removed: '%s'.";
  public static final String DESCRIPTION_CHANGED =
      "Documentation of '%s' has been changed from '%s' to '%s'.";

  private static List<ChangeEvent> computeDiffs(
      EditableDatasetProperties baseDatasetProperties,
      EditableDatasetProperties targetDatasetProperties,
      String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    ChangeEvent descriptionChangeEvent =
        getDescriptionChangeEvent(
            baseDatasetProperties, targetDatasetProperties, entityUrn, auditStamp);
    if (descriptionChangeEvent != null) {
      changeEvents.add(descriptionChangeEvent);
    }
    return changeEvents;
  }

  private static ChangeEvent getDescriptionChangeEvent(
      EditableDatasetProperties baseDatasetProperties,
      EditableDatasetProperties targetDatasetProperties,
      String entityUrn,
      AuditStamp auditStamp) {
    String baseDescription =
        (baseDatasetProperties != null) ? baseDatasetProperties.getDescription() : null;
    String targetDescription =
        (targetDatasetProperties != null) ? targetDatasetProperties.getDescription() : null;
    if (baseDescription == null && targetDescription != null) {
      // Description added
      return ChangeEvent.builder()
          .entityUrn(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .operation(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(DESCRIPTION_ADDED, entityUrn, targetDescription))
          .parameters(ImmutableMap.of("description", targetDescription))
          .auditStamp(auditStamp)
          .build();
    } else if (baseDescription != null && targetDescription == null) {
      // Description removed.
      return ChangeEvent.builder()
          .entityUrn(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .operation(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(DESCRIPTION_REMOVED, entityUrn, baseDescription))
          .parameters(ImmutableMap.of("description", baseDescription))
          .auditStamp(auditStamp)
          .build();
    } else if (baseDescription != null
        && targetDescription != null
        && !baseDescription.equals(targetDescription)) {
      // Description has been modified.
      return ChangeEvent.builder()
          .entityUrn(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .operation(ChangeOperation.MODIFY)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format(DESCRIPTION_CHANGED, entityUrn, baseDescription, targetDescription))
          .parameters(ImmutableMap.of("description", targetDescription))
          .auditStamp(auditStamp)
          .build();
    }
    return null;
  }

  private static EditableDatasetProperties getEditableDatasetPropertiesFromAspect(
      EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(
          EditableDatasetProperties.class, entityAspect.getMetadata());
    }
    return null;
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {

    if (currentValue == null) {
      throw new IllegalArgumentException("EntityAspect currentValue should not be null");
    }

    if (!previousValue.getAspect().equals(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME)
        || !currentValue.getAspect().equals(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException(
          "Aspect is not " + EDITABLE_DATASET_PROPERTIES_ASPECT_NAME);
    }

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      EditableDatasetProperties baseDatasetProperties =
          getEditableDatasetPropertiesFromAspect(previousValue);
      EditableDatasetProperties targetDatasetProperties =
          getEditableDatasetPropertiesFromAspect(currentValue);
      changeEvents.addAll(
          computeDiffs(
              baseDatasetProperties, targetDatasetProperties, currentValue.getUrn(), null));
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

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<EditableDatasetProperties> from,
      @Nonnull Aspect<EditableDatasetProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
