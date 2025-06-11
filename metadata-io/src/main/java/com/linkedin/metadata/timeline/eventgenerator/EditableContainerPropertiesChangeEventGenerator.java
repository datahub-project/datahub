package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.EditableContainerProperties;
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

public class EditableContainerPropertiesChangeEventGenerator
    extends EntityChangeEventGenerator<EditableContainerProperties> {

  public static final String DESCRIPTION_ADDED = "Documentation for '%s' has been added: '%s'.";
  public static final String DESCRIPTION_REMOVED = "Documentation for '%s' has been removed: '%s'.";
  public static final String DESCRIPTION_CHANGED =
      "Documentation of '%s' has been changed from '%s' to '%s'.";

  private static List<ChangeEvent> computeDiffs(
      EditableContainerProperties baseContainerProperties,
      EditableContainerProperties targetContainerProperties,
      String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    ChangeEvent descriptionChangeEvent =
        getDescriptionChangeEvent(
            baseContainerProperties, targetContainerProperties, entityUrn, auditStamp);
    if (descriptionChangeEvent != null) {
      changeEvents.add(descriptionChangeEvent);
    }
    return changeEvents;
  }

  private static ChangeEvent getDescriptionChangeEvent(
      EditableContainerProperties baseContainerProperties,
      EditableContainerProperties targetContainerProperties,
      String entityUrn,
      AuditStamp auditStamp) {
    String baseDescription =
        (baseContainerProperties != null) ? baseContainerProperties.getDescription() : null;
    String targetDescription =
        (targetContainerProperties != null) ? targetContainerProperties.getDescription() : null;
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

  private static EditableContainerProperties getEditableContainerPropertiesFromAspect(
      EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(
          EditableContainerProperties.class, entityAspect.getMetadata());
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

    if (!previousValue.getAspect().equals(CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME)
        || !currentValue.getAspect().equals(CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException(
          "Aspect is not " + CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME);
    }

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      EditableContainerProperties baseDatasetProperties =
          getEditableContainerPropertiesFromAspect(previousValue);
      EditableContainerProperties targetDatasetProperties =
          getEditableContainerPropertiesFromAspect(currentValue);
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
      @Nonnull Aspect<EditableContainerProperties> from,
      @Nonnull Aspect<EditableContainerProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
