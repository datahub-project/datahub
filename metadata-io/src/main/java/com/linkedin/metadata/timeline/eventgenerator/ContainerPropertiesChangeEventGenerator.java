package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.timeline.eventgenerator.EditableDatasetPropertiesChangeEventGenerator.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.container.ContainerProperties;
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
import javax.annotation.Nullable;

public class ContainerPropertiesChangeEventGenerator
    extends EntityChangeEventGenerator<ContainerProperties> {

  private static List<ChangeEvent> computeDiffs(
      ContainerProperties baseContainerProperties,
      @Nonnull ContainerProperties targetContainerProperties,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();
    String baseDescription =
        (baseContainerProperties != null) ? baseContainerProperties.getDescription() : null;
    String targetDescription =
        (targetContainerProperties != null) ? targetContainerProperties.getDescription() : null;

    if (baseDescription == null && targetDescription != null) {
      // Description added
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_ADDED, entityUrn, targetDescription))
              .parameters(ImmutableMap.of("description", targetDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null && targetDescription == null) {
      // Description removed.
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_REMOVED, entityUrn, baseDescription))
              .parameters(ImmutableMap.of("description", baseDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null
        && targetDescription != null
        && !baseDescription.equals(targetDescription)) {
      // Description has been modified.
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(DESCRIPTION_CHANGED, entityUrn, baseDescription, targetDescription))
              .parameters(ImmutableMap.of("description", targetDescription))
              .auditStamp(auditStamp)
              .build());
    }
    return changeEvents;
  }

  @Nullable
  private static ContainerProperties getContainerPropertiesFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(ContainerProperties.class, entityAspect.getMetadata());
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
    if (!previousValue.getAspect().equals(CONTAINER_PROPERTIES_ASPECT_NAME)
        || !currentValue.getAspect().equals(CONTAINER_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + CONTAINER_PROPERTIES_ASPECT_NAME);
    }
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      ContainerProperties baseContainerProperties = getContainerPropertiesFromAspect(previousValue);
      ContainerProperties targetContainerProperties =
          getContainerPropertiesFromAspect(currentValue);
      changeEvents.addAll(
          computeDiffs(
              baseContainerProperties, targetContainerProperties, currentValue.getUrn(), null));
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
      @Nonnull Aspect<ContainerProperties> from,
      @Nonnull Aspect<ContainerProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
