package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.StructuredPropertyAssignmentChangeEvent;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;

public class StructuredPropertyChangeEventGenerator
    extends EntityChangeEventGenerator<StructuredProperties> {
  private static final String STRUCTURED_PROPERTY_ADDED_FORMAT =
      "Structured property '%s' added to entity '%s'.";
  private static final String STRUCTURED_PROPERTY_REMOVED_FORMAT =
      "Structured property '%s' removed from entity '%s'.";

  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<StructuredProperties> from,
      @Nonnull Aspect<StructuredProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private static void sortStructuredPropertiesByStructuredPropertyUrn(
      StructuredProperties structuredProperties) {
    if (structuredProperties == null) {
      return;
    }
    List<StructuredPropertyValueAssignment> structuredPropertyValueAssignments =
        new ArrayList<>(structuredProperties.getProperties());
    structuredPropertyValueAssignments.sort(
        Comparator.comparing(
            StructuredPropertyValueAssignment::getPropertyUrn,
            Comparator.comparing(Urn::toString)));
    structuredProperties.setProperties(
        new StructuredPropertyValueAssignmentArray(structuredPropertyValueAssignments));
  }

  private static StructuredProperties getStructuredPropertiesFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(StructuredProperties.class, entityAspect.getMetadata());
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
    if (!previousValue.getAspect().equals(STRUCTURED_PROPERTIES_ASPECT_NAME)
        || !currentValue.getAspect().equals(STRUCTURED_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + STRUCTURED_PROPERTIES_ASPECT_NAME);
    }

    StructuredProperties baseGlobalTags = getStructuredPropertiesFromAspect(previousValue);
    StructuredProperties targetGlobalTags = getStructuredPropertiesFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.STRUCTURED_PROPERTY) {
      changeEvents.addAll(
          computeDiffs(baseGlobalTags, targetGlobalTags, currentValue.getUrn(), null));
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

  private List<ChangeEvent> computeDiffs(
      final StructuredProperties previousAspect,
      final StructuredProperties newAspect,
      final String entityUrn,
      final AuditStamp auditStamp) {

    sortStructuredPropertiesByStructuredPropertyUrn(previousAspect);
    sortStructuredPropertiesByStructuredPropertyUrn(newAspect);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    StructuredPropertyValueAssignmentArray baseAssignments =
        (previousAspect != null)
            ? previousAspect.getProperties()
            : new StructuredPropertyValueAssignmentArray();
    StructuredPropertyValueAssignmentArray targetAssignments =
        (newAspect != null)
            ? newAspect.getProperties()
            : new StructuredPropertyValueAssignmentArray();
    int baseIdx = 0;
    int targetIdx = 0;

    while (baseIdx < baseAssignments.size() && targetIdx < targetAssignments.size()) {
      StructuredPropertyValueAssignment baseAssignment = baseAssignments.get(baseIdx);
      StructuredPropertyValueAssignment targetAssignment = targetAssignments.get(targetIdx);
      int comparison =
          baseAssignment
              .getPropertyUrn()
              .toString()
              .compareTo(targetAssignment.getPropertyUrn().toString());

      if (comparison == 0) {
        // No change to this property.
        ++baseIdx;
        ++targetIdx;
        if (!baseAssignment.getValues().equals(targetAssignment.getValues())) {
          changeEvents.add(
              StructuredPropertyAssignmentChangeEvent
                  .entityStructuredPropertyAssignmentChangeEventBuilder()
                  .modifier(targetAssignment.getPropertyUrn().toString())
                  .entityUrn(entityUrn)
                  .category(ChangeCategory.STRUCTURED_PROPERTY)
                  .operation(ChangeOperation.MODIFY)
                  .semVerChange(SemanticChangeType.MINOR)
                  .description(
                      String.format(
                          STRUCTURED_PROPERTY_ADDED_FORMAT,
                          targetAssignment.getPropertyUrn().getId(),
                          entityUrn))
                  .auditStamp(auditStamp)
                  .structuredPropertyValueAssignment(targetAssignment)
                  .build());
        }
      } else if (comparison < 0) {
        // Property got removed.
        changeEvents.add(
            StructuredPropertyAssignmentChangeEvent.builder()
                .modifier(baseAssignment.getPropertyUrn().toString())
                .entityUrn(entityUrn)
                .category(ChangeCategory.STRUCTURED_PROPERTY)
                .operation(ChangeOperation.REMOVE)
                .semVerChange(SemanticChangeType.MINOR)
                .description(
                    String.format(
                        STRUCTURED_PROPERTY_REMOVED_FORMAT,
                        baseAssignment.getPropertyUrn().getId(),
                        entityUrn))
                .auditStamp(auditStamp)
                .build());
        ++baseIdx;
      } else {
        // Property got added.
        changeEvents.add(
            StructuredPropertyAssignmentChangeEvent
                .entityStructuredPropertyAssignmentChangeEventBuilder()
                .modifier(targetAssignment.getPropertyUrn().toString())
                .entityUrn(entityUrn)
                .category(ChangeCategory.STRUCTURED_PROPERTY)
                .operation(ChangeOperation.ADD)
                .semVerChange(SemanticChangeType.MINOR)
                .description(
                    String.format(
                        STRUCTURED_PROPERTY_ADDED_FORMAT,
                        targetAssignment.getPropertyUrn().getId(),
                        entityUrn))
                .auditStamp(auditStamp)
                .structuredPropertyValueAssignment(targetAssignment)
                .build());
        ++targetIdx;
      }
    }

    // Handle remaining properties in baseAssignments (removed properties)
    while (baseIdx < baseAssignments.size()) {
      StructuredPropertyValueAssignment baseAssignment = baseAssignments.get(baseIdx);
      changeEvents.add(
          StructuredPropertyAssignmentChangeEvent
              .entityStructuredPropertyAssignmentChangeEventBuilder()
              .modifier(baseAssignment.getPropertyUrn().toString())
              .entityUrn(entityUrn)
              .category(ChangeCategory.STRUCTURED_PROPERTY)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      STRUCTURED_PROPERTY_REMOVED_FORMAT,
                      baseAssignment.getPropertyUrn().getId(),
                      entityUrn))
              .auditStamp(auditStamp)
              .structuredPropertyValueAssignment(baseAssignment)
              .build());
      ++baseIdx;
    }

    // Handle remaining properties in targetAssignments (added properties)
    while (targetIdx < targetAssignments.size()) {
      StructuredPropertyValueAssignment targetAssignment = targetAssignments.get(targetIdx);
      changeEvents.add(
          StructuredPropertyAssignmentChangeEvent
              .entityStructuredPropertyAssignmentChangeEventBuilder()
              .modifier(targetAssignment.getPropertyUrn().toString())
              .entityUrn(entityUrn)
              .category(ChangeCategory.STRUCTURED_PROPERTY)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      STRUCTURED_PROPERTY_ADDED_FORMAT,
                      targetAssignment.getPropertyUrn().getId(),
                      entityUrn))
              .auditStamp(auditStamp)
              .structuredPropertyValueAssignment(targetAssignment)
              .build());
      ++targetIdx;
    }

    return changeEvents;
  }
}
