package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.StructuredPropertyAssignmentChangeEvent;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;

public class StructuredPropertyChangeEventGenerator
    extends EntityChangeEventGenerator<StructuredProperties> {
  private static final String STRUCTURED_PROPERTY_ADDED_FORMAT =
      "Structured property '%s' added to entity '%s'.";
  private static final String STRUCTURED_PROPERTY_REMOVED_FORMAT =
      "Structured property '%s' removed from entity '%s'.";
  private static final String STRUCTURED_PROPERTY_CHANGED_FORMAT =
      "Structured property '%s' changed on entity '%s'.";

  private static final Comparator<Urn> URN_COMPARATOR = Comparator.comparing(Urn::toString);

  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<StructuredProperties> from,
      @Nonnull Aspect<StructuredProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
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

    StructuredProperties baseStructuredProperties =
        getStructuredPropertiesFromAspect(previousValue);
    StructuredProperties targetStructuredProperties =
        getStructuredPropertiesFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.STRUCTURED_PROPERTY) {
      changeEvents.addAll(
          computeDiffs(
              baseStructuredProperties, targetStructuredProperties, currentValue.getUrn(), null));
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

  private static List<ChangeEvent> computeDiffs(
      final StructuredProperties previousAspect,
      final StructuredProperties newAspect,
      final String entityUrn,
      final AuditStamp auditStamp) {

    Map<Urn, StructuredPropertyValueAssignment> baseAssignments =
        buildAssignmentMap(previousAspect);
    Map<Urn, StructuredPropertyValueAssignment> targetAssignments = buildAssignmentMap(newAspect);

    // Sorted for deterministic event ordering.
    Set<Urn> allPropertyUrns = new TreeSet<>(URN_COMPARATOR);
    allPropertyUrns.addAll(baseAssignments.keySet());
    allPropertyUrns.addAll(targetAssignments.keySet());

    List<ChangeEvent> changeEvents = new ArrayList<>();
    for (Urn propertyUrn : allPropertyUrns) {
      StructuredPropertyValueAssignment baseAssignment = baseAssignments.get(propertyUrn);
      StructuredPropertyValueAssignment targetAssignment = targetAssignments.get(propertyUrn);

      if (baseAssignment == null) {
        changeEvents.add(
            buildChangeEvent(
                ChangeOperation.ADD,
                STRUCTURED_PROPERTY_ADDED_FORMAT,
                targetAssignment,
                entityUrn,
                auditStamp));
      } else if (targetAssignment == null) {
        changeEvents.add(
            buildChangeEvent(
                ChangeOperation.REMOVE,
                STRUCTURED_PROPERTY_REMOVED_FORMAT,
                baseAssignment,
                entityUrn,
                auditStamp));
      } else if (!baseAssignment.getValues().equals(targetAssignment.getValues())) {
        changeEvents.add(
            buildChangeEvent(
                ChangeOperation.MODIFY,
                STRUCTURED_PROPERTY_CHANGED_FORMAT,
                targetAssignment,
                entityUrn,
                auditStamp));
      }
    }

    return changeEvents;
  }

  private static Map<Urn, StructuredPropertyValueAssignment> buildAssignmentMap(
      StructuredProperties structuredProperties) {
    Map<Urn, StructuredPropertyValueAssignment> map = new TreeMap<>(URN_COMPARATOR);
    if (structuredProperties != null) {
      structuredProperties
          .getProperties()
          .forEach(assignment -> map.put(assignment.getPropertyUrn(), assignment));
    }
    return map;
  }

  private static ChangeEvent buildChangeEvent(
      final ChangeOperation operation,
      final String descriptionFormat,
      final StructuredPropertyValueAssignment assignment,
      final String entityUrn,
      final AuditStamp auditStamp) {
    return StructuredPropertyAssignmentChangeEvent
        .entityStructuredPropertyAssignmentChangeEventBuilder()
        .modifier(assignment.getPropertyUrn().toString())
        .entityUrn(entityUrn)
        .category(ChangeCategory.STRUCTURED_PROPERTY)
        .operation(operation)
        .semVerChange(SemanticChangeType.MINOR)
        .description(
            String.format(descriptionFormat, assignment.getPropertyUrn().getId(), entityUrn))
        .auditStamp(auditStamp)
        .structuredPropertyValueAssignment(assignment)
        .build();
  }
}
