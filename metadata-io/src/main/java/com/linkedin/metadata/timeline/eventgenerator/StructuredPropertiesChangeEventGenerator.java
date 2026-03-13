package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.StructuredPropertyChangeEvent;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

public class StructuredPropertiesChangeEventGenerator
    extends EntityChangeEventGenerator<StructuredProperties> {

  private static final String PROPERTY_VALUE_ADDED_FORMAT =
      "Structured property '%s' value added on entity '%s'.";
  private static final String PROPERTY_VALUE_REMOVED_FORMAT =
      "Structured property '%s' value removed from entity '%s'.";

  public static List<ChangeEvent> computeDiffs(
      StructuredProperties baseProps,
      StructuredProperties targetProps,
      String entityUrn,
      AuditStamp auditStamp) {
    Map<String, StructuredPropertyValueAssignment> baseMap =
        toMap(
            baseProps != null
                ? baseProps.getProperties()
                : new StructuredPropertyValueAssignmentArray());
    Map<String, StructuredPropertyValueAssignment> targetMap =
        toMap(
            targetProps != null
                ? targetProps.getProperties()
                : new StructuredPropertyValueAssignmentArray());

    List<ChangeEvent> changeEvents = new ArrayList<>();

    // Properties present in target (added or value-changed)
    for (Map.Entry<String, StructuredPropertyValueAssignment> entry : targetMap.entrySet()) {
      StructuredPropertyValueAssignment target = entry.getValue();
      StructuredPropertyValueAssignment base = baseMap.get(entry.getKey());

      Set<PrimitivePropertyValue> baseValues =
          base != null ? new HashSet<>(base.getValues()) : new HashSet<>();
      Set<PrimitivePropertyValue> targetValues = new HashSet<>(target.getValues());

      // Emit ADD for each value that is new
      for (PrimitivePropertyValue value : targetValues) {
        if (!baseValues.contains(value)) {
          changeEvents.add(
              buildEvent(
                  target.getPropertyUrn(),
                  entityUrn,
                  ChangeOperation.ADD,
                  PROPERTY_VALUE_ADDED_FORMAT,
                  auditStamp));
          break; // one event per property per operation direction is sufficient
        }
      }

      // Emit REMOVE for each value that was dropped
      for (PrimitivePropertyValue value : baseValues) {
        if (!targetValues.contains(value)) {
          changeEvents.add(
              buildEvent(
                  target.getPropertyUrn(),
                  entityUrn,
                  ChangeOperation.REMOVE,
                  PROPERTY_VALUE_REMOVED_FORMAT,
                  auditStamp));
          break;
        }
      }
    }

    // Properties fully removed
    for (Map.Entry<String, StructuredPropertyValueAssignment> entry : baseMap.entrySet()) {
      if (!targetMap.containsKey(entry.getKey())) {
        changeEvents.add(
            buildEvent(
                entry.getValue().getPropertyUrn(),
                entityUrn,
                ChangeOperation.REMOVE,
                PROPERTY_VALUE_REMOVED_FORMAT,
                auditStamp));
      }
    }

    return changeEvents;
  }

  private static Map<String, StructuredPropertyValueAssignment> toMap(
      StructuredPropertyValueAssignmentArray assignments) {
    Map<String, StructuredPropertyValueAssignment> map = new HashMap<>();
    for (StructuredPropertyValueAssignment a : assignments) {
      map.put(a.getPropertyUrn().toString(), a);
    }
    return map;
  }

  private static ChangeEvent buildEvent(
      Urn propertyUrn,
      String entityUrn,
      ChangeOperation operation,
      String format,
      AuditStamp auditStamp) {
    return StructuredPropertyChangeEvent.entityStructuredPropertyChangeEventBuilder()
        .modifier(propertyUrn.toString())
        .entityUrn(entityUrn)
        .category(ChangeCategory.STRUCTURED_PROPERTY)
        .operation(operation)
        .semVerChange(SemanticChangeType.MINOR)
        .description(String.format(format, propertyUrn.getId(), entityUrn))
        .propertyUrn(propertyUrn)
        .auditStamp(auditStamp)
        .build();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<StructuredProperties> from,
      @Nonnull Aspect<StructuredProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}