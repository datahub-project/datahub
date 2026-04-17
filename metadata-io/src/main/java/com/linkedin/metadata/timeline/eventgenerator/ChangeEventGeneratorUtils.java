package com.linkedin.metadata.timeline.eventgenerator;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldGlossaryTermChangeEvent;
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldTagChangeEvent;
import com.linkedin.metadata.timeline.data.entity.GlossaryTermChangeEvent;
import com.linkedin.metadata.timeline.data.entity.TagChangeEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ChangeEventGeneratorUtils {

  public static List<ChangeEvent> convertEntityTagChangeEvents(
      @Nonnull String fieldPath,
      @Nonnull Urn parentUrn,
      @Nonnull List<ChangeEvent> entityTagChangeEvents) {
    return entityTagChangeEvents.stream()
        .filter(entityTagChangeEvent -> entityTagChangeEvent instanceof TagChangeEvent)
        .map(entityTagChangeEvent -> (TagChangeEvent) entityTagChangeEvent)
        .map(
            entityTagChangeEvent -> {
              Map<String, Object> parameters =
                  ImmutableMap.of(
                      "fieldPath", fieldPath,
                      "parentUrn", parentUrn.toString(),
                      "tagUrn", entityTagChangeEvent.getParameters().get("tagUrn"),
                      "context", entityTagChangeEvent.getParameters().get("context"));
              return SchemaFieldTagChangeEvent.schemaFieldTagChangeEventBuilder()
                  .modifier(entityTagChangeEvent.getModifier())
                  .entityUrn(entityTagChangeEvent.getEntityUrn())
                  .category(entityTagChangeEvent.getCategory())
                  .operation(entityTagChangeEvent.getOperation())
                  .semVerChange(entityTagChangeEvent.getSemVerChange())
                  .description(entityTagChangeEvent.getDescription())
                  .parameters(parameters)
                  .auditStamp(entityTagChangeEvent.getAuditStamp())
                  .build();
            })
        .collect(Collectors.toList());
  }

  public static List<ChangeEvent> convertEntityGlossaryTermChangeEvents(
      @Nonnull String fieldPath,
      @Nonnull Urn parentUrn,
      @Nonnull List<ChangeEvent> entityGlossaryTermChangeEvents) {
    return entityGlossaryTermChangeEvents.stream()
        .filter(
            entityGlossaryTermChangeEvent ->
                entityGlossaryTermChangeEvent instanceof GlossaryTermChangeEvent)
        .map(
            entityGlossaryTermChangeEvent ->
                (GlossaryTermChangeEvent) entityGlossaryTermChangeEvent)
        .map(
            entityGlossaryTermChangeEvent -> {
              Map<String, Object> parameters =
                  ImmutableMap.of(
                      "fieldPath", fieldPath,
                      "parentUrn", parentUrn.toString(),
                      "termUrn", entityGlossaryTermChangeEvent.getParameters().get("termUrn"),
                      "context", entityGlossaryTermChangeEvent.getParameters().get("context"));

              return SchemaFieldGlossaryTermChangeEvent.schemaFieldGlossaryTermChangeEventBuilder()
                  .modifier(entityGlossaryTermChangeEvent.getModifier())
                  .entityUrn(entityGlossaryTermChangeEvent.getEntityUrn())
                  .category(entityGlossaryTermChangeEvent.getCategory())
                  .operation(entityGlossaryTermChangeEvent.getOperation())
                  .semVerChange(entityGlossaryTermChangeEvent.getSemVerChange())
                  .description(entityGlossaryTermChangeEvent.getDescription())
                  .parameters(parameters)
                  .auditStamp(entityGlossaryTermChangeEvent.getAuditStamp())
                  .build();
            })
        .collect(Collectors.toList());
  }

  public static ChangeEvent convertEntityDocumentationChangeEvent(
      @Nonnull String fieldPath, @Nonnull Urn parentUrn, @Nonnull ChangeEvent event) {
    Map<String, Object> parameters = new HashMap<>(event.getParameters());
    parameters.put("fieldPath", fieldPath);
    parameters.put("parentUrn", parentUrn.toString());

    return ChangeEvent.builder()
        .modifier(event.getModifier())
        .entityUrn(event.getEntityUrn())
        .category(event.getCategory())
        .operation(event.getOperation())
        .semVerChange(event.getSemVerChange())
        .description(event.getDescription())
        .parameters(parameters)
        .auditStamp(event.getAuditStamp())
        .build();
  }

  public static <T extends RecordTemplate> List<ChangeEvent> generateChangeEvents(
      @Nonnull EntityChangeEventGeneratorRegistry entityChangeEventGeneratorRegistry,
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final Aspect<T> from,
      @Nonnull final Aspect<T> to,
      @Nonnull AuditStamp auditStamp) {
    final List<EntityChangeEventGenerator<T>> entityChangeEventGenerators =
        entityChangeEventGeneratorRegistry.getEntityChangeEventGenerators(aspectName).stream()
            // Note: Assumes that correct types have been registered for the aspect.
            .map(changeEventGenerator -> (EntityChangeEventGenerator<T>) changeEventGenerator)
            .collect(Collectors.toList());
    final List<ChangeEvent> allChangeEvents = new ArrayList<>();
    for (EntityChangeEventGenerator<T> entityChangeEventGenerator : entityChangeEventGenerators) {
      allChangeEvents.addAll(
          entityChangeEventGenerator.getChangeEvents(
              urn, entityName, aspectName, from, to, auditStamp));
    }
    return allChangeEvents;
  }

  /**
   * Generic sorted-merge diff for two lists of items that can be represented as strings. Produces
   * ADD events for items in {@code target} but not in {@code base}, and REMOVE events for items in
   * {@code base} but not in {@code target}.
   *
   * @param base the previous state (nullable — treated as empty)
   * @param target the current state (nullable — treated as empty)
   * @param eventFactory called with (item, operation) to build the ChangeEvent
   */
  public static <T> List<ChangeEvent> sortedMergeDiff(
      @Nullable List<T> base,
      @Nullable List<T> target,
      @Nonnull java.util.function.Function<T, String> toKey,
      @Nonnull BiFunction<T, ChangeOperation, ChangeEvent> eventFactory) {
    List<T> baseList = base != null ? new ArrayList<>(base) : new ArrayList<>();
    List<T> targetList = target != null ? new ArrayList<>(target) : new ArrayList<>();
    baseList.sort(Comparator.comparing(toKey));
    targetList.sort(Comparator.comparing(toKey));

    List<ChangeEvent> events = new ArrayList<>();
    int bi = 0;
    int ti = 0;
    while (bi < baseList.size() && ti < targetList.size()) {
      int cmp = toKey.apply(baseList.get(bi)).compareTo(toKey.apply(targetList.get(ti)));
      if (cmp == 0) {
        ++bi;
        ++ti;
      } else if (cmp < 0) {
        events.add(eventFactory.apply(baseList.get(bi), ChangeOperation.REMOVE));
        ++bi;
      } else {
        events.add(eventFactory.apply(targetList.get(ti), ChangeOperation.ADD));
        ++ti;
      }
    }
    while (bi < baseList.size()) {
      events.add(eventFactory.apply(baseList.get(bi), ChangeOperation.REMOVE));
      ++bi;
    }
    while (ti < targetList.size()) {
      events.add(eventFactory.apply(targetList.get(ti), ChangeOperation.ADD));
      ++ti;
    }
    return events;
  }

  private ChangeEventGeneratorUtils() {}
}
