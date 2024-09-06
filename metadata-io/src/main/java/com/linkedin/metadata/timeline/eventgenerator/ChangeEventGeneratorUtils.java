package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldGlossaryTermChangeEvent;
import com.linkedin.metadata.timeline.data.dataset.schema.SchemaFieldTagChangeEvent;
import com.linkedin.metadata.timeline.data.entity.GlossaryTermChangeEvent;
import com.linkedin.metadata.timeline.data.entity.TagChangeEvent;
import com.linkedin.schema.SchemaField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class ChangeEventGeneratorUtils {

  public static Urn getSchemaFieldUrn(
      @Nonnull String datasetUrnStr, @Nonnull String schemaFieldPath) {
    return UrnUtils.getUrn(
        String.format("urn:li:schemaField:(%s,%s)", datasetUrnStr, schemaFieldPath));
  }

  public static Urn getSchemaFieldUrn(@Nonnull Urn datasetUrn, @Nonnull String schemaFieldPath) {
    return UrnUtils.getUrn(
        String.format("urn:li:schemaField:(%s,%s)", datasetUrn.toString(), schemaFieldPath));
  }

  public static Urn getSchemaFieldUrn(@Nonnull Urn datasetUrn, @Nonnull SchemaField schemaField) {
    return UrnUtils.getUrn(
        String.format("urn:li:schemaField:(%s,%s)", datasetUrn, getFieldPathV1(schemaField)));
  }

  public static String getFieldPathV1(@Nonnull SchemaField field) {
    String[] v1PathTokens =
        Arrays.stream(field.getFieldPath().split("\\."))
            .filter(x -> !(x.startsWith("[") || x.endsWith("]")))
            .toArray(String[]::new);
    return String.join(".", v1PathTokens);
  }

  public static List<ChangeEvent> convertEntityTagChangeEvents(
      @Nonnull String fieldPath,
      @Nonnull Urn parentUrn,
      @Nonnull List<ChangeEvent> entityTagChangeEvents) {
    return entityTagChangeEvents.stream()
        .filter(entityTagChangeEvent -> entityTagChangeEvent instanceof TagChangeEvent)
        .map(entityTagChangeEvent -> (TagChangeEvent) entityTagChangeEvent)
        .map(
            entityTagChangeEvent ->
                SchemaFieldTagChangeEvent.schemaFieldTagChangeEventBuilder()
                    .modifier(entityTagChangeEvent.getModifier())
                    .entityUrn(entityTagChangeEvent.getEntityUrn())
                    .category(entityTagChangeEvent.getCategory())
                    .operation(entityTagChangeEvent.getOperation())
                    .semVerChange(entityTagChangeEvent.getSemVerChange())
                    .description(entityTagChangeEvent.getDescription())
                    .tagUrn(
                        UrnUtils.getUrn(
                            (String) entityTagChangeEvent.getParameters().get("tagUrn")))
                    .auditStamp(entityTagChangeEvent.getAuditStamp())
                    .fieldPath(fieldPath)
                    .parentUrn(parentUrn)
                    .build())
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
            entityGlossaryTermChangeEvent ->
                SchemaFieldGlossaryTermChangeEvent.schemaFieldGlossaryTermChangeEventBuilder()
                    .modifier(entityGlossaryTermChangeEvent.getModifier())
                    .entityUrn(entityGlossaryTermChangeEvent.getEntityUrn())
                    .category(entityGlossaryTermChangeEvent.getCategory())
                    .operation(entityGlossaryTermChangeEvent.getOperation())
                    .semVerChange(entityGlossaryTermChangeEvent.getSemVerChange())
                    .description(entityGlossaryTermChangeEvent.getDescription())
                    .termUrn(
                        UrnUtils.getUrn(
                            (String) entityGlossaryTermChangeEvent.getParameters().get("termUrn")))
                    .auditStamp(entityGlossaryTermChangeEvent.getAuditStamp())
                    .fieldPath(fieldPath)
                    .parentUrn(parentUrn)
                    .build())
        .collect(Collectors.toList());
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

  private ChangeEventGeneratorUtils() {}
}
