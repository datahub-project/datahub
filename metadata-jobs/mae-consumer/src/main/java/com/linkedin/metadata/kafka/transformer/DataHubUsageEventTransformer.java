package com.linkedin.metadata.kafka.transformer;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.datahubusage.DataHubUsageEventConstants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.kafka.hydrator.EntityHydrator;
import com.linkedin.metadata.kafka.hydrator.EntityType;
import java.util.Optional;
import java.util.Set;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Transformer that transforms usage event (schema defined HERE) into a search document */
@Slf4j
@Component
public class DataHubUsageEventTransformer {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private static final Set<DataHubUsageEventType> EVENTS_WITH_ENTITY_URN =
      ImmutableSet.of(
          DataHubUsageEventType.SEARCH_RESULT_CLICK_EVENT,
          DataHubUsageEventType.BROWSE_RESULT_CLICK_EVENT,
          DataHubUsageEventType.ENTITY_VIEW_EVENT,
          DataHubUsageEventType.ENTITY_SECTION_VIEW_EVENT,
          DataHubUsageEventType.ENTITY_ACTION_EVENT);

  private final EntityHydrator _entityHydrator;

  @Value
  public static class TransformedDocument {
    String id;
    String document;
  }

  public DataHubUsageEventTransformer(EntityHydrator entityHydrator) {
    this._entityHydrator = entityHydrator;
  }

  public Optional<TransformedDocument> transformDataHubUsageEvent(String dataHubUsageEvent) {
    ObjectNode usageEvent;
    try {
      usageEvent = (ObjectNode) OBJECT_MAPPER.readTree(dataHubUsageEvent);
    } catch (Exception e) {
      log.info("Failed to parse event: {}", dataHubUsageEvent);
      return Optional.empty();
    }
    // Search event inherits all fields from the usage event
    ObjectNode eventDocument = usageEvent.deepCopy();
    // Type is required
    if (!usageEvent.has(TYPE)) {
      return Optional.empty();
    }
    DataHubUsageEventType eventType = DataHubUsageEventType.getType(usageEvent.get(TYPE).asText());
    if (eventType == null) {
      log.info("Invalid event type: {}", usageEvent.get(TYPE).asText());
      return Optional.empty();
    }

    // Timestamp is required
    if (!usageEvent.has(TIMESTAMP)) {
      return Optional.empty();
    }
    // Set @timestamp
    eventDocument.put("@timestamp", usageEvent.get(TIMESTAMP).asLong());

    // Hydrate actor fields
    setFieldsForEntity(EntityType.CORP_USER, usageEvent.get(ACTOR_URN).asText(), eventDocument);

    // Hydrate entity fields for events with entity URN
    if (EVENTS_WITH_ENTITY_URN.contains(eventType)) {
      setFieldsForEntity(usageEvent, eventDocument);
    }

    try {
      return Optional.of(
          new TransformedDocument(
              getId(eventDocument), OBJECT_MAPPER.writeValueAsString(eventDocument)));
    } catch (JsonProcessingException e) {
      log.info("Failed to package document: {}", eventDocument);
      return Optional.empty();
    }
  }

  private void setFieldsForEntity(ObjectNode recordObject, ObjectNode searchObject) {
    if (!recordObject.has(ENTITY_TYPE) || !recordObject.has(ENTITY_URN)) {
      return;
    }

    String entityType = recordObject.get(ENTITY_TYPE).asText();
    EntityType type;
    try {
      type = EntityType.valueOf(entityType);
    } catch (IllegalArgumentException e) {
      log.info("Unsupported entity type: {}", entityType);
      return;
    }

    setFieldsForEntity(type, recordObject.get(ENTITY_URN).asText(), searchObject);
  }

  private void setFieldsForEntity(EntityType entityType, String urn, ObjectNode searchObject) {
    Optional<ObjectNode> entityObject = _entityHydrator.getHydratedEntity(urn);
    if (!entityObject.isPresent()) {
      log.info("No matches for urn {}", urn);
      return;
    }
    entityObject
        .get()
        .fieldNames()
        .forEachRemaining(
            key ->
                searchObject.put(
                    entityType.name().toLowerCase() + "_" + key,
                    entityObject.get().get(key).asText()));
  }

  private String getId(final ObjectNode eventDocument) {
    return eventDocument.get(TYPE).asText()
        + "_"
        + eventDocument.get(ACTOR_URN).asText()
        + "_"
        + eventDocument.get(TIMESTAMP).asText();
  }
}
