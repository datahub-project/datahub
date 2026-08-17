package io.datahubproject.openapi.openlineage.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public final class OpenLineageEventDeserializer {
  private static final UUID NAMESPACE_URL_UUID =
      UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");

  private final ObjectMapper objectMapper;

  public OpenLineageEventDeserializer() {
    objectMapper = OpenLineageClientUtils.newObjectMapper();
    SimpleModule flexibleTimeModule = new SimpleModule("OpenLineageFlexibleTime");
    flexibleTimeModule.addDeserializer(
        ZonedDateTime.class, new SystemDefaultZoneZonedDateTimeDeserializer());
    objectMapper.registerModule(flexibleTimeModule);
    objectMapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
  }

  public <T> T deserialize(JsonNode event, Class<T> eventClass) throws JsonProcessingException {
    return objectMapper.treeToValue(normalizeLegacyRunId(event, eventClass), eventClass);
  }

  private static JsonNode normalizeLegacyRunId(JsonNode event, Class<?> eventClass) {
    if (eventClass != OpenLineage.RunEvent.class || !event.path("run").isObject()) {
      return event;
    }
    JsonNode runId = event.path("run").path("runId");
    if (!runId.isTextual() || isUuid(runId.textValue())) {
      return event;
    }

    ObjectNode normalized = event.deepCopy();
    String identity =
        String.join(
            ".",
            event.path("job").path("namespace").textValue(),
            event.path("job").path("name").textValue(),
            runId.textValue());
    byte[] name = identity.getBytes(StandardCharsets.UTF_8);
    ByteBuffer namespacedName = ByteBuffer.allocate(16 + name.length);
    namespacedName
        .putLong(NAMESPACE_URL_UUID.getMostSignificantBits())
        .putLong(NAMESPACE_URL_UUID.getLeastSignificantBits())
        .put(name);
    ((ObjectNode) normalized.path("run"))
        .put("runId", UUID.nameUUIDFromBytes(namespacedName.array()).toString());
    return normalized;
  }

  private static boolean isUuid(String value) {
    try {
      UUID.fromString(value);
      return true;
    } catch (IllegalArgumentException exception) {
      return false;
    }
  }

  private static final class SystemDefaultZoneZonedDateTimeDeserializer
      extends InstantDeserializer<ZonedDateTime> {
    private static final DateTimeFormatter DATE_TIME_OPTIONAL_OFFSET =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .parseLenient()
            .optionalStart()
            .appendOffsetId()
            .toFormatter();

    private SystemDefaultZoneZonedDateTimeDeserializer() {
      super(
          ZonedDateTime.class,
          DATE_TIME_OPTIONAL_OFFSET,
          SystemDefaultZoneZonedDateTimeDeserializer::fromTemporal,
          value -> ZonedDateTime.ofInstant(Instant.ofEpochMilli(value.value), value.zoneId),
          value ->
              ZonedDateTime.ofInstant(
                  Instant.ofEpochSecond(value.integer, value.fraction), value.zoneId),
          ZonedDateTime::withZoneSameInstant,
          false);
    }

    private static ZonedDateTime fromTemporal(TemporalAccessor temporal) {
      ZoneId zone = temporal.query(TemporalQueries.zone());
      return zone == null
          ? LocalDateTime.from(temporal).atZone(ZoneId.systemDefault())
          : ZonedDateTime.from(temporal);
    }
  }
}
