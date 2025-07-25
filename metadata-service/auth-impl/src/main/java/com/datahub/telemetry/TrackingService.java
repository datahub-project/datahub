package com.datahub.telemetry;

import static com.linkedin.metadata.Constants.*;

import com.datahub.plugins.auth.authentication.Authenticator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.telemetry.TelemetryClientId;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

@Slf4j
public class TrackingService {
  private static final String APP_VERSION_FIELD = "appVersion";
  private static final String EVENT_TYPE_FIELD = "type";
  private static final String FAILED_EVENT_NANE = "FailedEvent";
  private static final String ENTITY_TYPE_FIELD = "entityType";
  private static final String ENTITY_TYPE_FILTER_FIELD = "entityTypeFilter";
  private static final String PAGE_NUMBER_FIELD = "pageNumber";
  private static final String PAGE_FIELD = "page";
  private static final String TOTAL_FIELD = "total";
  private static final String INDEX_FIELD = "index";
  private static final String RESULT_TYPE_FIELD = "resultType";
  private static final String RENDER_ID_FIELD = "renderId";
  private static final String MODULE_ID_FIELD = "moduleId";
  private static final String RENDER_TYPE_FIELD = "renderType";
  private static final String SCENARIO_TYPE_FIELD = "scenarioType";
  private static final String SECTION_FIELD = "section";
  private static final String ACCESS_TOKEN_TYPE_FIELD = "accessTokenType";
  private static final String DURATION_FIELD = "duration";
  private static final String ROLE_URN_FIELD = "roleUrn";
  private static final String POLICY_URN_FIELD = "policyUrn";
  private static final String SOURCE_TYPE_FIELD = "sourceType";
  private static final String INTERVAL_FIELD = "interval";
  private static final String VIEW_TYPE_FIELD = "viewType";

  private static final Set<String> ALLOWED_EVENT_FIELDS =
      new HashSet<>(
          ImmutableList.of(
              EVENT_TYPE_FIELD,
              ENTITY_TYPE_FIELD,
              ENTITY_TYPE_FILTER_FIELD,
              PAGE_NUMBER_FIELD,
              PAGE_FIELD,
              TOTAL_FIELD,
              INDEX_FIELD,
              RESULT_TYPE_FIELD,
              RENDER_ID_FIELD,
              MODULE_ID_FIELD,
              RENDER_TYPE_FIELD,
              SCENARIO_TYPE_FIELD,
              SECTION_FIELD,
              ACCESS_TOKEN_TYPE_FIELD,
              DURATION_FIELD,
              ROLE_URN_FIELD,
              POLICY_URN_FIELD,
              SOURCE_TYPE_FIELD,
              INTERVAL_FIELD,
              VIEW_TYPE_FIELD));

  private static final String ACTOR_URN_FIELD = "actorUrn";
  private static final String ORIGIN_FIELD = "origin";
  private static final String ENTITY_URN_FIELD = "entityUrn";
  private static final String ENTITY_URNS_FIELD = "entityUrns";
  private static final String GROUP_NAME_FIELD = "groupName";
  private static final String ENTITY_PAGE_FILTER_FIELD = "entityPageFilter";
  private static final String PATH_FIELD = "path";
  private static final String USER_URN_FIELD = "userUrn";
  private static final String USER_URNS_FIELD = "userUrns";
  private static final String PARENT_NODE_URN_FIELD = "parentNodeUrn";
  private static final Set<String> ALLOWED_OBFUSCATED_EVENT_FIELDS =
      new HashSet<>(
          ImmutableList.of(
              ACTOR_URN_FIELD,
              ORIGIN_FIELD,
              ENTITY_URN_FIELD,
              ENTITY_URNS_FIELD,
              GROUP_NAME_FIELD,
              SECTION_FIELD,
              ENTITY_PAGE_FILTER_FIELD,
              PATH_FIELD,
              USER_URN_FIELD,
              USER_URNS_FIELD,
              PARENT_NODE_URN_FIELD));

  private final TopicsConfiguration topicsConfiguration;
  private final SecretService secretService;
  private final MessageBuilder messageBuilder;
  private final MixpanelAPI mixpanelAPI;
  private final EntityService _entityService;
  private final GitVersion _gitVersion;
  private final ObjectMapper _objectMapper = new ObjectMapper();
  private final ObjectWriter _objectWriter = _objectMapper.writerWithDefaultPrettyPrinter();
  private String _clientId;
  private final Producer<String, String> dataHubUsageProducer;

  public TrackingService(
      final TopicsConfiguration topicsConfiguration,
      final SecretService secretService,
      final MessageBuilder messageBuilder,
      final MixpanelAPI mixpanelAPI,
      @Nonnull EntityService<?> entityService,
      @Nonnull GitVersion gitVersion,
      @Nullable Producer<String, String> dataHubUsageProducer) {
    this.topicsConfiguration = topicsConfiguration;
    this.secretService = secretService;
    this.messageBuilder = messageBuilder;
    this.mixpanelAPI = mixpanelAPI;
    this._entityService = entityService;
    this._gitVersion = gitVersion;
    this.dataHubUsageProducer = dataHubUsageProducer;

    // Log Mixpanel configuration
    if (mixpanelAPI != null && messageBuilder != null) {
      log.info("TrackingService initialized with Mixpanel client and message builder");
      log.info("Mixpanel client is configured and ready to send events");
    } else {
      log.info("TrackingService initialized without Mixpanel client or message builder");
    }

    // Log Kafka configuration
    if (dataHubUsageProducer != null) {
      log.info("TrackingService initialized with Kafka producer for DataHubUsageEvent");
    } else {
      log.info("TrackingService initialized without Kafka producer for DataHubUsageEvent");
    }
  }

  /**
   * Parse a timestamp from various formats (numeric or string) and convert it to epoch milliseconds
   *
   * @param timestamp The timestamp to parse
   * @return The timestamp in epoch milliseconds
   */
  @VisibleForTesting
  long parseTimestamp(Object timestamp) {
    if (timestamp == null) {
      return System.currentTimeMillis();
    }

    try {
      if (timestamp instanceof Number) {
        // If it's already a number, assume it's in milliseconds if > 1e12, otherwise seconds
        long value = ((Number) timestamp).longValue();
        if (value > 1e12) {
          return value; // Already in milliseconds
        } else {
          return value * 1000; // Convert seconds to milliseconds
        }
      } else if (timestamp instanceof String) {
        String timestampStr = (String) timestamp;
        try {
          // Try parsing as ISO 8601
          return java.time.Instant.parse(timestampStr).toEpochMilli();
        } catch (Exception e) {
          // If not ISO format, try parsing as a number
          try {
            long value = Long.parseLong(timestampStr);
            if (value > 1e12) {
              return value; // Already in milliseconds
            } else {
              return value * 1000; // Convert seconds to milliseconds
            }
          } catch (NumberFormatException nfe) {
            log.warn("Failed to parse timestamp: {}", timestampStr, nfe);
            return System.currentTimeMillis();
          }
        }
      } else {
        log.warn("Unsupported timestamp type: {}", timestamp.getClass().getName());
        return System.currentTimeMillis();
      }
    } catch (Exception e) {
      log.warn("Error parsing timestamp: {}", timestamp, e);
      return System.currentTimeMillis();
    }
  }

  /**
   * Format a timestamp in epoch milliseconds to ISO 8601 format
   *
   * @param timestampMillis The timestamp in epoch milliseconds
   * @return The timestamp in ISO 8601 format
   */
  private String formatTimestampToISO(long timestampMillis) {
    return java.time.Instant.ofEpochMilli(timestampMillis).toString();
  }

  /**
   * Track an event with additional properties from a JsonNode This will route the event to all the
   * configured tracking destinations If you want to send to a specific tracking destination use the
   * {@link #track(String, OperationContext, Authenticator, EntityClient, JsonNode, Set)} method
   *
   * @param eventName The name of the event
   * @param opContext The operation context
   * @param authenticator The authenticator
   * @param entityClient The entity client
   * @param eventData The event data as a JsonNode
   */
  public int track(
      @Nonnull final String eventName,
      @Nonnull final OperationContext opContext,
      @Nullable final Authenticator authenticator,
      @Nullable final EntityClient entityClient,
      @Nonnull final JsonNode eventData) {
    // Call the track method with all destinations
    return track(
        eventName,
        opContext,
        authenticator,
        entityClient,
        eventData,
        java.util.EnumSet.allOf(TrackingDestination.class));
  }

  /**
   * Track an event with additional properties from a JsonNode and specify which destinations to use
   *
   * @param eventName The name of the event
   * @param opContext The operation context
   * @param authenticator The authenticator
   * @param entityClient The entity client
   * @param eventData The event data as a JsonNode
   * @param destinations The set of destinations to send the event to
   * @return the number of destinations that were sent to
   */
  public int track(
      @Nonnull final String eventName,
      @Nonnull final OperationContext opContext,
      @Nullable final Authenticator authenticator,
      @Nullable final EntityClient entityClient,
      @Nonnull final JsonNode eventData,
      @Nonnull final java.util.Set<TrackingDestination> destinations) {

    int numDestinationsSent = 0;

    final String actorId =
        eventData.has("actorUrn")
            ? eventData.get("actorUrn").asText()
            : opContext.getActorContext().getActorUrn().toString();

    // Send to Mixpanel if requested and available
    if (destinations.contains(TrackingDestination.MIXPANEL)
        && mixpanelAPI != null
        && messageBuilder != null) {
      try {
        log.debug("Mixpanel - Raw event data: {}", eventData.toPrettyString());

        // Create a new properties object
        JSONObject properties = new JSONObject();

        // Add standard properties
        properties.put("distinct_id", actorId);
        properties.put("actor", actorId);
        properties.put("version", _gitVersion.getVersion());

        // Parse and format timestamp for Mixpanel (ISO format)
        long timestampMillis = System.currentTimeMillis();
        if (eventData.has("timestamp")) {
          JsonNode timestampNode = eventData.get("timestamp");
          if (timestampNode.isNumber()) {
            timestampMillis = parseTimestamp(timestampNode.numberValue());
          } else if (timestampNode.isTextual()) {
            timestampMillis = parseTimestamp(timestampNode.asText());
          }
        }

        // Add ISO formatted timestamp to properties
        properties.put("time", formatTimestampToISO(timestampMillis));

        // Add the event type to the properties
        properties.put(EVENT_TYPE_FIELD, eventName);
        log.debug("Added standard properties: {}", properties.toString());

        // Add all fields from the event data to the properties
        Iterator<Map.Entry<String, JsonNode>> fields = eventData.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          String key = field.getKey();
          JsonNode value = field.getValue();

          // Skip the timestamp field as we've already handled it
          if ("timestamp".equals(key)) {
            continue;
          }

          if (value.isTextual()) {
            properties.put(key, value.asText());
          } else if (value.isNumber()) {
            properties.put(key, value.numberValue());
          } else if (value.isBoolean()) {
            properties.put(key, value.booleanValue());
          } else if (value.isNull()) {
            properties.put(key, JSONObject.NULL);
          } else if (value.isObject() || value.isArray()) {
            properties.put(key, value.toString());
          }
        }

        // Sanitize the properties
        JSONObject sanitizedProperties = sanitizeEvent(properties);
        if (sanitizedProperties != null) {
          log.debug("Final sanitized properties: {}", sanitizedProperties);

          // Create the event message using MessageBuilder with the sanitized properties
          JSONObject message = messageBuilder.event(actorId, eventName, sanitizedProperties);
          log.debug("Final message to be sent: {}", message);

          // Send the message to Mixpanel
          mixpanelAPI.sendMessage(message);
          log.debug("Successfully sent event {} to Mixpanel with additional properties", eventName);
          numDestinationsSent += 1;
        } else {
          log.warn("Sanitization returned null properties, skipping Mixpanel event");
        }
      } catch (Exception e) {
        log.error(
            "Failed to track event in Mixpanel: {} - Error: {}", eventName, e.getMessage(), e);
      }
    } else if (destinations.contains(TrackingDestination.MIXPANEL)) {
      log.warn("Mixpanel tracking is not enabled or not requested. Skipping event: {}", eventName);
    }

    // Send to Kafka if requested and available
    if (destinations.contains(TrackingDestination.KAFKA) && dataHubUsageProducer != null) {
      try {
        log.debug("Sending event to Kafka: {}", eventName);

        // Create a copy of the event data with the timestamp in epoch milliseconds
        ObjectNode kafkaEventData = _objectMapper.createObjectNode();
        kafkaEventData.setAll((ObjectNode) eventData);

        // Parse and format timestamp for Kafka (epoch milliseconds)
        long timestampMillis = System.currentTimeMillis();
        if (eventData.has("timestamp")) {
          JsonNode timestampNode = eventData.get("timestamp");
          if (timestampNode.isNumber()) {
            timestampMillis = parseTimestamp(timestampNode.numberValue());
          } else if (timestampNode.isTextual()) {
            timestampMillis = parseTimestamp(timestampNode.asText());
          }
        }

        // Add the timestamp in epoch milliseconds
        kafkaEventData.put("timestamp", timestampMillis);

        // Also update the timestamp in the nested event if it exists
        if (kafkaEventData.has("event") && kafkaEventData.get("event").isObject()) {
          ObjectNode eventNode = (ObjectNode) kafkaEventData.get("event");
          if (eventNode.has("timestamp")) {
            eventNode.put("timestamp", timestampMillis);
          }
        }

        String eventJson = _objectWriter.writeValueAsString(kafkaEventData);
        dataHubUsageProducer.send(
            new ProducerRecord<>(topicsConfiguration.getDataHubUsage(), actorId, eventJson),
            (metadata, exception) -> {
              if (exception != null) {
                log.error(
                    "Failed to send event to Kafka: {} - Error: {}",
                    eventName,
                    exception.getMessage(),
                    exception);
              } else {
                log.debug(
                    "Successfully sent event to Kafka: {} - Topic: {}, Partition: {}, Offset: {}",
                    eventName,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
              }
            });
        numDestinationsSent += 1;
      } catch (Exception e) {
        log.error("Failed to send event to Kafka: {} - Error: {}", eventName, e.getMessage(), e);
      }
    } else if (destinations.contains(TrackingDestination.KAFKA)) {
      log.warn(
          "Kafka producer is not available or not requested. Skipping Kafka event: {}", eventName);
    }
    return numDestinationsSent;
  }

  @Nonnull
  public String getClientId(@Nonnull OperationContext opContext) {
    // Return cached client id if it exists
    if (_clientId != null) {
      return _clientId;
    }

    Urn clientIdUrn = UrnUtils.getUrn(CLIENT_ID_URN);
    // Create a new client id if it doesn't exist
    if (!_entityService.exists(opContext, clientIdUrn, true)) {
      return createClientIdIfNotPresent(opContext, _entityService);
    }

    // Otherwise, return the existing client id from the metadata store
    RecordTemplate clientIdTemplate =
        _entityService.getLatestAspect(opContext, clientIdUrn, CLIENT_ID_ASPECT);
    // Should always be present here from above, so no need for null check
    _clientId = ((TelemetryClientId) clientIdTemplate).getClientId();
    return _clientId;
  }

  @Nullable
  public JSONObject sanitizeEvent(@Nonnull final JSONObject event) {
    final JSONObject sanitizedEvent = new JSONObject();

    // Add app version to the sanitized event
    sanitizedEvent.put(APP_VERSION_FIELD, _gitVersion.getVersion());

    // Add event type to the sanitized event
    if (event.has(EVENT_TYPE_FIELD)) {
      sanitizedEvent.put(EVENT_TYPE_FIELD, event.get(EVENT_TYPE_FIELD));
    } else {
      sanitizedEvent.put(EVENT_TYPE_FIELD, FAILED_EVENT_NANE);
    }

    final Iterator<String> keys = event.keys();

    while (keys.hasNext()) {
      final String key = keys.next();
      try {
        // we only send an allowed list of fields
        if (ALLOWED_EVENT_FIELDS.contains(key)) {
          sanitizedEvent.put(key, event.get(key));
        } else if (ALLOWED_OBFUSCATED_EVENT_FIELDS.contains(key)) {
          // we obfuscate fields that are sensitive
          sanitizedEvent.put(key, secretService.hashString(event.getString(key)));
        }
      } catch (JSONException e) {
        log.warn("Failed to sanitize field {}. Skipping this field.", key, e);
      }
    }
    return sanitizedEvent;
  }

  @Nullable
  @VisibleForTesting
  JSONObject createFailedEvent() {
    final ObjectNode failedEventObj = JsonNodeFactory.instance.objectNode();
    failedEventObj.put(APP_VERSION_FIELD, _gitVersion.getVersion());
    failedEventObj.put(EVENT_TYPE_FIELD, FAILED_EVENT_NANE);

    return transformObjectNodeToJSONObject(failedEventObj);
  }

  @Nullable
  @VisibleForTesting
  JSONObject transformObjectNodeToJSONObject(@Nonnull final ObjectNode objectNode) {
    final JSONObject jsonObject;
    try {
      jsonObject = new JSONObject(_objectWriter.writeValueAsString(objectNode));
    } catch (Exception e) {
      log.error("Failed to serialize sanitized event", e);
      return null;
    }
    return jsonObject;
  }

  @Nonnull
  private static String createClientIdIfNotPresent(
      @Nonnull OperationContext opContext, @Nonnull final EntityService entityService) {
    String uuid = UUID.randomUUID().toString();
    TelemetryClientId clientId = new TelemetryClientId().setClientId(uuid);
    final AuditStamp clientIdStamp = new AuditStamp();
    clientIdStamp.setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR));
    clientIdStamp.setTime(System.currentTimeMillis());
    entityService.ingestAspectIfNotPresent(
        opContext, UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT, clientId, clientIdStamp, null);
    return uuid;
  }
}
