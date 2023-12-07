package com.datahub.telemetry;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.telemetry.TelemetryClientId;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;

@Slf4j
@RequiredArgsConstructor
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

  private final MixpanelAPI _mixpanelAPI;
  private final MessageBuilder _mixpanelMessageBuilder;
  private final SecretService _secretService;
  private final EntityService _entityService;
  private final GitVersion _gitVersion;
  private final ObjectMapper _objectMapper = new ObjectMapper();
  private final ObjectWriter _objectWriter = _objectMapper.writerWithDefaultPrettyPrinter();
  private String _clientId;

  public void emitAnalyticsEvent(@Nonnull final JsonNode event) {
    final JSONObject sanitizedEvent = sanitizeEvent(event);
    if (sanitizedEvent == null) {
      return;
    }

    final String eventType;
    try {
      eventType = sanitizedEvent.getString(EVENT_TYPE_FIELD);
    } catch (JSONException e) {
      log.error("Failed to parse event type from event", e);
      return;
    }

    try {
      _mixpanelAPI.sendMessage(
          _mixpanelMessageBuilder.event(getClientId(), eventType, sanitizedEvent));
    } catch (IOException e) {
      log.info(
          "Failed to send event to Mixpanel; this does not affect the functionality of the application");
      log.debug("Failed to send event to Mixpanel", e);
    }
  }

  @Nonnull
  public String getClientId() {
    // Return cached client id if it exists
    if (_clientId != null) {
      return _clientId;
    }

    Urn clientIdUrn = UrnUtils.getUrn(CLIENT_ID_URN);
    // Create a new client id if it doesn't exist
    if (!_entityService.exists(clientIdUrn)) {
      return createClientIdIfNotPresent(_entityService);
    }

    // Otherwise, return the existing client id from the metadata store
    RecordTemplate clientIdTemplate = _entityService.getLatestAspect(clientIdUrn, CLIENT_ID_ASPECT);
    // Should always be present here from above, so no need for null check
    _clientId = ((TelemetryClientId) clientIdTemplate).getClientId();
    return _clientId;
  }

  @Nullable
  JSONObject sanitizeEvent(@Nonnull final JsonNode event) {
    final ObjectNode sanitizedEventObj = _objectMapper.createObjectNode();
    sanitizedEventObj.put(APP_VERSION_FIELD, _gitVersion.getVersion());

    final JSONObject unsanitizedEventObj;
    try {
      unsanitizedEventObj =
          new JSONObject(_objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(event));
    } catch (Exception e) {
      log.warn("Failed to serialize event", e);
      return createFailedEvent();
    }

    if (!unsanitizedEventObj.has(EVENT_TYPE_FIELD) || !unsanitizedEventObj.has(ACTOR_URN_FIELD)) {
      log.warn("Event is missing a required field");
      return createFailedEvent();
    }

    unsanitizedEventObj
        .keys()
        .forEachRemaining(
            key -> {
              String keyString = (String) key;
              try {
                if (ALLOWED_EVENT_FIELDS.contains(keyString)) {
                  sanitizedEventObj.put(keyString, unsanitizedEventObj.get(keyString).toString());
                } else if (ALLOWED_OBFUSCATED_EVENT_FIELDS.contains(keyString)) {
                  sanitizedEventObj.put(
                      keyString,
                      _secretService.hashString(unsanitizedEventObj.get(keyString).toString()));
                }
              } catch (JSONException e) {
                log.warn(
                    String.format("Failed to sanitize field %s. Skipping this field.", keyString),
                    e);
              }
            });

    return transformObjectNodeToJSONObject(sanitizedEventObj);
  }

  @Nullable
  JSONObject createFailedEvent() {
    final ObjectNode failedEventObj = _objectMapper.createObjectNode();
    failedEventObj.put(APP_VERSION_FIELD, _gitVersion.getVersion());
    failedEventObj.put(EVENT_TYPE_FIELD, FAILED_EVENT_NANE);

    return transformObjectNodeToJSONObject(failedEventObj);
  }

  @Nullable
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
  private static String createClientIdIfNotPresent(@Nonnull final EntityService entityService) {
    String uuid = UUID.randomUUID().toString();
    TelemetryClientId clientId = new TelemetryClientId().setClientId(uuid);
    final AuditStamp clientIdStamp = new AuditStamp();
    clientIdStamp.setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR));
    clientIdStamp.setTime(System.currentTimeMillis());
    entityService.ingestAspectIfNotPresent(
        UrnUtils.getUrn(CLIENT_ID_URN), CLIENT_ID_ASPECT, clientId, clientIdStamp, null);
    return uuid;
  }
}
