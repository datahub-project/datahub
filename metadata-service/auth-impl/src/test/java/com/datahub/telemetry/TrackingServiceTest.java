package com.datahub.telemetry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.telemetry.TelemetryClientId;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import java.io.IOException;
import java.util.Optional;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class TrackingServiceTest {
  private static final String APP_VERSION_FIELD = "appVersion";
  private static final String APP_VERSION = "1.0.0";
  private static final String CLIENT_ID = "testClientId";
  private static final TelemetryClientId TELEMETRY_CLIENT_ID = new TelemetryClientId().setClientId(CLIENT_ID);
  private static final String NOT_ALLOWED_FIELD = "browserId";
  private static final String NOT_ALLOWED_FIELD_VALUE = "testBrowserId";
  private static final String EVENT_TYPE_FIELD = "type";
  private static final String EVENT_TYPE = "TestEvent";
  private static final String FAILED_EVENT_TYPE = "FailedEvent";
  private static final String ACTOR_URN_FIELD = "actorUrn";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:user";
  private static final String HASHED_ACTOR_URN_STRING = "hashedActorUrn";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private Urn _clientIdUrn;
  private JSONObject _mixpanelMessage;
  private MixpanelAPI _mixpanelAPI;
  private MessageBuilder _mixpanelMessageBuilder;
  private SecretService _secretService;
  private EntityService _entityService;
  private TrackingService _trackingService;

  @BeforeMethod
  public void setupTest() {
    _clientIdUrn = UrnUtils.getUrn(CLIENT_ID_URN);
    _mixpanelMessage = new JSONObject();

    _mixpanelAPI = mock(MixpanelAPI.class);
    _mixpanelMessageBuilder = mock(MessageBuilder.class);
    _secretService = mock(SecretService.class);
    _entityService = mock(EntityService.class);
    GitVersion gitVersion = new GitVersion(APP_VERSION, "", Optional.empty());

    _trackingService =
        new TrackingService(_mixpanelAPI, _mixpanelMessageBuilder, _secretService, _entityService, gitVersion);
  }

  @Test
  public void testEmitAnalyticsEvent() throws IOException {
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    when(_entityService.exists(_clientIdUrn)).thenReturn(true);
    when(_entityService.getLatestAspect(eq(_clientIdUrn), eq(CLIENT_ID_ASPECT))).thenReturn(TELEMETRY_CLIENT_ID);
    when(_mixpanelMessageBuilder.event(eq(CLIENT_ID), eq(EVENT_TYPE), any())).thenReturn(_mixpanelMessage);

    final String eventString =
        String.format("{\"%s\": \"%s\", \"%s\": \"%s\", \"%s\": \"%s\"}", EVENT_TYPE_FIELD, EVENT_TYPE, ACTOR_URN_FIELD,
            ACTOR_URN_STRING, NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);
    final JsonNode event = OBJECT_MAPPER.readTree(eventString);
    _trackingService.emitAnalyticsEvent(event);

    verify(_mixpanelAPI, times(1)).sendMessage(eq(_mixpanelMessage));
  }

  @Test
  public void testGetClientIdAlreadyExists() {
    when(_entityService.exists(_clientIdUrn)).thenReturn(true);
    when(_entityService.getLatestAspect(eq(_clientIdUrn), eq(CLIENT_ID_ASPECT))).thenReturn(TELEMETRY_CLIENT_ID);

    assertEquals(CLIENT_ID, _trackingService.getClientId());
  }

  @Test
  public void testGetClientIdDoesNotExist() {
    when(_entityService.exists(_clientIdUrn)).thenReturn(false);

    assertNotNull(_trackingService.getClientId());
    verify(_entityService, times(1)).ingestAspectIfNotPresent(eq(_clientIdUrn), eq(CLIENT_ID_ASPECT),
        any(TelemetryClientId.class), any(), eq(null));
  }

  @Test
  public void testSanitizeEventNoEventType() throws JsonProcessingException, JSONException {
    final String eventString =
        String.format("{\"%s\": \"%s\", \"%s\": \"%s\"}", ACTOR_URN_FIELD, ACTOR_URN_STRING, NOT_ALLOWED_FIELD,
            NOT_ALLOWED_FIELD_VALUE);
    final JsonNode event = OBJECT_MAPPER.readTree(eventString);

    final JSONObject sanitizedEvent = _trackingService.sanitizeEvent(event);
    assertNotNull(sanitizedEvent);
    assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
    assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
    assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), FAILED_EVENT_TYPE);
  }

  @Test
  public void testSanitizeEventNoActorUrn() throws JsonProcessingException, JSONException {
    final String eventString =
        String.format("{\"%s\": \"%s\", \"%s\": \"%s\"}", EVENT_TYPE_FIELD, EVENT_TYPE, NOT_ALLOWED_FIELD,
            NOT_ALLOWED_FIELD_VALUE);
    final JsonNode event = OBJECT_MAPPER.readTree(eventString);

    final JSONObject sanitizedEvent = _trackingService.sanitizeEvent(event);
    assertNotNull(sanitizedEvent);
    assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
    assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
    assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), FAILED_EVENT_TYPE);
  }

  @Test
  public void testSanitizeEvent() throws JsonProcessingException, JSONException {
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);

    final String eventString =
        String.format("{\"%s\": \"%s\", \"%s\": \"%s\", \"%s\": \"%s\"}", EVENT_TYPE_FIELD, EVENT_TYPE, ACTOR_URN_FIELD,
            ACTOR_URN_STRING, NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);
    final JsonNode event = OBJECT_MAPPER.readTree(eventString);

    final JSONObject sanitizedEvent = _trackingService.sanitizeEvent(event);
    assertNotNull(sanitizedEvent);
    assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
    assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
    assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), EVENT_TYPE);
    assertTrue(sanitizedEvent.has(ACTOR_URN_FIELD));
    assertEquals(sanitizedEvent.get(ACTOR_URN_FIELD), HASHED_ACTOR_URN_STRING);
    assertFalse(sanitizedEvent.has(NOT_ALLOWED_FIELD));
  }
}
