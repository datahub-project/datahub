package com.datahub.telemetry;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.kafka.TopicsConfiguration;
import com.linkedin.metadata.config.telemetry.MixpanelConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.telemetry.TelemetryClientId;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.io.IOException;
import java.util.Optional;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TrackingServiceTest {
  private static final String APP_VERSION_FIELD = "appVersion";
  private static final String APP_VERSION = "1.0.0";
  private static final String CLIENT_ID = "testClientId";
  private static final TelemetryClientId TELEMETRY_CLIENT_ID =
      new TelemetryClientId().setClientId(CLIENT_ID);
  private static final String NOT_ALLOWED_FIELD = "browserId";
  private static final String NOT_ALLOWED_FIELD_VALUE = "testBrowserId";
  private static final String EVENT_TYPE_FIELD = "type";
  private static final String EVENT_TYPE = "TestEvent";
  private static final String FAILED_EVENT_TYPE = "FailedEvent";
  private static final String ACTOR_URN_FIELD = "actorUrn";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:user";
  private static final String HASHED_ACTOR_URN_STRING = "hashedActorUrn";
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

  private Urn _clientIdUrn;
  private JSONObject _mixpanelMessage;
  private MixpanelAPI _mixpanelAPI;
  private MessageBuilder _mixpanelMessageBuilder;
  private SecretService _secretService;
  private EntityService<?> _entityService;
  private TrackingService _trackingService;
  private OperationContext opContext;
  private Producer<String, String> dataHubUsageProducer;
  private TrackingService _noObfuscationTrackingService;

  @BeforeMethod
  public void setupTest() {
    _clientIdUrn = UrnUtils.getUrn(CLIENT_ID_URN);
    _mixpanelAPI = mock(MixpanelAPI.class);
    _mixpanelMessageBuilder = mock(MessageBuilder.class);
    _mixpanelMessage = spy(new JSONObject());
    _mixpanelMessage.put("properties", new JSONObject());
    when(_mixpanelMessageBuilder.event(anyString(), anyString(), any(JSONObject.class)))
        .thenReturn(_mixpanelMessage);
    _secretService = mock(SecretService.class);
    _entityService = mock(EntityService.class);
    GitVersion gitVersion = new GitVersion(APP_VERSION, "", Optional.empty());
    MixpanelConfiguration mixpanelConfiguration = mock(MixpanelConfiguration.class);
    when(mixpanelConfiguration.isDisableObfuscation()).thenReturn(false);

    TopicsConfiguration topicsConfiguration = mock(TopicsConfiguration.class);
    when(topicsConfiguration.getDataHubUsage()).thenReturn("DataHubUsageEvent_v1");

    dataHubUsageProducer = mock(Producer.class);
    when(dataHubUsageProducer.send(any(ProducerRecord.class), any())).thenReturn(null);

    // Mock the operation context
    opContext = mock(OperationContext.class);
    io.datahubproject.metadata.context.ActorContext actorContext =
        mock(io.datahubproject.metadata.context.ActorContext.class);
    when(opContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    _trackingService =
        new TrackingService(
            mixpanelConfiguration,
            topicsConfiguration,
            _secretService,
            _mixpanelMessageBuilder,
            _mixpanelAPI,
            _entityService,
            gitVersion,
            dataHubUsageProducer);

    MixpanelConfiguration noObfuscationMixpanelConfiguration = mock(MixpanelConfiguration.class);
    when(noObfuscationMixpanelConfiguration.isDisableObfuscation()).thenReturn(true);
    _noObfuscationTrackingService =
        new TrackingService(
            noObfuscationMixpanelConfiguration,
            topicsConfiguration,
            _secretService,
            _mixpanelMessageBuilder,
            _mixpanelAPI,
            _entityService,
            gitVersion,
            dataHubUsageProducer);
  }

  @Test
  public void testTrackEvent() throws IOException {
    // Mock the secret service
    when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    // Create a test event
    JSONObject testEvent = new JSONObject();
    testEvent.put("type", EVENT_TYPE);
    testEvent.put("entityType", "dataset");
    testEvent.put(
        "entityUrn", "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)");
    testEvent.put("actorUrn", ACTOR_URN_STRING);
    testEvent.put("customField", "test_value");
    testEvent.put("timestamp", "2025-04-05T15:35:03.506874+00:00");

    // Convert to JsonNode
    com.fasterxml.jackson.databind.JsonNode eventNode =
        OBJECT_MAPPER.readTree(testEvent.toString());

    // Mock the opContext behavior
    when(opContext.getActorContext().getActorUrn()).thenReturn(UrnUtils.getUrn(ACTOR_URN_STRING));

    // Call the track method directly
    _trackingService.track(EVENT_TYPE, opContext, null, null, eventNode);

    // Verify that the Mixpanel API was called
    verify(_mixpanelAPI, times(1)).sendMessage(any(JSONObject.class));

    // Verify that the Kafka producer was called
    verify(dataHubUsageProducer, times(1)).send(any(ProducerRecord.class), any());
  }

  @Test
  public void testGetClientIdAlreadyExists() {
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(true);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_clientIdUrn), eq(CLIENT_ID_ASPECT)))
        .thenReturn(TELEMETRY_CLIENT_ID);

    assertEquals(CLIENT_ID, _trackingService.getClientId(opContext));
  }

  @Test
  public void testGetClientIdDoesNotExist() {
    when(_entityService.exists(any(OperationContext.class), eq(_clientIdUrn), eq(true)))
        .thenReturn(false);

    assertNotNull(_trackingService.getClientId(opContext));
    verify(_entityService, times(1))
        .ingestAspectIfNotPresent(
            any(OperationContext.class),
            eq(_clientIdUrn),
            eq(CLIENT_ID_ASPECT),
            any(TelemetryClientId.class),
            any(),
            eq(null));
  }

  @Test
  public void testSanitizeEventNoEventType() throws JsonProcessingException, JSONException {
    final JSONObject event = new JSONObject();
    event.put(ACTOR_URN_FIELD, ACTOR_URN_STRING);
    event.put(NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);

    final JSONObject sanitizedEvent = _trackingService.sanitizeEvent(event);
    assertNotNull(sanitizedEvent);
    assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
    assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
    assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
    assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), FAILED_EVENT_TYPE);
  }

  @Test
  public void testSanitizeEventNoActorUrn() throws JsonProcessingException, JSONException {
    for (boolean enableObfuscation : new boolean[] {true, false}) {
      final JSONObject event = new JSONObject();
      event.put(EVENT_TYPE_FIELD, EVENT_TYPE);
      event.put(NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);

      final JSONObject sanitizedEvent =
          enableObfuscation
              ? _trackingService.sanitizeEvent(event)
              : _noObfuscationTrackingService.sanitizeEvent(event);
      assertNotNull(sanitizedEvent);
      assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
      assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
      assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
      assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), EVENT_TYPE);
      assertFalse(sanitizedEvent.has(ACTOR_URN_FIELD));
      if (enableObfuscation) {
        assertFalse(sanitizedEvent.has(NOT_ALLOWED_FIELD));
      } else {
        assertTrue(sanitizedEvent.has(NOT_ALLOWED_FIELD));
      }
    }
  }

  @Test
  public void testSanitizeEvent() throws JsonProcessingException, JSONException {

    for (boolean enableObfuscation : new boolean[] {true, false}) {
      reset(_secretService);
      when(_secretService.hashString(eq(ACTOR_URN_STRING))).thenReturn(HASHED_ACTOR_URN_STRING);
      final JSONObject event = new JSONObject();
      event.put(EVENT_TYPE_FIELD, EVENT_TYPE);
      event.put(ACTOR_URN_FIELD, ACTOR_URN_STRING);
      event.put(NOT_ALLOWED_FIELD, NOT_ALLOWED_FIELD_VALUE);

      final JSONObject sanitizedEvent =
          enableObfuscation
              ? _trackingService.sanitizeEvent(event)
              : _noObfuscationTrackingService.sanitizeEvent(event);
      assertNotNull(sanitizedEvent);
      assertTrue(sanitizedEvent.has(APP_VERSION_FIELD));
      assertEquals(sanitizedEvent.get(APP_VERSION_FIELD), APP_VERSION);
      assertTrue(sanitizedEvent.has(EVENT_TYPE_FIELD));
      assertEquals(sanitizedEvent.get(EVENT_TYPE_FIELD), EVENT_TYPE);
      assertTrue(sanitizedEvent.has(ACTOR_URN_FIELD));
      if (enableObfuscation) {
        assertEquals(sanitizedEvent.get(ACTOR_URN_FIELD), HASHED_ACTOR_URN_STRING);
        assertFalse(sanitizedEvent.has(NOT_ALLOWED_FIELD));
      } else {
        assertEquals(sanitizedEvent.get(ACTOR_URN_FIELD), ACTOR_URN_STRING);
        assertTrue(sanitizedEvent.has(NOT_ALLOWED_FIELD));
        // Verify secretService was never called
        verify(_secretService, never()).hashString(any());
      }
    }
  }
}
