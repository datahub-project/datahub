package io.datahubproject.openapi.analytics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TrackingControllerTest {

  @Mock private TrackingService trackingService;

  private OperationContext systemOperationContext;

  @Mock private AuthorizerChain authorizerChain;

  @Mock private HttpServletRequest request;

  @Mock private Authentication authentication;

  private TrackingController controller;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "test_user"));
    when(request.getRemoteAddr()).thenReturn("");
    systemOperationContext = TestOperationContexts.systemContextNoValidate();
    controller = new TrackingController(trackingService, systemOperationContext, authorizerChain);
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testTrackEvent() throws Exception {
    // Create a test event
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");
    event.put("entityType", "dataset");
    event.put("entityUrn", "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)");
    event.put("actorUrn", "urn:li:corpuser:test_user");
    event.put("customField", "test_value");

    // Mock authentication
    AuthenticationContext.setAuthentication(authentication);

    // Call the endpoint
    ResponseEntity<Void> response = controller.trackEvent(request, event);

    // Verify response
    assertEquals(response.getStatusCode().value(), 200);

    // Verify tracking service was called with the event
    verify(trackingService)
        .track(eq("TestEvent"), any(OperationContext.class), eq(null), eq(null), eq(event));
  }

  @Test
  public void testTrackEventWithTelemetryDisabled() throws Exception {
    // Create a test event
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");
    event.put("entityType", "dataset");
    event.put("entityUrn", "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)");
    event.put("actorUrn", "urn:li:corpuser:test_user");
    event.put("customField", "test_value");

    // Mock authentication
    AuthenticationContext.setAuthentication(authentication);

    // Create a controller with telemetry disabled
    TrackingService disabledTrackingService =
        new TrackingService(null, null, null, null, null, null, null);
    TrackingController disabledController =
        new TrackingController(disabledTrackingService, systemOperationContext, authorizerChain);

    // Call the endpoint
    ResponseEntity<Void> response = disabledController.trackEvent(request, event);

    // Verify response is still successful even with telemetry disabled
    assertEquals(response.getStatusCode().value(), 200);
  }

  @Test
  public void testTrackEventWithoutAuthentication() throws Exception {
    // Create a test event
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");

    // Clear authentication
    AuthenticationContext.setAuthentication(null);

    // Call the endpoint
    ResponseEntity<Void> response = controller.trackEvent(request, event);

    // Verify response is unauthorized
    assertEquals(response.getStatusCode().value(), 401);
  }

  @Test
  public void testTrackEventWithInvalidEvent() throws Exception {
    // Create an invalid event (missing type)
    ObjectNode event = objectMapper.createObjectNode();
    event.put("entityType", "dataset");

    // Mock authentication
    AuthenticationContext.setAuthentication(authentication);

    // Call the endpoint
    ResponseEntity<Void> response = controller.trackEvent(request, event);

    // Verify response is bad request
    assertEquals(response.getStatusCode().value(), 400);
  }

  @Test
  public void testTrackEventFallsBackToAcceptLanguageHeader() throws Exception {
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");
    when(request.getHeader("Accept-Language")).thenReturn("de-DE,de;q=0.9,en-US;q=0.8");

    AuthenticationContext.setAuthentication(authentication);

    ResponseEntity<Void> response = controller.trackEvent(request, event);

    assertEquals(response.getStatusCode().value(), 200);
    assertEquals(event.get("language").asText(), "de-DE");
  }

  @Test
  public void testTrackEventPrefersClientSuppliedLanguage() throws Exception {
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");
    event.put("language", "ja");
    when(request.getHeader("Accept-Language")).thenReturn("de-DE,de;q=0.9");

    AuthenticationContext.setAuthentication(authentication);

    controller.trackEvent(request, event);

    assertEquals(event.get("language").asText(), "ja");
  }

  @Test
  public void testTrackEventWithoutUsableAcceptLanguageHeader() throws Exception {
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");
    when(request.getHeader("Accept-Language")).thenReturn(null);

    AuthenticationContext.setAuthentication(authentication);

    ResponseEntity<Void> response = controller.trackEvent(request, event);

    assertEquals(response.getStatusCode().value(), 200);
    assertFalse(event.has("language"));
  }

  @Test
  public void testParsePreferredLanguage() {
    // Highest q-value wins regardless of position, and canonical casing is restored.
    assertEquals(
        TrackingController.parsePreferredLanguage("en;q=0.5,pt-br;q=0.9"), "pt-BR", "q-ordering");
    assertEquals(TrackingController.parsePreferredLanguage("fr-CA"), "fr-CA", "single tag");
    // A wildcard expresses no locale, so fall through to the next usable range.
    assertEquals(TrackingController.parsePreferredLanguage("*,sv;q=0.5"), "sv", "wildcard skipped");
    assertNull(TrackingController.parsePreferredLanguage("*"), "wildcard only");
    assertNull(TrackingController.parsePreferredLanguage(null), "absent header");
    assertNull(TrackingController.parsePreferredLanguage("   "), "blank header");
    assertNull(TrackingController.parsePreferredLanguage("de;q=not-a-number"), "malformed header");
  }
}
