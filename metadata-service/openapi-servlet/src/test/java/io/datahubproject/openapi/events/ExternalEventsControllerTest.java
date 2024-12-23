package io.datahubproject.openapi.events;

import static io.datahubproject.openapi.events.ExternalEventsController.MAX_LIMIT;
import static io.datahubproject.openapi.events.ExternalEventsController.MAX_POLL_TIMEOUT_SECONDS;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.google.common.collect.ImmutableList;
import io.acryl.event.ExternalEvents;
import io.acryl.event.ExternalEventsService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {ExternalEventsController.class, ExternalEventsControllerTest.TestConfig.class})
public class ExternalEventsControllerTest extends AbstractTestNGSpringContextTests {

  @Autowired private ExternalEventsController controller;

  @MockBean private ExternalEventsService eventsService;

  @MockBean private AuthorizerChain authorizationChain;

  @MockBean private HttpServletRequest request;

  @Mock private Authentication authentication;

  @Mock private Actor actor;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn("urn:li:corpuser:example");
    AuthenticationContext.setAuthentication(authentication);
    MockitoAnnotations.initMocks(this);

    // Mock the IP address returned by the HttpServletRequest - needed for OperationContext to be
    // instantiated.
    when(request.getRemoteAddr()).thenReturn("192.168.1.1");
  }

  @Configuration
  static class TestConfig {
    @Bean("systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization(
          TestOperationContexts.emptyActiveUsersAspectRetriever(null));
    }
  }

  @Test
  public void testPollSuccess() throws Exception {
    // Given
    String topic = "PlatformEvent_v1";
    String offsetId = "offset123";
    Integer limit = 100;
    Integer pollTimeoutSeconds = 10;
    Integer lookbackWindowDays = 5;
    String expectedOffsetId = "expectedOffsetId";
    Long expectedCount = 100L;
    List<io.acryl.event.ExternalEvent> expectedEvents =
        ImmutableList.of(new io.acryl.event.ExternalEvent("application/json", "test-event-1"));
    ExternalEvents mockedEvents =
        new ExternalEvents(expectedOffsetId, expectedCount, expectedEvents);
    when(eventsService.poll(
            eq(topic), eq(offsetId), eq(limit), eq(pollTimeoutSeconds), eq(lookbackWindowDays)))
        .thenReturn(mockedEvents);

    // When
    ResponseEntity<ExternalEventsResponse> response =
        controller.poll(request, topic, offsetId, limit, pollTimeoutSeconds, lookbackWindowDays);

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());

    // Verify interactions
    verify(eventsService)
        .poll(eq(topic), eq(offsetId), eq(limit), eq(pollTimeoutSeconds), eq(lookbackWindowDays));
  }

  @Test
  public void testPollLimitArguments() throws Exception {
    // Given
    String topic = "PlatformEvent_v1";
    String offsetId = "offset123";
    Integer limit = 1000000000;
    Integer pollTimeoutSeconds = 1000000000;
    Integer lookbackWindowDays = 5;

    String expectedOffsetId = "expectedOffsetId";
    Long expectedCount = 100L;
    List<io.acryl.event.ExternalEvent> expectedEvents =
        ImmutableList.of(new io.acryl.event.ExternalEvent("application/json", "test-event-1"));
    ExternalEvents mockedEvents =
        new ExternalEvents(expectedOffsetId, expectedCount, expectedEvents);
    when(eventsService.poll(
            eq(topic),
            eq(offsetId),
            eq(MAX_LIMIT),
            eq(MAX_POLL_TIMEOUT_SECONDS),
            eq(lookbackWindowDays)))
        .thenReturn(mockedEvents);

    // When
    ResponseEntity<ExternalEventsResponse> response =
        controller.poll(request, topic, offsetId, limit, pollTimeoutSeconds, lookbackWindowDays);

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertNotNull(response.getBody());

    // Verify that the poll method was called with the correct parameters
    verify(eventsService)
        .poll(
            eq(topic),
            eq(offsetId),
            eq(MAX_LIMIT),
            eq(MAX_POLL_TIMEOUT_SECONDS),
            eq(lookbackWindowDays));
  }
}
