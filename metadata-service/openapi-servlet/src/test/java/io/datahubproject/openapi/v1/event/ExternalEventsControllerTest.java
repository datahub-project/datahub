package io.datahubproject.openapi.v1.event;

import static io.datahubproject.event.ExternalEventsService.PLATFORM_EVENT_TOPIC_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.event.ExternalEventsService;
import io.datahubproject.event.models.v1.ExternalEvent;
import io.datahubproject.event.models.v1.ExternalEvents;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.TraceContext;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v1.event"})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  ExternalEventsControllerTest.ExternalEventsControllerTestConfig.class
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class ExternalEventsControllerTest extends AbstractTestNGSpringContextTests {
  @Autowired private ExternalEventsController externalEventsController;
  @Autowired private MockMvc mockMvc;
  @Autowired private ExternalEventsService mockEventsService;
  @Autowired private AuthorizerChain mockAuthorizerChain;
  @Autowired private OperationContext opContext;
  @MockBean private ConfigurationProvider configurationProvider;

  private static final String ACTOR_URN = "urn:li:corpuser:testuser";
  private static final String TEST_OFFSET_ID = "test-offset-id";
  private static final String TEST_CONTENT = "{\"event\":\"test\"}";
  private static final String TEST_CONTENT_TYPE = "application/json";

  @BeforeMethod
  public void setup() {
    // Reset mocks and setup common behavior for each test
    Authentication authentication = mock(Authentication.class);
    Actor actor = new Actor(ActorType.USER, "testuser");
    when(authentication.getActor()).thenReturn(actor);
    AuthenticationContext.setAuthentication(authentication);
  }

  @Test
  public void initTest() {
    assertNotNull(externalEventsController);
  }

  @Test
  public void testPollWithAuthorization() throws Exception {
    // Setup mock authorization
    when(mockAuthorizerChain.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    // Setup mock response
    List<ExternalEvent> events = new ArrayList<>();
    ExternalEvent event = new ExternalEvent();
    event.setValue(TEST_CONTENT);
    event.setContentType(TEST_CONTENT_TYPE);
    events.add(event);

    ExternalEvents externalEvents = new ExternalEvents();
    externalEvents.setEvents(events);
    externalEvents.setOffsetId(TEST_OFFSET_ID);
    externalEvents.setCount(1L);

    when(mockEventsService.poll(
            eq(PLATFORM_EVENT_TOPIC_NAME),
            nullable(String.class),
            anyInt(),
            anyInt(),
            nullable(Integer.class)))
        .thenReturn(externalEvents);

    // Execute test
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v1/events/poll")
                .param("topic", PLATFORM_EVENT_TOPIC_NAME)
                .param("limit", "100")
                .param("pollTimeoutSeconds", "10")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.count").value(1))
        .andExpect(MockMvcResultMatchers.jsonPath("$.offsetId").value(TEST_OFFSET_ID))
        .andExpect(MockMvcResultMatchers.jsonPath("$.events[0].value").value(TEST_CONTENT))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.events[0].contentType").value(TEST_CONTENT_TYPE));
  }

  @Test
  public void testPollWithOffset() throws Exception {
    // Setup mock authorization
    when(mockAuthorizerChain.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    // Setup mock response
    List<ExternalEvent> events = new ArrayList<>();
    ExternalEvent event = new ExternalEvent();
    event.setValue(TEST_CONTENT);
    event.setContentType(TEST_CONTENT_TYPE);
    events.add(event);

    ExternalEvents externalEvents = new ExternalEvents();
    externalEvents.setEvents(events);
    externalEvents.setOffsetId("new-offset-id");
    externalEvents.setCount(1L);

    when(mockEventsService.poll(
            eq(PLATFORM_EVENT_TOPIC_NAME),
            eq(TEST_OFFSET_ID),
            anyInt(),
            anyInt(),
            nullable(Integer.class)))
        .thenReturn(externalEvents);

    // Execute test
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v1/events/poll")
                .param("topic", PLATFORM_EVENT_TOPIC_NAME)
                .param("offsetId", TEST_OFFSET_ID)
                .param("limit", "100")
                .param("pollTimeoutSeconds", "10")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.count").value(1))
        .andExpect(MockMvcResultMatchers.jsonPath("$.offsetId").value("new-offset-id"));
  }

  @Test
  public void testPollWithLookbackWindow() throws Exception {
    // Setup mock authorization
    when(mockAuthorizerChain.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    // Setup mock response
    List<ExternalEvent> events = new ArrayList<>();
    ExternalEvent event = new ExternalEvent();
    event.setValue(TEST_CONTENT);
    event.setContentType(TEST_CONTENT_TYPE);
    events.add(event);

    ExternalEvents externalEvents = new ExternalEvents();
    externalEvents.setEvents(events);
    externalEvents.setOffsetId(TEST_OFFSET_ID);
    externalEvents.setCount(1L);

    when(mockEventsService.poll(
            eq(PLATFORM_EVENT_TOPIC_NAME), nullable(String.class), anyInt(), anyInt(), eq(7)))
        .thenReturn(externalEvents);

    // Execute test
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v1/events/poll")
                .param("topic", PLATFORM_EVENT_TOPIC_NAME)
                .param("lookbackWindowDays", "7")
                .param("limit", "100")
                .param("pollTimeoutSeconds", "10")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.count").value(1))
        .andExpect(MockMvcResultMatchers.jsonPath("$.offsetId").value(TEST_OFFSET_ID));
  }

  @Test
  public void testPollExceedingMaxLimit() throws Exception {
    // Setup mock authorization
    when(mockAuthorizerChain.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    // Setup mock response
    List<ExternalEvent> events = new ArrayList<>();
    ExternalEvents externalEvents = new ExternalEvents();
    externalEvents.setEvents(events);
    externalEvents.setOffsetId(TEST_OFFSET_ID);
    externalEvents.setCount(0L);

    // Verify the limit is capped at MAX_LIMIT (5000)
    when(mockEventsService.poll(
            eq(PLATFORM_EVENT_TOPIC_NAME),
            nullable(String.class),
            eq(5000), // This should be capped at 5000
            anyInt(),
            nullable(Integer.class)))
        .thenReturn(externalEvents);

    // Execute test with a limit exceeding MAX_LIMIT
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v1/events/poll")
                .param("topic", PLATFORM_EVENT_TOPIC_NAME)
                .param("limit", "10000") // Exceeds MAX_LIMIT
                .param("pollTimeoutSeconds", "10")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.count").value(0))
        .andExpect(MockMvcResultMatchers.jsonPath("$.offsetId").value(TEST_OFFSET_ID));
  }

  @Test
  public void testPollExceedingMaxPollTimeout() throws Exception {
    // Setup mock authorization
    when(mockAuthorizerChain.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    // Setup mock response
    List<ExternalEvent> events = new ArrayList<>();
    ExternalEvents externalEvents = new ExternalEvents();
    externalEvents.setEvents(events);
    externalEvents.setOffsetId(TEST_OFFSET_ID);
    externalEvents.setCount(0L);

    // Verify the timeout is capped at MAX_POLL_TIMEOUT_SECONDS (60)
    when(mockEventsService.poll(
            eq(PLATFORM_EVENT_TOPIC_NAME),
            nullable(String.class),
            anyInt(),
            eq(60), // This should be capped at 60
            nullable(Integer.class)))
        .thenReturn(externalEvents);

    // Execute test with a timeout exceeding MAX_POLL_TIMEOUT_SECONDS
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v1/events/poll")
                .param("topic", PLATFORM_EVENT_TOPIC_NAME)
                .param("limit", "100")
                .param("pollTimeoutSeconds", "120") // Exceeds MAX_POLL_TIMEOUT_SECONDS
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(MockMvcResultMatchers.jsonPath("$.count").value(0))
        .andExpect(MockMvcResultMatchers.jsonPath("$.offsetId").value(TEST_OFFSET_ID));
  }

  @Test
  public void testPollWithUnsupportedTopic() throws Exception {
    // Setup mock authorization
    when(mockAuthorizerChain.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    // Execute test with an unsupported topic
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/openapi/v1/events/poll")
                .param("topic", "UnsupportedTopic")
                .param("limit", "100")
                .param("pollTimeoutSeconds", "10")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isForbidden());
  }

  @TestConfiguration
  public static class ExternalEventsControllerTestConfig {
    @MockBean public ExternalEventsService eventsService;
    @MockBean public AuthorizerChain authorizerChain;
    @MockBean public TraceContext traceContext;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean("entityRegistry")
    @Primary
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") final OperationContext testOperationContext) {
      return testOperationContext.getEntityRegistry();
    }

    @Bean
    @Primary
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);

      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "testuser"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);

      return authorizerChain;
    }
  }
}
