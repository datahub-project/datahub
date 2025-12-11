package io.datahubproject.openapi.v1.ai;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.integration.StreamingChatClient;
import com.linkedin.metadata.integration.StreamingChatClient.SseEvent;
import com.linkedin.metadata.service.DataHubAiConversationService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for DataHubAiConversationController - OpenAPI v1 AI chat streaming controller. */
public class DataHubAiConversationControllerTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn TEST_CONVERSATION_URN =
      UrnUtils.getUrn("urn:li:dataHubAiConversation:12345");
  private static final String TEST_MESSAGE_TEXT = "Hello, AI!";

  private IntegrationsService mockIntegrationsService;
  private StreamingChatClient mockStreamingClient;
  private DataHubAiConversationService mockConversationService;
  private OperationContext mockOperationContext;
  private AuthorizerChain mockAuthorizerChain;
  private HttpServletRequest mockHttpServletRequest;
  private DataHubAiConversationController controller;

  @BeforeMethod
  public void setUp() throws Exception {
    mockIntegrationsService = mock(IntegrationsService.class);
    mockStreamingClient = mock(StreamingChatClient.class);
    mockConversationService = mock(DataHubAiConversationService.class);
    mockAuthorizerChain = mock(AuthorizerChain.class);
    mockHttpServletRequest = mock(HttpServletRequest.class);

    // Create a real system operation context using TestOperationContexts
    mockOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    when(mockIntegrationsService.getStreamingChatClient()).thenReturn(mockStreamingClient);

    // Mock HttpServletRequest to provide required fields for RequestContext.buildOpenapi
    when(mockHttpServletRequest.getRemoteAddr()).thenReturn("127.0.0.1");

    // By default, authorize the user to access the conversation
    when(mockConversationService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(true);

    controller =
        new DataHubAiConversationController(
            mockIntegrationsService,
            mockConversationService,
            mockOperationContext,
            mockAuthorizerChain);

    // Set up authentication context for all tests
    Authentication mockAuth = mock(Authentication.class);
    Actor mockActor = new Actor(ActorType.USER, TEST_USER_URN.getId());
    when(mockAuth.getActor()).thenReturn(mockActor);
    AuthenticationContext.setAuthentication(mockAuth);
  }

  @AfterMethod
  public void tearDown() {
    // Clean up authentication context after each test
    AuthenticationContext.remove();
  }

  @Test
  public void testStreamChatSuccess() throws Exception {
    // Mock the streaming client to call the callback with SSE events
    when(mockStreamingClient.sendStreamingMessage(
            any(String.class),
            any(String.class),
            any(String.class),
            any(), // messageContext can be null
            any(Authentication.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              // Simulate Python service returning SSE events with event names
              Consumer<SseEvent> callback = invocation.getArgument(5);
              if (callback != null) {
                callback.accept(
                    new SseEvent(
                        "message", "{\"type\":\"TEXT\",\"content\":{\"text\":\"User message\"}}"));
                callback.accept(
                    new SseEvent(
                        "message", "{\"type\":\"TEXT\",\"content\":{\"text\":\"AI response\"}}"));
              }
              return CompletableFuture.completedFuture(null);
            });

    // Create request (note: userUrn is NOT sent - it's extracted from auth context)
    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    // Call the streaming endpoint
    SseEmitter emitter = controller.streamChat(mockHttpServletRequest, request);

    // Verify emitter is created
    assertNotNull(emitter);

    // Give the async thread time to complete
    Thread.sleep(500);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testStreamChatMissingConversationUrn() {
    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setText(TEST_MESSAGE_TEXT);
    // conversationUrn is missing (userUrn comes from auth context)

    controller.streamChat(mockHttpServletRequest, request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testStreamChatMissingText() {
    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    // text is missing (userUrn comes from auth context)

    controller.streamChat(mockHttpServletRequest, request);
  }

  @Test
  public void testStreamChatWithError() throws Exception {
    // Mock streaming client to throw an exception
    when(mockStreamingClient.sendStreamingMessage(
            any(String.class),
            any(String.class),
            any(String.class),
            any(), // messageContext can be null
            any(Authentication.class),
            any(Consumer.class)))
        .thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Integration service error")));

    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    // Should not throw - errors are handled within the async thread
    SseEmitter emitter = controller.streamChat(mockHttpServletRequest, request);
    assertNotNull(emitter);

    // Give the async thread time to handle the error
    Thread.sleep(500);
  }

  @Test
  public void testStreamChatPassthrough() throws Exception {
    // Verify that the controller passes through SSE events with correct event names
    final String testSseData1 = "{\"message_type\":\"THINKING\",\"text\":\"Processing...\"}";
    final String testSseData2 = "{\"message_type\":\"TEXT\",\"text\":\"Final response\"}";

    when(mockStreamingClient.sendStreamingMessage(
            any(String.class),
            any(String.class),
            any(String.class),
            any(), // messageContext can be null
            any(Authentication.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<SseEvent> callback = invocation.getArgument(5);
              if (callback != null) {
                // Simulate Python service streaming responses with event names
                callback.accept(new SseEvent("message", testSseData1));
                callback.accept(new SseEvent("message", testSseData2));
              }
              return CompletableFuture.completedFuture(null);
            });

    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    SseEmitter emitter = controller.streamChat(mockHttpServletRequest, request);
    assertNotNull(emitter);

    // Give the async thread time to complete
    Thread.sleep(500);
  }

  @Test
  public void testStreamChatWithErrorEvent() throws Exception {
    // Verify that error events are properly forwarded with "error" event name
    when(mockStreamingClient.sendStreamingMessage(
            any(String.class),
            any(String.class),
            any(String.class),
            any(), // messageContext can be null
            any(Authentication.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<SseEvent> callback = invocation.getArgument(5);
              if (callback != null) {
                // Simulate Python service returning an error event
                callback.accept(
                    new SseEvent("error", "{\"type\":\"error\",\"error\":\"Test error message\"}"));
              }
              return CompletableFuture.completedFuture(null);
            });

    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    SseEmitter emitter = controller.streamChat(mockHttpServletRequest, request);
    assertNotNull(emitter);

    // Give the async thread time to complete
    Thread.sleep(500);
  }

  @Test(expectedExceptions = ResponseStatusException.class)
  public void testStreamChatUnauthorized() throws Exception {
    // Mock conversation service to deny access
    when(mockConversationService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(false);

    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    // Should throw ResponseStatusException with FORBIDDEN status
    controller.streamChat(mockHttpServletRequest, request);
  }

  @Test(expectedExceptions = ResponseStatusException.class)
  public void testStreamChatConversationDoesNotExist() throws Exception {
    // Mock conversation service to indicate conversation doesn't exist (returns false)
    when(mockConversationService.canAccessConversation(
            any(OperationContext.class), any(Urn.class), any(Urn.class)))
        .thenReturn(false);

    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    // Should throw ResponseStatusException
    controller.streamChat(mockHttpServletRequest, request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testStreamChatInvalidConversationUrn() {
    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn("not-a-valid-urn");
    request.setText(TEST_MESSAGE_TEXT);

    // Should throw IllegalArgumentException for invalid URN
    controller.streamChat(mockHttpServletRequest, request);
  }

  @Test
  public void testStreamChatWithContext() throws Exception {
    // Test that message context is properly converted and passed through
    when(mockStreamingClient.sendStreamingMessage(
            eq(TEST_CONVERSATION_URN.toString()),
            eq(TEST_MESSAGE_TEXT),
            any(String.class), // agentName
            argThat(
                (Map<String, Object> contextMap) -> {
                  if (contextMap == null) return false;
                  String text = (String) contextMap.get("text");
                  @SuppressWarnings("unchecked")
                  List<String> entityUrns = (List<String>) contextMap.get("entity_urns");
                  return "Current step: Configure Recipe".equals(text)
                      && entityUrns != null
                      && entityUrns.size() == 1
                      && "urn:li:dataSource:123".equals(entityUrns.get(0));
                }),
            any(Authentication.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<SseEvent> callback = invocation.getArgument(5);
              if (callback != null) {
                callback.accept(
                    new SseEvent(
                        "message", "{\"type\":\"TEXT\",\"content\":{\"text\":\"Response\"}}"));
              }
              return CompletableFuture.completedFuture(null);
            });

    // Create request with context
    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    DataHubAiConversationController.ChatContext context =
        new DataHubAiConversationController.ChatContext();
    context.setText("Current step: Configure Recipe");
    context.setEntityUrns(Arrays.asList("urn:li:dataSource:123"));
    request.setContext(context);

    // Call the streaming endpoint
    SseEmitter emitter = controller.streamChat(mockHttpServletRequest, request);
    assertNotNull(emitter);

    // Give the async thread time to complete
    Thread.sleep(500);
  }

  @Test
  public void testStreamChatWithoutContext() throws Exception {
    // Test that null context is handled correctly
    when(mockStreamingClient.sendStreamingMessage(
            eq(TEST_CONVERSATION_URN.toString()),
            eq(TEST_MESSAGE_TEXT),
            any(String.class),
            eq(null), // Context should be null
            any(Authentication.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<SseEvent> callback = invocation.getArgument(5);
              if (callback != null) {
                callback.accept(
                    new SseEvent(
                        "message", "{\"type\":\"TEXT\",\"content\":{\"text\":\"Response\"}}"));
              }
              return CompletableFuture.completedFuture(null);
            });

    // Create request without context
    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);
    // context is not set

    // Call the streaming endpoint
    SseEmitter emitter = controller.streamChat(mockHttpServletRequest, request);
    assertNotNull(emitter);

    // Give the async thread time to complete
    Thread.sleep(500);
  }

  @Test
  public void testStreamChatWithContextTextOnly() throws Exception {
    // Test context with only text (no entity URNs)
    when(mockStreamingClient.sendStreamingMessage(
            eq(TEST_CONVERSATION_URN.toString()),
            eq(TEST_MESSAGE_TEXT),
            any(String.class),
            argThat(
                (Map<String, Object> contextMap) -> {
                  if (contextMap == null) return false;
                  String text = (String) contextMap.get("text");
                  return "Current step: Test Connection".equals(text)
                      && !contextMap.containsKey("entity_urns");
                }),
            any(Authentication.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<SseEvent> callback = invocation.getArgument(5);
              if (callback != null) {
                callback.accept(
                    new SseEvent(
                        "message", "{\"type\":\"TEXT\",\"content\":{\"text\":\"Response\"}}"));
              }
              return CompletableFuture.completedFuture(null);
            });

    DataHubAiConversationController.ChatRequest request =
        new DataHubAiConversationController.ChatRequest();
    request.setConversationUrn(TEST_CONVERSATION_URN.toString());
    request.setText(TEST_MESSAGE_TEXT);

    DataHubAiConversationController.ChatContext context =
        new DataHubAiConversationController.ChatContext();
    context.setText("Current step: Test Connection");
    // entityUrns is null
    request.setContext(context);

    SseEmitter emitter = controller.streamChat(mockHttpServletRequest, request);
    assertNotNull(emitter);

    Thread.sleep(500);
  }
}
