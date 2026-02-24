package io.datahubproject.openapi.v1.ai;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.billing.BillingHandler;
import com.linkedin.metadata.billing.BillingProduct;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.integration.StreamingChatClient;
import com.linkedin.metadata.service.DataHubAiConversationService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.RateLimitExceededException;
import io.datahubproject.openapi.exception.UnauthorizedException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * REST controller for streaming chat completions via Server-Sent Events (SSE).
 *
 * <p>This endpoint acts as a simple passthrough to the Python integrations service, which handles
 * both message persistence and AI response generation. The Python service saves messages directly
 * to DataHub and returns structured SSE events that this controller forwards to the client.
 */
@RestController
@RequestMapping("/openapi/v1/ai-chat")
@Slf4j
public class DataHubAiConversationController {

  private static final String STREAM_ENDPOINT = "/message";
  private static final String MESSAGE_EVENT_NAME = "message";
  private static final String COMPLETE_EVENT_NAME = "complete";
  private static final String ERROR_EVENT_NAME = "error";
  private static final String COMMENT_EVENT_NAME = "__comment__";
  private static final String CONVERSATION_URN_REQUIRED_ERROR =
      "conversationUrn and text are required";

  private final IntegrationsService integrationsService;
  private final DataHubAiConversationService conversationService;
  private final OperationContext systemOperationContext;
  private final AuthorizerChain authorizerChain;

  @Nullable private final BillingHandler billingHandler;

  @Nullable private final ConfigurationProvider configProvider;

  @Autowired
  public DataHubAiConversationController(
      IntegrationsService integrationsService,
      DataHubAiConversationService conversationService,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      AuthorizerChain authorizerChain,
      @Autowired(required = false) @Qualifier("billingHandler") BillingHandler billingHandler,
      @Autowired(required = false) @Qualifier("configurationProvider")
          ConfigurationProvider configProvider) {
    this.integrationsService = integrationsService;
    this.conversationService = conversationService;
    this.systemOperationContext = systemOperationContext;
    this.authorizerChain = authorizerChain;
    this.billingHandler = billingHandler;
    this.configProvider = configProvider;
  }

  @Data
  public static class ChatRequest {

    private String conversationUrn;
    private String text;
    private String agentName;
    private String viewUrn;
    private ChatContext context;
  }

  @Data
  public static class ChatContext {

    private String text;
    private List<String> entityUrns;
  }

  /**
   * Stream chat completions via Server-Sent Events.
   *
   * <p>This endpoint acts as a simple passthrough: the Python integrations service handles message
   * persistence and returns SSE events that we forward directly to the client without parsing or
   * modification.
   *
   * <p><strong>Security Note:</strong> The user URN is extracted from the authenticated session
   * context and is NOT accepted from the client request to prevent impersonation attacks.
   *
   * <p><strong>Authentication Forwarding:</strong> The user's complete authentication object
   * (containing actor and credentials) is forwarded to the Python integrations service. This allows
   * the integrations service to make authenticated calls back to DataHub (e.g., for search
   * operations, conversation management) on behalf of the user, ensuring proper authorization
   * throughout the request lifecycle.
   *
   * @param request The chat request containing conversation URN and text
   * @return SSE emitter that streams the AI response
   */
  @PostMapping(value = STREAM_ENDPOINT, produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter streamChat(
      HttpServletRequest httpServletRequest, @RequestBody ChatRequest request) {
    log.debug("Received chat stream request for conversation: {}", request.getConversationUrn());

    // Extract authenticated user from context - do NOT trust client-provided userUrn
    Authentication authentication = AuthenticationContext.getAuthentication();
    String authenticatedUserUrn = authentication.getActor().toUrnStr();
    Urn userUrn = UrnUtils.getUrn(authenticatedUserUrn);

    // Build user-scoped operation context for authorization checks
    OperationContext userOpContext =
        buildOperationContext(authentication, httpServletRequest, "streamChat");

    // Validate request
    if (request.getConversationUrn() == null || request.getText() == null) {
      throw new IllegalArgumentException(CONVERSATION_URN_REQUIRED_ERROR);
    }

    // Parse conversation URN
    Urn conversationUrn;
    try {
      conversationUrn = UrnUtils.getUrn(request.getConversationUrn());
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid conversation URN: " + request.getConversationUrn());
    }

    // Check authorization - user must be the creator of the conversation
    if (!isAuthorizedToSendMessage(userOpContext, conversationUrn, userUrn)) {
      log.warn(
          "User {} attempted to send message to conversation {} but is not authorized",
          authenticatedUserUrn,
          request.getConversationUrn());
      throw new UnauthorizedException(
          "You are not authorized to send messages to this conversation. Only the conversation creator can send messages.");
    }

    // Check usage limit
    checkUsageLimit();

    log.debug(
        "Processing chat request for user: {}, conversation: {}",
        authenticatedUserUrn,
        request.getConversationUrn());

    // Create SSE emitter with 30 minute timeout
    SseEmitter emitter = new SseEmitter(30 * 60 * 1000L);

    // Process asynchronously
    new Thread(
            () -> {
              try {
                // Stream response from Python integrations service
                // Python handles both message persistence and AI response generation
                StreamingChatClient streamingClient = integrationsService.getStreamingChatClient();

                // Convert ChatContext to Map for Python service
                Map<String, Object> contextMap = null;
                if (request.getContext() != null) {
                  contextMap = new HashMap<>();
                  contextMap.put("text", request.getContext().getText());
                  if (request.getContext().getEntityUrns() != null) {
                    contextMap.put("entity_urns", request.getContext().getEntityUrns());
                  }
                }

                streamingClient
                    .sendStreamingMessage(
                        request.getConversationUrn(),
                        request.getText(),
                        request.getAgentName(),
                        request.getViewUrn(),
                        contextMap,
                        authentication, // Forward user's authentication to integrations service
                        sseEvent -> {
                          try {
                            // Handle SSE comments (keepalives) separately
                            if (COMMENT_EVENT_NAME.equals(sseEvent.getEventName())) {
                              // Forward as SSE comment to keep connection alive
                              log.debug(
                                  "Forwarding SSE comment: {}, conversation={}",
                                  sseEvent.getData(),
                                  request.getConversationUrn());
                              emitter.send(SseEmitter.event().comment(sseEvent.getData()));
                            } else {
                              // Forward SSE event with proper event name from Python service
                              // Python sends: event: message/error/complete\ndata: {...}
                              // We preserve the event name to maintain error/completion semantics
                              log.debug(
                                  "Forwarding SSE event: name={}, conversation={}",
                                  sseEvent.getEventName(),
                                  request.getConversationUrn());
                              emitter.send(
                                  SseEmitter.event()
                                      .name(sseEvent.getEventName())
                                      .data(sseEvent.getData()));
                            }
                          } catch (IOException e) {
                            log.error("Failed to forward SSE event", e);
                            emitter.completeWithError(e);
                          } catch (Exception e) {
                            log.error("Unexpected error in streaming callback", e);
                            emitter.completeWithError(e);
                          }
                        })
                    .get(); // Wait for the streaming to complete

                // Complete the stream
                emitter.send(SseEmitter.event().name(COMPLETE_EVENT_NAME).data(""));
                emitter.complete();

                // Report usage after successful completion
                reportUsage(request.getConversationUrn(), authenticatedUserUrn);

                log.debug(
                    "Chat stream completed for conversation: {}", request.getConversationUrn());
              } catch (Exception e) {
                log.error(
                    "Failed to stream chat for conversation: {}", request.getConversationUrn(), e);
                try {
                  emitter.send(SseEmitter.event().name(ERROR_EVENT_NAME).data(e.getMessage()));
                } catch (IOException ioException) {
                  log.error("Failed to send error event", ioException);
                }
                emitter.completeWithError(e);
              }
            })
        .start();

    return emitter;
  }

  /**
   * Checks if a user is authorized to send a message to a conversation.
   *
   * <p>A user is authorized if they are the creator of the conversation. This prevents users from
   * sending messages to conversations they don't own.
   *
   * @param opContext the user's operation context (must be user-scoped, not system context)
   * @param conversationUrn the conversation URN
   * @param userUrn the user URN
   * @return true if authorized, false otherwise
   */
  private boolean isAuthorizedToSendMessage(
      OperationContext opContext, Urn conversationUrn, Urn userUrn) {
    try {
      return conversationService.canAccessConversation(opContext, conversationUrn, userUrn);
    } catch (Exception e) {
      log.error("Error checking conversation access for user {}: {}", userUrn, e.getMessage(), e);
      return false;
    }
  }

  /**
   * Builds a user-scoped OperationContext for the authenticated user.
   *
   * <p>This ensures authorization checks use the actual user's identity rather than the system
   * context, preventing unauthorized access to other users' resources.
   */
  private OperationContext buildOperationContext(
      Authentication authentication, HttpServletRequest request, String actionName) {
    return OperationContext.asSession(
        systemOperationContext,
        RequestContext.builder()
            .buildOpenapi(authentication.getActor().toUrnStr(), request, actionName, List.of()),
        authorizerChain,
        authentication,
        true);
  }

  /**
   * Check usage limit for Ask DataHub.
   *
   * <p>This method is called before processing an AI chat request. It checks if the instance has
   * remaining credits in the billing system (Metronome) for the Ask DataHub product. If the limit
   * is exhausted, it throws a RateLimitExceededException which results in a 429 Too Many Requests
   * response.
   *
   * @throws RateLimitExceededException if usage limit is exceeded
   */
  private void checkUsageLimit() {
    // Check if billing handler is available
    if (billingHandler == null || !billingHandler.isEnabled()) {
      return;
    }

    // Check if credits remain for the product
    if (!billingHandler.hasRemainingCredits(BillingProduct.ASK_DATAHUB)) {
      log.warn("Usage limit exceeded for Ask DataHub");
      throw new RateLimitExceededException(
          "Oops! You've exceeded your monthly account limit for Ask DataHub. Reach out to the DataHub team to upgrade your plan.");
    }
  }

  /**
   * Report usage after successful AI chat completion.
   *
   * <p>This method is called after an AI chat response is successfully generated and streamed to
   * the client. It reports the usage to the billing system (Metronome) to decrement the customer's
   * credit balance.
   *
   * @param conversationUrn The conversation URN (used to generate transaction ID)
   * @param userUrn The user URN who made the request
   */
  private void reportUsage(String conversationUrn, String userUrn) {
    // Check if billing handler is available
    if (billingHandler == null || !billingHandler.isEnabled()) {
      return;
    }

    // Generate unique transaction ID
    String transactionId = conversationUrn + "_" + System.currentTimeMillis();

    try {
      // Build properties map
      java.util.Map<String, Object> properties = new java.util.HashMap<>();
      properties.put("conversation_id", conversationUrn);
      if (userUrn != null && !userUrn.isEmpty()) {
        properties.put("user_id", userUrn);
      }

      billingHandler.reportUsage(
          BillingHandler.EVENT_TYPE_AI_MESSAGE, transactionId, 1, properties);
    } catch (Exception e) {
      log.error(
          "Failed to report usage for conversation: {}, transaction: {}",
          conversationUrn,
          transactionId,
          e);
    }
  }
}
