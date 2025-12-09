package com.linkedin.metadata.integration;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Simplified streaming client for chat interactions with the Python integrations service.
 *
 * <p>This client acts as a passthrough, forwarding raw SSE event data from the Python service to
 * the callback without parsing or transforming it. The Python service handles both message
 * persistence and AI response generation, returning formatted SSE events.
 */
@Slf4j
public class StreamingChatClient {

  private static final String STREAMING_MESSAGE_ENDPOINT = "/private/api/chat/message";
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final String ACCEPT_SSE = "text/event-stream";
  private static final String CACHE_CONTROL_NO_CACHE = "no-cache";
  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String BEARER_PREFIX = "Bearer ";
  private static final String DATA_PREFIX = "data: ";
  private static final String EVENT_PREFIX = "event: ";
  private static final String COMPLETE_EVENT_TYPE = "complete";
  private static final String ERROR_EVENT_TYPE = "error";

  private final CloseableHttpClient httpClient;
  private final String chatServiceUrl;
  private final ObjectMapper objectMapper;
  private final ExecutorService executorService;

  public StreamingChatClient(
      @Nonnull final CloseableHttpClient httpClient, @Nonnull final String chatServiceUrl) {
    this.httpClient = httpClient;
    this.chatServiceUrl = chatServiceUrl;
    this.objectMapper = new ObjectMapper();
    this.executorService = Executors.newCachedThreadPool();
  }

  /** Simple event holder for SSE events with event name and data. */
  public static class SseEvent {
    private final String eventName;
    private final String data;

    public SseEvent(String eventName, String data) {
      this.eventName = eventName;
      this.data = data;
    }

    public String getEventName() {
      return eventName;
    }

    public String getData() {
      return data;
    }
  }

  /**
   * Send a streaming chat message with user authentication and forward SSE events to the callback.
   *
   * <p>The Python service handles message persistence and returns SSE events that are forwarded to
   * the callback with both event name and data preserved.
   *
   * <p>User authentication is required and forwarded to the Python integrations service in the
   * Authorization header. The integrations service uses these credentials for all operations
   * including conversation management and tool calls, ensuring proper authorization throughout.
   *
   * @param conversationUrn The conversation URN
   * @param messageText The message text
   * @param authentication The user's authentication object containing actor and credentials
   * @param progressCallback Callback that receives SSE events with event name and data
   * @return CompletableFuture that completes when streaming is done
   */
  public CompletableFuture<Void> sendStreamingMessage(
      @Nonnull final String conversationUrn,
      @Nonnull final String messageText,
      @Nullable final String agentName,
      @Nonnull final Authentication authentication,
      @Nullable final Consumer<SseEvent> progressCallback) {

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // Extract user URN and credentials from authentication
            final String userUrn = authentication.getActor().toUrnStr();
            final String userCredentials = authentication.getCredentials();

            // Send the message to the Python service via streaming endpoint
            final HttpPost messageRequest =
                new HttpPost(chatServiceUrl + STREAMING_MESSAGE_ENDPOINT);
            messageRequest.setHeader("Content-Type", CONTENT_TYPE_JSON);
            messageRequest.setHeader("Accept", ACCEPT_SSE);
            messageRequest.setHeader("Cache-Control", CACHE_CONTROL_NO_CACHE);

            // Forward user credentials in Authorization header
            // User credentials might already have Bearer prefix
            String authValue =
                userCredentials.startsWith(BEARER_PREFIX)
                    ? userCredentials
                    : BEARER_PREFIX + userCredentials;
            messageRequest.setHeader(AUTHORIZATION_HEADER, authValue);
            log.debug(
                "Forwarding user credentials for chat request to conversation: {}, user: {}",
                conversationUrn,
                userUrn);

            // Build request body
            final Map<String, String> requestBody = new HashMap<>();
            requestBody.put("conversation_urn", conversationUrn);
            requestBody.put("user_urn", userUrn);
            requestBody.put("text", messageText);
            if (agentName != null) {
              requestBody.put("agent_name", agentName);
            }
            final String requestJson = objectMapper.writeValueAsString(requestBody);
            messageRequest.setEntity(new StringEntity(requestJson, StandardCharsets.UTF_8));

            // Execute message request
            try (CloseableHttpResponse messageResponse = httpClient.execute(messageRequest)) {
              if (messageResponse.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException(
                    "Failed to send message to chat service: "
                        + messageResponse.getStatusLine().getStatusCode());
              }

              // Forward the SSE stream
              forwardSSEStream(messageResponse, progressCallback);
            }

            return null;

          } catch (Exception e) {
            log.error("Failed to send streaming chat message", e);
            throw new RuntimeException("Failed to send streaming chat message", e);
          }
        },
        executorService);
  }

  /**
   * Forward SSE events from the Python service to the callback.
   *
   * <p>This reads the SSE stream and forwards both event name and data payload to the callback,
   * preserving the event type information so the controller can use the appropriate SSE event name.
   */
  private void forwardSSEStream(
      @Nonnull final CloseableHttpResponse response,
      @Nullable final Consumer<SseEvent> progressCallback)
      throws IOException {

    final HttpEntity entity = response.getEntity();
    if (entity == null) {
      throw new RuntimeException("No response entity from chat service");
    }

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {

      String line;
      String currentEventName = "message"; // Default event name if not specified
      StringBuilder dataBuffer = new StringBuilder();
      boolean shouldStopStreaming = false;

      while ((line = reader.readLine()) != null && !shouldStopStreaming) {
        if (line.isEmpty()) {
          // Empty line signifies end of an event
          if (dataBuffer.length() > 0 && progressCallback != null) {
            // Forward event with both name and data
            String jsonData = dataBuffer.toString();
            log.debug(
                "Forwarding SSE event: name={}, data={}",
                currentEventName,
                jsonData.substring(0, Math.min(200, jsonData.length())));
            progressCallback.accept(new SseEvent(currentEventName, jsonData));
            dataBuffer.setLength(0);
          }

          // Check if we should stop streaming after this event
          if (COMPLETE_EVENT_TYPE.equals(currentEventName)
              || ERROR_EVENT_TYPE.equals(currentEventName)) {
            log.info("Received stream termination event: {}", currentEventName);
            shouldStopStreaming = true;
          }

          // Reset to default for next event
          currentEventName = "message";
          continue;
        }

        if (line.startsWith(EVENT_PREFIX)) {
          currentEventName = line.substring(EVENT_PREFIX.length()).trim();
          log.debug("SSE event type: {}", currentEventName);
        } else if (line.startsWith(DATA_PREFIX)) {
          // Accumulate data lines (there may be multiple data lines per event)
          if (dataBuffer.length() > 0) {
            dataBuffer.append("\n");
          }
          dataBuffer.append(line.substring(DATA_PREFIX.length()));
        }
      }

      // Forward any remaining data before termination
      if (dataBuffer.length() > 0 && progressCallback != null) {
        progressCallback.accept(new SseEvent(currentEventName, dataBuffer.toString()));
      }

      log.info("SSE stream consumption completed");
    }
  }

  /** Shutdown the executor service when done. */
  public void shutdown() {
    executorService.shutdown();
  }
}
