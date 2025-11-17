package com.linkedin.metadata.integration;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for StreamingChatClient - the simplified passthrough client. */
public class StreamingChatClientTest {

  private static final String TEST_CHAT_SERVICE_URL = "http://localhost:8080";
  private static final String TEST_USER_CREDENTIALS = "test-user-token-12345";
  private static final String TEST_CONVERSATION_URN = "urn:li:dataHubAiConversation:test123";
  private static final String TEST_USER_URN = "urn:li:corpuser:testUser";
  private static final String TEST_MESSAGE_TEXT = "Hello, AI!";

  private CloseableHttpClient mockHttpClient;
  private StreamingChatClient streamingChatClient;
  private Authentication testAuthentication;

  @BeforeMethod
  public void setUp() {
    mockHttpClient = mock(CloseableHttpClient.class);
    streamingChatClient = new StreamingChatClient(mockHttpClient, TEST_CHAT_SERVICE_URL);
    testAuthentication =
        new Authentication(new Actor(ActorType.USER, TEST_USER_URN), TEST_USER_CREDENTIALS);
  }

  @Test
  public void testConstructor() {
    // Test that constructor properly initializes fields
    assertNotNull(streamingChatClient);
  }

  @Test
  public void testSendStreamingMessageSuccess() throws Exception {
    // Mock response with SSE stream
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.getEntity()).thenReturn(mockEntity);

    // Simulate SSE stream
    String sseStream =
        "event: message\n"
            + "data: {\"type\":\"TEXT\",\"content\":{\"text\":\"User message\"},\"actor\":{\"type\":\"USER\"}}\n"
            + "\n"
            + "event: message\n"
            + "data: {\"type\":\"TEXT\",\"content\":{\"text\":\"AI response\"},\"actor\":{\"type\":\"AGENT\"}}\n"
            + "\n"
            + "event: complete\n"
            + "data: \n"
            + "\n";

    InputStream inputStream = new ByteArrayInputStream(sseStream.getBytes(StandardCharsets.UTF_8));
    when(mockEntity.getContent()).thenReturn(inputStream);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Capture forwarded events
    List<StreamingChatClient.SseEvent> forwardedEvents = new ArrayList<>();
    Consumer<StreamingChatClient.SseEvent> callback = forwardedEvents::add;

    // Execute
    CompletableFuture<Void> future =
        streamingChatClient.sendStreamingMessage(
            TEST_CONVERSATION_URN, TEST_MESSAGE_TEXT, testAuthentication, callback);

    // Wait for completion
    future.get();

    // Verify we forwarded the SSE data with correct event names
    assertEquals(forwardedEvents.size(), 2); // Two message events
    assertEquals(forwardedEvents.get(0).getEventName(), "message");
    assertTrue(forwardedEvents.get(0).getData().contains("User message"));
    assertEquals(forwardedEvents.get(1).getEventName(), "message");
    assertTrue(forwardedEvents.get(1).getData().contains("AI response"));
  }

  @Test
  public void testSendStreamingMessageWithoutCallback() throws Exception {
    // Mock response
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.getEntity()).thenReturn(mockEntity);

    String sseStream =
        "event: message\ndata: {\"text\":\"response\"}\n\nevent: complete\ndata: \n\n";
    InputStream inputStream = new ByteArrayInputStream(sseStream.getBytes(StandardCharsets.UTF_8));
    when(mockEntity.getContent()).thenReturn(inputStream);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute without callback (should not throw)
    CompletableFuture<Void> future =
        streamingChatClient.sendStreamingMessage(
            TEST_CONVERSATION_URN, TEST_MESSAGE_TEXT, testAuthentication, null);

    future.get(); // Should complete successfully
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testSendStreamingMessageHttpError() throws Exception {
    // Mock HTTP error response
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);

    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute - should throw
    CompletableFuture<Void> future =
        streamingChatClient.sendStreamingMessage(
            TEST_CONVERSATION_URN, TEST_MESSAGE_TEXT, testAuthentication, null);

    try {
      future.get();
    } catch (Exception e) {
      // Unwrap ExecutionException
      throw (RuntimeException) e.getCause();
    }
  }

  @Test
  public void testShutdown() {
    // Should not throw
    streamingChatClient.shutdown();
  }

  @Test
  public void testMultilineSSEData() throws Exception {
    // Test handling of multi-line SSE data fields
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.getEntity()).thenReturn(mockEntity);

    // Multi-line data in SSE
    String sseStream =
        "event: message\n"
            + "data: line1\n"
            + "data: line2\n"
            + "data: line3\n"
            + "\n"
            + "event: complete\n"
            + "data: \n"
            + "\n";

    InputStream inputStream = new ByteArrayInputStream(sseStream.getBytes(StandardCharsets.UTF_8));
    when(mockEntity.getContent()).thenReturn(inputStream);

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    List<StreamingChatClient.SseEvent> forwardedEvents = new ArrayList<>();
    Consumer<StreamingChatClient.SseEvent> callback = forwardedEvents::add;

    CompletableFuture<Void> future =
        streamingChatClient.sendStreamingMessage(
            TEST_CONVERSATION_URN, TEST_MESSAGE_TEXT, testAuthentication, callback);

    future.get();

    // Should concatenate multi-line data with newlines
    assertEquals(forwardedEvents.size(), 1);
    assertEquals(forwardedEvents.get(0).getEventName(), "message");
    assertTrue(forwardedEvents.get(0).getData().contains("line1\nline2\nline3"));
  }
}
