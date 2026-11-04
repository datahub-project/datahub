package com.linkedin.metadata.search.embedding;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AiGatewayEmbeddingProviderTest {

  @Mock private HttpClient mockHttpClient;

  @Mock private HttpResponse<String> mockTokenResponse;

  @Mock private HttpResponse<String> mockEmbedResponse;

  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testEmbedSuccess() throws Exception {
    String tokenJson = "{\"access_token\": \"fake-token\", \"expires_in\": 3600}";
    when(mockTokenResponse.statusCode()).thenReturn(200);
    when(mockTokenResponse.body()).thenReturn(tokenJson);

    String embedJson = "{\"embeddings\": [[0.1, 0.2, 0.3]]}";
    when(mockEmbedResponse.statusCode()).thenReturn(200);
    when(mockEmbedResponse.body()).thenReturn(embedJson);

    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockTokenResponse)
        .thenReturn(mockEmbedResponse);

    AiGatewayEmbeddingProvider provider =
        new AiGatewayEmbeddingProvider(
            "http://fake-gateway",
            "test-platform",
            "test-model",
            "http://fake-token-url",
            "client-id",
            "client-secret",
            0,
            mockHttpClient);

    float[] result = provider.embed("test text", null);

    assertNotNull(result);
    assertEquals(result.length, 3);
    assertEquals(result[0], 0.1f, 0.001);
    assertEquals(result[1], 0.2f, 0.001);
    assertEquals(result[2], 0.3f, 0.001);

    verify(mockHttpClient, times(2))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }
}
