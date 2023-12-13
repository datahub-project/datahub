package com.linkedin.metadata.integration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.datahub.authentication.Authentication;
import com.linkedin.link.LinkPreviewInfo;
import com.linkedin.link.LinkPreviewType;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IntegrationsServiceTest {

  @Mock private CloseableHttpClient httpClient;

  @Mock private Authentication systemAuthentication;

  @Mock private BackoffPolicy backoffPolicy;

  private IntegrationsService integrationsService;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    integrationsService =
        new IntegrationsService(
            "localhost", 8080, false, systemAuthentication, httpClient, backoffPolicy, 3);
  }

  @Test
  public void testGetLinkPreviewSuccess() throws Exception {
    String url = "https://test.slack.com/archives/something";
    String expectedJson =
        "{\"url\":\"https://test.slack.com/archives/something\", \"testField\": 10, \"lastRefreshedMs\": 1234567890}";
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(mockResponse.getEntity()).thenReturn(entity);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    LinkPreviewInfo result = integrationsService.getLinkPreview(url);

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:8080/private/get_link_preview",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertEquals(
        String.format("{\n  \"type\" : \"SLACK_MESSAGE\",\n  \"url\" : \"%s\"\n}", url),
        EntityUtils.toString(((StringEntity) request.getEntity())));
    assertEquals(LinkPreviewType.SLACK_MESSAGE, result.getType());
    assertEquals(
        "{\"url\":\"https://test.slack.com/archives/something\",\"testField\":10,\"lastRefreshedMs\":1234567890}",
        result.getJson());
    assertEquals(1234567890L, result.getLastRefreshedMs());
  }

  @Test
  public void testGetLinkPreviewException() throws Exception {
    String url = "https://test.slack.com/archives/something";
    String expectedJson =
        "{\"url\":\"https://test.slack.com/archives/something\", \"testField\": 10, \"lastRefreshedMs\": 1234567890}";
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    when(mockResponse.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 500, "ERROR"));

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(expectedJson.getBytes(StandardCharsets.UTF_8)));
    when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    LinkPreviewInfo result = integrationsService.getLinkPreview(url);

    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);
    verify(httpClient, times(1)).execute(argument.capture());
    HttpPost request = argument.getValue();

    assertEquals(
        "localhost:8080/private/get_link_preview",
        request.getURI().getAuthority() + request.getURI().getPath());
    assertEquals(
        String.format("{\n  \"type\" : \"SLACK_MESSAGE\",\n  \"url\" : \"%s\"\n}", url),
        EntityUtils.toString(((StringEntity) request.getEntity())));
    assertNull(result);
  }
}
