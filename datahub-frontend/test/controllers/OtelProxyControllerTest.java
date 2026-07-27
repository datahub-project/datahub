package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pekko.util.ByteString;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import play.mvc.Http;
import play.mvc.Result;

public class OtelProxyControllerTest {

  private static final String COLLECTOR = "http://fake-forwarder:9999";

  // Fresh per test method (JUnit instantiates the class per test).
  private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

  private OtelProxyController controller(HttpClient httpClient, String collectorEndpoint) {
    Map<String, Object> configMap = new HashMap<>();
    if (collectorEndpoint != null) {
      configMap.put("otel.exporterOtlpEndpoint", collectorEndpoint);
    }
    Config config = ConfigFactory.parseMap(configMap);
    MetricUtils metricUtils = mock(MetricUtils.class);
    when(metricUtils.getRegistry()).thenReturn(meterRegistry);
    return new OtelProxyController(httpClient, config, metricUtils);
  }

  private Http.Request mockOtlpRequest(byte[] body) {
    return mockOtlpRequest(body, "application/x-protobuf");
  }

  private Http.Request mockOtlpRequest(byte[] body, String contentType) {
    Http.Request request = mock(Http.Request.class);
    Http.RequestBody requestBody = mock(Http.RequestBody.class);
    when(request.body()).thenReturn(requestBody);
    when(requestBody.asBytes()).thenReturn(body == null ? null : ByteString.fromArray(body));
    when(request.contentType()).thenReturn(Optional.of(contentType));
    when(request.header(Http.HeaderNames.CONTENT_ENCODING)).thenReturn(Optional.empty());
    return request;
  }

  private int statusOf(CompletableFuture<Result> future)
      throws ExecutionException, InterruptedException {
    return future.get().status();
  }

  @SuppressWarnings("unchecked")
  private HttpResponse<Void> responseWithStatus(int code) {
    HttpResponse<Void> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(code);
    return response;
  }

  private double counter(String name, String... tags) {
    return meterRegistry.counter(name, tags).count();
  }

  @Test
  public void testReturnsServiceUnavailableWhenCollectorNotConfigured() throws Exception {
    OtelProxyController controller = controller(mock(HttpClient.class), null);
    assertEquals(503, statusOf(controller.exportTraces(mockOtlpRequest(new byte[] {1, 2, 3}))));
    assertEquals(1.0, counter("otel_proxy_rejected_total", "reason", "no_endpoint"));
  }

  @Test
  public void testReturnsBadRequestForEmptyPayload() throws Exception {
    OtelProxyController controller = controller(mock(HttpClient.class), COLLECTOR);
    assertEquals(400, statusOf(controller.exportTraces(mockOtlpRequest(new byte[0]))));
    assertEquals(1.0, counter("otel_proxy_rejected_total", "reason", "empty"));
  }

  @Test
  public void testReturnsPayloadTooLargeWhenBodyExceedsCap() throws Exception {
    OtelProxyController controller = controller(mock(HttpClient.class), COLLECTOR);
    byte[] tooBig = new byte[4 * 1024 * 1024 + 1];
    assertEquals(413, statusOf(controller.exportTraces(mockOtlpRequest(tooBig))));
    assertEquals(1.0, counter("otel_proxy_rejected_total", "reason", "too_large"));
  }

  @Test
  public void testReturnsUnsupportedMediaTypeForBadContentType() throws Exception {
    OtelProxyController controller = controller(mock(HttpClient.class), COLLECTOR);
    assertEquals(
        415, statusOf(controller.exportTraces(mockOtlpRequest(new byte[] {1}, "text/html"))));
    assertEquals(1.0, counter("otel_proxy_rejected_total", "reason", "bad_type"));
  }

  @Test
  public void testForwardsAndReturnsAcceptedOnSuccess() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    doReturn(CompletableFuture.completedFuture(responseWithStatus(200)))
        .when(httpClient)
        .sendAsync(any(), any());

    OtelProxyController controller = controller(httpClient, COLLECTOR);
    assertEquals(202, statusOf(controller.exportTraces(mockOtlpRequest(new byte[] {1, 2, 3, 4}))));
    verify(httpClient, times(1)).sendAsync(any(), any());
    assertEquals(1.0, counter("otel_proxy_forward_success_total"));
  }

  @Test
  public void testPermanentUpstreamErrorIsNotRetriedAndReturns500() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    doReturn(CompletableFuture.completedFuture(responseWithStatus(400)))
        .when(httpClient)
        .sendAsync(any(), any());

    OtelProxyController controller = controller(httpClient, COLLECTOR);
    assertEquals(500, statusOf(controller.exportTraces(mockOtlpRequest(new byte[] {1, 2, 3, 4}))));
    // 400 is permanent: forwarded exactly once, no retries.
    verify(httpClient, times(1)).sendAsync(any(), any());
    assertEquals(1.0, counter("otel_proxy_forward_failure_total", "reason", "permanent"));
  }

  @Test
  public void testRetryableUpstreamStatusIsRetriedThenReported() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    doReturn(CompletableFuture.completedFuture(responseWithStatus(503)))
        .when(httpClient)
        .sendAsync(any(), any());

    OtelProxyController controller = controller(httpClient, COLLECTOR);
    // 503 is retryable → surfaced as-is so the browser exporter can retry.
    assertEquals(503, statusOf(controller.exportTraces(mockOtlpRequest(new byte[] {1, 2, 3, 4}))));
    // Default maxAttempts = 3 → 2 retries.
    verify(httpClient, times(3)).sendAsync(any(), any());
    assertEquals(2.0, counter("otel_proxy_retry_total"));
    assertEquals(1.0, counter("otel_proxy_forward_failure_total", "reason", "retryable"));
  }

  @Test
  public void testNetworkFailureRetriedThenReturnsServiceUnavailable() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    CompletableFuture<HttpResponse<Void>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new java.net.ConnectException("refused"));
    doReturn(failed).when(httpClient).sendAsync(any(), any());

    OtelProxyController controller = controller(httpClient, COLLECTOR);
    assertEquals(503, statusOf(controller.exportTraces(mockOtlpRequest(new byte[] {1, 2, 3, 4}))));
    verify(httpClient, times(3)).sendAsync(any(), any());
    assertEquals(1.0, counter("otel_proxy_forward_failure_total", "reason", "network"));
  }

  @Test
  public void testReturnsServiceUnavailableWhenOverloaded() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    // Never-completing forwards hold their permits, draining the bulkhead. Fired without get() (the
    // returned future never completes for the ones that acquire a permit).
    doReturn(new CompletableFuture<HttpResponse<Void>>()).when(httpClient).sendAsync(any(), any());

    OtelProxyController controller = controller(httpClient, COLLECTOR);
    // Well above the default in-flight limit so the bulkhead is saturated regardless of its value.
    for (int i = 0; i < 200; i++) {
      controller.exportTraces(mockOtlpRequest(new byte[] {1}));
    }

    assertEquals(503, statusOf(controller.exportTraces(mockOtlpRequest(new byte[] {1}))));
    assertTrue(counter("otel_proxy_rejected_total", "reason", "overloaded") >= 1.0);
  }

  @Test
  public void testForwardsContentEncodingHeader() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    doReturn(CompletableFuture.completedFuture(responseWithStatus(200)))
        .when(httpClient)
        .sendAsync(captor.capture(), any());

    OtelProxyController controller = controller(httpClient, COLLECTOR);
    Http.Request request = mockOtlpRequest(new byte[] {1, 2, 3});
    when(request.header(Http.HeaderNames.CONTENT_ENCODING)).thenReturn(Optional.of("gzip"));

    statusOf(controller.exportTraces(request));
    assertEquals("gzip", captor.getValue().headers().firstValue("Content-Encoding").orElse(null));
  }
}
