package com.linkedin.gms.factory.usage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.metadata.usage.UsageTestFixtures;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.metadata.context.usage.instrumentation.CountingHttpServletResponseWrapper;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageRecordingFilterTest {

  @Test
  public void testCompleteResponseUsesContentLength() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    UsageRecordingFilter filter = new UsageRecordingFilter(enricher, true);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getRequestURI()).thenReturn("/openapi/v3/entity/dataset");
    when(request.getMethod()).thenReturn("POST");
    when(response.getHeader("Content-Length")).thenReturn("42");
    when(response.getHeader("Transfer-Encoding")).thenReturn(null);

    filter.doFilter(request, response, chain);

    ArgumentCaptor<HttpServletResponse> responseCaptor =
        ArgumentCaptor.forClass(HttpServletResponse.class);
    verify(chain).doFilter(any(), responseCaptor.capture());
    Assert.assertTrue(responseCaptor.getValue() instanceof CountingHttpServletResponseWrapper);

    ArgumentCaptor<Long> outputBytesCaptor = ArgumentCaptor.forClass(Long.class);
    verify(enricher).completeResponse(outputBytesCaptor.capture());
    Assert.assertEquals(outputBytesCaptor.getValue(), Long.valueOf(42));
  }

  @Test
  public void testCompleteResponseUsesMeasuredBytesForChunkedResponse() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    UsageRecordingFilter filter = new UsageRecordingFilter(enricher, true);

    HttpServletRequest request = mock(HttpServletRequest.class);
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getOutputStream())
        .thenReturn(
            new ServletOutputStream() {
              @Override
              public void write(int b) {
                body.write(b);
              }

              @Override
              public boolean isReady() {
                return true;
              }

              @Override
              public void setWriteListener(WriteListener writeListener) {}
            });
    when(response.getHeader("Content-Length")).thenReturn("100");
    when(response.getHeader("Transfer-Encoding")).thenReturn("chunked");

    FilterChain chain =
        (req, resp) -> resp.getOutputStream().write("hello".getBytes(StandardCharsets.UTF_8));

    filter.doFilter(request, response, chain);

    ArgumentCaptor<Long> outputBytesCaptor = ArgumentCaptor.forClass(Long.class);
    verify(enricher).completeResponse(outputBytesCaptor.capture());
    Assert.assertEquals(outputBytesCaptor.getValue(), Long.valueOf(5));
  }

  @Test
  public void testCompleteResponseNullForChunkedResponseWithoutBody() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    UsageRecordingFilter filter = new UsageRecordingFilter(enricher, true);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(request.getRequestURI()).thenReturn("/openapi/v3/entity/dataset");
    when(request.getMethod()).thenReturn("GET");
    when(response.getHeader("Content-Length")).thenReturn("100");
    when(response.getHeader("Transfer-Encoding")).thenReturn("chunked");

    filter.doFilter(request, response, chain);

    verify(enricher).completeResponse(isNull());
  }

  @Test
  public void testGraphqlStyleEarlyCompletionSkipsPositiveOutputBytes() throws Exception {
    List<Long> recordedBytes = new ArrayList<>();
    UsageAggregationStore trackingStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            recordedBytes.add(outputBytes);
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher enricher = new UsageMetricsSessionEnricher(trackingStore, true);
    OperationContext sessionContext = graphqlSession();
    enricher.onSessionReady(sessionContext);

    UsageRecordingFilter filter = new UsageRecordingFilter(enricher, true);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getRequestURI()).thenReturn("/api/graphql");
    when(request.getMethod()).thenReturn("POST");
    when(response.getHeader("Content-Length")).thenReturn("100");
    when(response.getHeader("Transfer-Encoding")).thenReturn("chunked");
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(request, response, chain);

    Assert.assertFalse(
        recordedBytes.stream().anyMatch(bytes -> bytes != null && bytes > 0),
        "GraphQL-style filter completion should not record positive output_bytes");
  }

  private static OperationContext graphqlSession() throws Exception {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.GRAPHQL)
            .requestID("smokeUsageAggregationMe")
            .userAgent("")
            .usageOperation(UsageOperation.METADATA_QUERY.key())
            .usageIdentity(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .authChannel(AuthChannel.SESSION)
            .build();
    return TestOperationContexts.systemContextNoValidate().toBuilder()
        .requestContext(requestContext)
        .build(
            new Authentication(
                new Actor(ActorType.USER, UsageTestFixtures.REGULAR_CORP_USER_ID), "test"),
            true);
  }

  @Test
  public void testDisabledFilterSkipsEnricher() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    UsageRecordingFilter filter = new UsageRecordingFilter(enricher, false);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(request, response, chain);

    verify(chain).doFilter(request, response);
    verify(enricher, never()).completeResponse(any());
  }

  @Test
  public void testClearsUsageMdcAfterRequest() throws Exception {
    UsageMetricsSessionEnricher enricher = mock(UsageMetricsSessionEnricher.class);
    UsageRecordingFilter filter = new UsageRecordingFilter(enricher, true);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);
    when(response.getHeader("Content-Length")).thenReturn(null);
    when(response.getHeader("Transfer-Encoding")).thenReturn(null);

    try (MockedStatic<MDC> mdc = Mockito.mockStatic(MDC.class)) {
      filter.doFilter(request, response, chain);

      mdc.verify(() -> MDC.remove(RequestContext.MDC_USAGE_OPERATION));
      mdc.verify(() -> MDC.remove(RequestContext.MDC_AUTH_CHANNEL));
      mdc.verify(() -> MDC.remove(RequestContext.MDC_USAGE_QUANTITY));
    }
  }
}
