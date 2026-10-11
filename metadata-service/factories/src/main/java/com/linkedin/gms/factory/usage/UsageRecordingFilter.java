package com.linkedin.gms.factory.usage;

import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.instrumentation.CountingHttpServletResponseWrapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.springframework.web.filter.OncePerRequestFilter;

@RequiredArgsConstructor
public class UsageRecordingFilter extends OncePerRequestFilter {

  private final UsageMetricsSessionEnricher sessionEnricher;
  private final boolean enabled;

  @Override
  protected void doFilterInternal(
      @Nonnull HttpServletRequest request,
      @Nonnull HttpServletResponse response,
      @Nonnull FilterChain filterChain)
      throws ServletException, IOException {
    if (!enabled) {
      filterChain.doFilter(request, response);
      return;
    }

    CountingHttpServletResponseWrapper countingResponse =
        new CountingHttpServletResponseWrapper(response);
    try {
      filterChain.doFilter(request, countingResponse);
    } finally {
      try {
        sessionEnricher.completeResponse(
            RequestContext.resolveResponseOutputBytes(
                response, countingResponse.getBytesWritten()));
      } finally {
        RequestContext.clearUsageFieldsFromMdc();
      }
    }
  }
}
