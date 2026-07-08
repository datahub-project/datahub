package com.linkedin.gms.factory.usage;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Tags SCIMple servlet requests for usage aggregation. SCIM HTTP is handled by Apache SCIMple
 * rather than DataHub controllers, so path/method heuristics open a session here.
 */
@RequiredArgsConstructor
public class ScimUsageSessionFilter extends OncePerRequestFilter {

  static final String SCIM_PATH_PREFIX = "/openapi/scim/v2";

  private final OperationContext systemOperationContext;
  private final UsageMetricsSessionEnricher usageMetricsSessionEnricher;

  @Override
  protected void doFilterInternal(
      @Nonnull HttpServletRequest request,
      @Nonnull HttpServletResponse response,
      @Nonnull FilterChain filterChain)
      throws ServletException, IOException {
    if (shouldTag(request)) {
      openUsageSession(request);
    }
    filterChain.doFilter(request, response);
  }

  private static boolean shouldTag(HttpServletRequest request) {
    String uri = request.getRequestURI();
    return uri != null && uri.startsWith(SCIM_PATH_PREFIX);
  }

  private void openUsageSession(HttpServletRequest request) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null || authentication.getActor() == null) {
      return;
    }

    UsageOperation usageOperation =
        isReadMethod(request.getMethod()) ? UsageOperation.OTHER_READ : UsageOperation.OTHER_WRITE;
    String operation = "scim" + request.getMethod() + request.getRequestURI();

    usageMetricsSessionEnricher.recordTaggedServletRequest(
        systemOperationContext,
        RequestContext.builder()
            .buildOpenapi(authentication.getActor().toUrnStr(), request, operation, List.of())
            .withUsageOperation(usageOperation),
        authentication);
  }

  private static boolean isReadMethod(String method) {
    return "GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method);
  }
}
