package com.linkedin.gms.factory.clientwrite;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Counts accepted metadata writes by the client that sent them: a write is counted once the server
 * has answered it with a status below 400. Requests rejected by authorization or validation, failed
 * with a server error, or whose handler threw are not counted.
 *
 * <p>Shadow mode only: every request continues unchanged, and a failure here is logged (rate
 * limited) and ignored. Must be ordered after the authentication extraction filter so the actor is
 * known.
 */
@RequiredArgsConstructor
public class ClientWriteTrackingFilter extends OncePerRequestFilter {

  @Nonnull private final ClientWriteRecorder recorder;
  @Nonnull private final String systemActorUrn;
  private final ClientWriteTrackingErrors errors =
      new ClientWriteTrackingErrors(ClientWriteTrackingFilter.class.getSimpleName());

  @Override
  protected void doFilterInternal(
      @Nonnull HttpServletRequest request,
      @Nonnull HttpServletResponse response,
      @Nonnull FilterChain filterChain)
      throws ServletException, IOException {
    PendingWrite pending = null;
    try {
      pending = classify(request);
    } catch (RuntimeException e) {
      errors.report(e);
    }
    // An exception from the chain propagates unchanged and the write is not counted.
    filterChain.doFilter(request, response);
    if (pending == null) {
      return;
    }
    try {
      if (request.isAsyncStarted()) {
        // The status is only final once the async handler completes.
        request.getAsyncContext().addListener(new RecordOnComplete(pending, response));
      } else {
        recordIfAccepted(pending, response);
      }
    } catch (RuntimeException e) {
      errors.report(e);
    }
  }

  @Nullable
  private PendingWrite classify(@Nonnull HttpServletRequest request) {
    Optional<ClientWrite> write =
        ClientWriteClassifier.classify(
            request.getMethod(),
            pathWithinContext(request),
            request.getQueryString(),
            request::getHeader);
    if (write.isEmpty()) {
      return null;
    }
    String actorUrn = currentActorUrn();
    return new PendingWrite(write.get(), actorUrn, systemActorUrn.equals(actorUrn));
  }

  private void recordIfAccepted(
      @Nonnull PendingWrite pending, @Nonnull HttpServletResponse response) {
    if (response.getStatus() < 400) {
      recorder.record(pending.write, pending.actorUrn, pending.systemActor);
    }
  }

  /**
   * The servlet path and path info, which never include the context path. Spring Boot serves the
   * GMS base path as the context path, so the result does not depend on base path settings.
   */
  @Nonnull
  static String pathWithinContext(@Nonnull HttpServletRequest request) {
    String servletPath = request.getServletPath();
    String pathInfo = request.getPathInfo();
    return (servletPath == null ? "" : servletPath) + (pathInfo == null ? "" : pathInfo);
  }

  @Nullable
  private static String currentActorUrn() {
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null || authentication.getActor() == null) {
      return null;
    }
    return authentication.getActor().toUrnStr();
  }

  @RequiredArgsConstructor
  private static final class PendingWrite {
    @Nonnull final ClientWrite write;
    @Nullable final String actorUrn;
    final boolean systemActor;
  }

  @RequiredArgsConstructor
  private final class RecordOnComplete implements AsyncListener {
    @Nonnull private final PendingWrite pending;
    @Nonnull private final HttpServletResponse response;

    @Override
    public void onComplete(AsyncEvent event) {
      try {
        recordIfAccepted(pending, response);
      } catch (RuntimeException e) {
        errors.report(e);
      }
    }

    @Override
    public void onTimeout(AsyncEvent event) {}

    @Override
    public void onError(AsyncEvent event) {}

    @Override
    public void onStartAsync(AsyncEvent event) {}
  }
}
