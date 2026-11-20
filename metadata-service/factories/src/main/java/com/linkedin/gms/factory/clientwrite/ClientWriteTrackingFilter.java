package com.linkedin.gms.factory.clientwrite;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.linkedin.metadata.utils.BasePathUtils;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Counts metadata writes by the client that sent them. Shadow mode only: every request continues
 * unchanged, and a failure here is logged and ignored. Must be ordered after the authentication
 * extraction filter so the actor is known.
 */
@Slf4j
@RequiredArgsConstructor
public class ClientWriteTrackingFilter extends OncePerRequestFilter {

  @Nonnull private final ClientWriteRecorder recorder;
  @Nullable private final String basePath;
  @Nonnull private final String systemActorUrn;

  @Override
  protected void doFilterInternal(
      @Nonnull HttpServletRequest request,
      @Nonnull HttpServletResponse response,
      @Nonnull FilterChain filterChain)
      throws ServletException, IOException {
    try {
      track(request);
    } catch (RuntimeException e) {
      log.debug("Client write tracking failed; the request continues unaffected", e);
    }
    filterChain.doFilter(request, response);
  }

  private void track(@Nonnull HttpServletRequest request) {
    String path = BasePathUtils.stripBasePath(request.getRequestURI(), basePath);
    ClientWriteClassifier.classify(
            request.getMethod(), path, request.getQueryString(), request::getHeader)
        .ifPresent(
            write -> {
              String actorUrn = currentActorUrn();
              recorder.record(write, actorUrn, systemActorUrn.equals(actorUrn));
            });
  }

  @Nullable
  private static String currentActorUrn() {
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null || authentication.getActor() == null) {
      return null;
    }
    return authentication.getActor().toUrnStr();
  }
}
