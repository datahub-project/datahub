package com.datahub.auth.authentication.filter;

import static com.datahub.authentication.AuthenticationConstants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.authenticator.AuthenticatorChain;
import com.datahub.authentication.authenticator.DataHubSystemAuthenticator;
import com.datahub.authentication.authenticator.DataHubTokenAuthenticator;
import com.datahub.authentication.authenticator.HealthStatusAuthenticator;
import com.datahub.authentication.authenticator.NoOpAuthenticator;
import com.datahub.authentication.token.StatefulTokenService;
import com.google.common.collect.ImmutableMap;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import jakarta.inject.Named;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Phase 1 Authentication Filter - Authentication Extraction
 *
 * <p>This filter ALWAYS runs and NEVER fails. It attempts to extract authentication information
 * from incoming requests and sets the appropriate authentication context:
 *
 * <p>- If valid authentication is found → Sets authenticated user context - If no/invalid
 * authentication → Sets anonymous/guest context
 *
 * <p>This provides universal authentication context for all requests, enabling controllers to
 * implement progressive disclosure based on user authorization levels.
 *
 * <p>Order: HIGHEST_PRECEDENCE - runs before all other authentication filters
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class AuthenticationExtractionFilter extends OncePerRequestFilter {

  // Constants for anonymous authentication
  private static final String ANONYMOUS_ACTOR_ID = "anonymous";
  private static final String ANONYMOUS_CREDENTIALS = "";

  @Autowired private ConfigurationProvider configurationProvider;

  @Autowired
  @Named("entityService")
  private EntityService<?> _entityService;

  @Autowired
  @Named("dataHubTokenService")
  private StatefulTokenService _tokenService;

  @Value("#{new Boolean('${authentication.logAuthenticatorExceptions}')}")
  private boolean _logAuthenticatorExceptions;

  private AuthenticatorChain authenticatorChain;

  @PostConstruct
  public void init() {
    buildAuthenticatorChain();
    log.info("AuthenticationExtractionFilter initialized.");
  }

  /**
   * Builds a minimal authenticator chain for extraction purposes only. This is a simplified version
   * of the full authenticator chain used in AuthenticationFilter.
   */
  private void buildAuthenticatorChain() {
    authenticatorChain = new AuthenticatorChain();

    boolean isAuthEnabled = this.configurationProvider.getAuthentication().isEnabled();

    // Create authentication context object to pass to authenticator instances
    final AuthenticatorContext authenticatorContext =
        new AuthenticatorContext(
            ImmutableMap.of(
                ENTITY_SERVICE, this._entityService, TOKEN_SERVICE, this._tokenService));

    if (isAuthEnabled) {
      log.info("Auth is enabled. Building extraction authenticator chain...");

      // Register system authenticator
      DataHubSystemAuthenticator systemAuthenticator = new DataHubSystemAuthenticator();
      systemAuthenticator.init(
          ImmutableMap.of(
              SYSTEM_CLIENT_ID_CONFIG,
              this.configurationProvider.getAuthentication().getSystemClientId(),
              SYSTEM_CLIENT_SECRET_CONFIG,
              this.configurationProvider.getAuthentication().getSystemClientSecret()),
          authenticatorContext);
      authenticatorChain.register(systemAuthenticator);

      // Register token authenticator for JWT tokens
      DataHubTokenAuthenticator tokenAuthenticator = new DataHubTokenAuthenticator();
      tokenAuthenticator.init(
          ImmutableMap.of(
              DataHubTokenAuthenticator.SIGNING_KEY_CONFIG_NAME,
              this.configurationProvider.getAuthentication().getTokenService().getSigningKey(),
              DataHubTokenAuthenticator.SALT_CONFIG_NAME,
              this.configurationProvider.getAuthentication().getTokenService().getSalt()),
          authenticatorContext);
      authenticatorChain.register(tokenAuthenticator);

      // Add health status authenticator for system health endpoints
      HealthStatusAuthenticator healthAuthenticator = new HealthStatusAuthenticator();
      healthAuthenticator.init(
          ImmutableMap.of(
              SYSTEM_CLIENT_ID_CONFIG,
              this.configurationProvider.getAuthentication().getSystemClientId()),
          authenticatorContext);
      authenticatorChain.register(healthAuthenticator);

    } else {
      // Authentication is not enabled. Use no-op authenticator.
      log.info("Auth is disabled. Building no-op extraction authenticator chain...");
      final NoOpAuthenticator noOpAuthenticator = new NoOpAuthenticator();
      noOpAuthenticator.init(
          ImmutableMap.of(
              SYSTEM_CLIENT_ID_CONFIG,
              this.configurationProvider.getAuthentication().getSystemClientId()),
          authenticatorContext);
      authenticatorChain.register(noOpAuthenticator);
    }
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {

    // Build authentication context from request
    AuthenticationRequest authRequest = buildAuthContext(request);
    Authentication authentication = null;

    try {
      // Attempt to authenticate the request
      authentication = authenticatorChain.authenticate(authRequest, false);

      if (authentication != null) {
        log.debug(
            "Successfully extracted authentication for actor: {} (type: {})",
            authentication.getActor().getId(),
            authentication.getActor().getType());
      }

    } catch (Exception e) {
      // Authentication failed - this is expected and handled gracefully
      log.debug("Authentication extraction failed, will set anonymous context: {}", e.getMessage());
    }

    // Set authentication context - either authenticated user or anonymous
    if (authentication != null) {
      AuthenticationContext.setAuthentication(authentication);
    } else {
      // Set anonymous authentication context
      Authentication anonymousAuth = createAnonymousAuthentication();
      AuthenticationContext.setAuthentication(anonymousAuth);
      log.debug("Set anonymous authentication context for unauthenticated request");
    }

    try {
      // Always proceed to next filter/controller
      chain.doFilter(request, response);
    } finally {
      // Clean up authentication context after request processing
      AuthenticationContext.remove();
    }
  }

  /**
   * Creates an anonymous authentication context for unauthenticated requests. This allows
   * controllers to detect unauthenticated users and provide appropriate responses.
   *
   * @return Anonymous authentication with USER actor type
   */
  private Authentication createAnonymousAuthentication() {
    Actor anonymousActor = new Actor(ActorType.USER, ANONYMOUS_ACTOR_ID);
    return new Authentication(anonymousActor, ANONYMOUS_CREDENTIALS, Collections.emptyMap());
  }

  /**
   * Builds authentication request context from HTTP request. Extracts path info and headers needed
   * for authentication.
   *
   * @param request HTTP servlet request
   * @return AuthenticationRequest with request context
   */
  private AuthenticationRequest buildAuthContext(HttpServletRequest request) {
    return new AuthenticationRequest(
        request.getServletPath(),
        request.getPathInfo(),
        Collections.list(request.getHeaderNames()).stream()
            .collect(Collectors.toMap(headerName -> headerName, request::getHeader)));
  }

  /**
   * This filter should run for ALL requests - no exclusions. The existing AuthenticationFilter will
   * handle exclusions based on configuration.
   *
   * @param request HTTP servlet request
   * @return false - never skip this filter
   */
  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    return false; // Always run this filter
  }
}
