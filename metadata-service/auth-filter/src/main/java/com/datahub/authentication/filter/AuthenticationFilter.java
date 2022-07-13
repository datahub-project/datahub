package com.datahub.authentication.filter;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorConfiguration;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.authenticator.AuthenticatorChain;
import com.datahub.authentication.authenticator.DataHubSystemAuthenticator;
import com.datahub.authentication.authenticator.NoOpAuthenticator;
import com.datahub.authentication.token.StatefulTokenService;
import com.google.common.collect.ImmutableMap;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import static com.datahub.authentication.AuthenticationConstants.*;


/**
 * A servlet {@link Filter} for authenticating requests inbound to the Metadata Service. This filter is applied to the
 * GraphQL Servlet, the Rest.li Servlet, and the Auth (token) Servlet.
 */
@Slf4j
public class AuthenticationFilter implements Filter {

  @Inject
  private ConfigurationProvider configurationProvider;

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("dataHubTokenService")
  private StatefulTokenService _tokenService;

  private AuthenticatorChain authenticatorChain;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
    buildAuthenticatorChain();
  }

  @Override
  public void doFilter(
      ServletRequest request,
      ServletResponse response,
      FilterChain chain)
      throws IOException, ServletException {
    AuthenticationRequest context = buildAuthContext((HttpServletRequest) request);
    Authentication authentication = null;
    try {
      authentication = this.authenticatorChain.authenticate(context);
    } catch (AuthenticationException e) {
      // For AuthenticationExpiredExceptions, terminate and provide that feedback to the user
      log.debug("Failed to authenticate request. Received an AuthenticationExpiredException from authenticator chain.", e);
      ((HttpServletResponse) response).sendError(HttpServletResponse.SC_UNAUTHORIZED, e.getMessage());
      return;
    }

    if (authentication != null) {
      // Successfully authenticated.
      log.debug(String.format("Successfully authenticated request for Actor with type: %s, id: %s",
          authentication.getActor().getType(),
          authentication.getActor().getId()));
      AuthenticationContext.setAuthentication(authentication);
      chain.doFilter(request, response);
    } else {
      // Reject request
      log.debug("Failed to authenticate request. Received 'null' Authentication value from authenticator chain.");
      ((HttpServletResponse) response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized to perform this action.");
      return;
    }
    AuthenticationContext.remove();
  }

  @Override
  public void destroy() {
    // Nothing
  }

  /**
   * Constructs an {@link AuthenticatorChain} via the provided {@link AuthenticationConfiguration}.
   *
   * The process is simple: For each configured {@link Authenticator}, attempt to instantiate the class using a default (zero-arg)
   * constructor, then call it's initialize method passing in a freeform block of associated configurations as a {@link Map}. Finally,
   * register the {@link Authenticator} in the authenticator chain.
   */
  private void buildAuthenticatorChain() {

    authenticatorChain = new AuthenticatorChain();

    boolean isAuthEnabled = this.configurationProvider.getAuthentication().isEnabled();

    // Create authentication context object to pass to authenticator instances. They can use it as needed.
    final AuthenticatorContext authenticatorContext = new AuthenticatorContext(ImmutableMap.of(
        ENTITY_SERVICE,
        this._entityService,
        TOKEN_SERVICE,
        this._tokenService
    ));

    if (isAuthEnabled) {
      log.info("Auth is enabled. Building authenticator chain...");

      // First register the required system authenticator
      DataHubSystemAuthenticator systemAuthenticator = new DataHubSystemAuthenticator();
      systemAuthenticator.init(ImmutableMap.of(
          SYSTEM_CLIENT_ID_CONFIG, this.configurationProvider.getAuthentication().getSystemClientId(),
          SYSTEM_CLIENT_SECRET_CONFIG, this.configurationProvider.getAuthentication().getSystemClientSecret()
      ), authenticatorContext);
      authenticatorChain.register(systemAuthenticator); // Always register authenticator for internal system.

      // Then create a list of authenticators based on provided configs.
      final List<AuthenticatorConfiguration> authenticatorConfigurations = this.configurationProvider.getAuthentication().getAuthenticators();

      for (AuthenticatorConfiguration config : authenticatorConfigurations) {
        final String type = config.getType();
        final Map<String, Object> configs = config.getConfigs();

        log.debug(String.format("Found configs for Authenticator of type %s: %s ", type, configs));

        // Instantiate the Authenticator class.
        Class<? extends Authenticator> clazz = null;
        try {
          clazz = (Class<? extends Authenticator>) Class.forName(type);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(
              String.format("Failed to find Authenticator class with name %s on the classpath.", type));
        }

        // Ensure class conforms to the correct type.
        if (!Authenticator.class.isAssignableFrom(clazz)) {
          throw new IllegalArgumentException(
              String.format(
                  "Failed to instantiate invalid Authenticator with class name %s. Class does not implement the 'Authenticator' interface",
                  clazz.getCanonicalName()));
        }

        // Else construct an instance of the class, each class should have an empty constructor.
        try {
          final Authenticator authenticator = clazz.newInstance();
          // Successfully created authenticator. Now init and register it.
          log.debug(String.format("Initializing Authenticator with name %s", type));
            authenticator.init(configs, authenticatorContext);
          log.info(String.format("Registering Authenticator with name %s", type));
          authenticatorChain.register(authenticator);
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to instantiate Authenticator with class name %s", clazz.getCanonicalName()), e);
        }
      }
    } else {
      // Authentication is not enabled. Populate authenticator chain with a purposely permissive Authenticator.
      log.info("Auth is disabled. Building no-op authenticator chain...");
      final NoOpAuthenticator noOpAuthenticator = new NoOpAuthenticator();
      noOpAuthenticator.init(ImmutableMap.of(
          SYSTEM_CLIENT_ID_CONFIG,
          this.configurationProvider.getAuthentication().getSystemClientId()), authenticatorContext);
      authenticatorChain.register(noOpAuthenticator);
    }
  }

  private AuthenticationRequest buildAuthContext(HttpServletRequest request) {
    return new AuthenticationRequest(Collections.list(request.getHeaderNames())
        .stream()
        .collect(Collectors.toMap(headerName -> headerName, request::getHeader)));
  }
}
