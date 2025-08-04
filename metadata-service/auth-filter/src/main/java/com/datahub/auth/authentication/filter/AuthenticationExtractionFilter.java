package com.datahub.auth.authentication.filter;

import static com.datahub.authentication.AuthenticationConstants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorConfiguration;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.authenticator.*;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.plugins.PluginConstant;
import com.datahub.plugins.auth.authentication.Authenticator;
import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginPermissionManager;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.SecurityMode;
import com.datahub.plugins.configuration.Config;
import com.datahub.plugins.configuration.ConfigProvider;
import com.datahub.plugins.factory.PluginConfigFactory;
import com.datahub.plugins.loader.IsolatedClassLoader;
import com.datahub.plugins.loader.PluginPermissionManagerImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import jakarta.inject.Named;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  public static final String ANONYMOUS_ACTOR_ID = "anonymous";
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
   * Constructs an {@link AuthenticatorChain} via the provided {@link AuthenticationConfiguration}.
   *
   * <p>The process is simple: For each configured {@link Authenticator}, attempt to instantiate the
   * class using a default (zero-arg) constructor, then call it's initialize method passing in a
   * freeform block of associated configurations as a {@link Map}. Finally, register the {@link
   * Authenticator} in the authenticator chain.
   */
  private void buildAuthenticatorChain() {

    authenticatorChain = new AuthenticatorChain();

    boolean isAuthEnabled = this.configurationProvider.getAuthentication().isEnabled();

    // Create authentication context object to pass to authenticator instances. They can use it as
    // needed.
    final AuthenticatorContext authenticatorContext =
        new AuthenticatorContext(
            ImmutableMap.of(
                ENTITY_SERVICE, this._entityService, TOKEN_SERVICE, this._tokenService));

    if (isAuthEnabled) {
      log.info("Auth is enabled. Building extraction authenticator chain...");
      this.registerNativeAuthenticator(
          authenticatorChain, authenticatorContext); // Register native authenticators
      this.registerPlugins(authenticatorChain); // Register plugin authenticators
    } else {
      // Authentication is not enabled. Populate authenticator chain with a purposely permissive
      // Authenticator.
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
  @VisibleForTesting
  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    return false; // Always run this filter
  }

  private void registerPlugins(AuthenticatorChain authenticatorChain) {
    // TODO: Introduce plugin factory to reduce duplicate code around authentication and
    // authorization processing

    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    Path pluginBaseDirectory =
        Paths.get(configurationProvider.getDatahub().getPlugin().getAuth().getPath());
    Optional<Config> optionalConfig = (new ConfigProvider(pluginBaseDirectory)).load();
    optionalConfig.ifPresent(
        (config) -> {
          log.info(
              "Processing authenticator plugin from auth plugin directory {}", pluginBaseDirectory);
          PluginConfigFactory authenticatorPluginPluginConfigFactory =
              new PluginConfigFactory(config);

          List<PluginConfig> authorizers =
              authenticatorPluginPluginConfigFactory.loadPluginConfigs(PluginType.AUTHENTICATOR);
          // Filter enabled authenticator plugins
          List<PluginConfig> enabledAuthenticators =
              authorizers.stream()
                  .filter(
                      pluginConfig -> {
                        if (!pluginConfig.getEnabled()) {
                          log.info(
                              String.format(
                                  "Authenticator %s is not enabled", pluginConfig.getName()));
                        }
                        return pluginConfig.getEnabled();
                      })
                  .collect(Collectors.toList());

          SecurityMode securityMode =
              SecurityMode.valueOf(
                  this.configurationProvider.getDatahub().getPlugin().getPluginSecurityMode());
          // Create permission manager with security mode
          PluginPermissionManager permissionManager = new PluginPermissionManagerImpl(securityMode);

          // Initiate Authenticators
          enabledAuthenticators.forEach(
              (pluginConfig) -> {
                IsolatedClassLoader isolatedClassLoader =
                    new IsolatedClassLoader(permissionManager, pluginConfig);
                // Create context
                AuthenticatorContext context =
                    new AuthenticatorContext(
                        ImmutableMap.of(
                            PluginConstant.PLUGIN_HOME,
                            pluginConfig.getPluginHomeDirectory().toString()));

                try {
                  Thread.currentThread().setContextClassLoader((ClassLoader) isolatedClassLoader);
                  Authenticator authenticator =
                      (Authenticator) isolatedClassLoader.instantiatePlugin(Authenticator.class);
                  log.info("Initializing plugin {}", pluginConfig.getName());
                  authenticator.init(
                      pluginConfig.getConfigs().orElse(Collections.emptyMap()), context);
                  authenticatorChain.register(authenticator);
                  log.info("Plugin {} is initialized", pluginConfig.getName());
                } catch (ClassNotFoundException e) {
                  throw new RuntimeException(
                      String.format("Plugin className %s not found", pluginConfig.getClassName()),
                      e);
                } finally {
                  Thread.currentThread().setContextClassLoader(contextClassLoader);
                }
              });
        });
  }

  private void registerNativeAuthenticator(
      AuthenticatorChain authenticatorChain, AuthenticatorContext authenticatorContext) {
    log.info("Registering native authenticators");
    // Register system authenticator
    DataHubSystemAuthenticator systemAuthenticator = new DataHubSystemAuthenticator();
    systemAuthenticator.init(
        ImmutableMap.of(
            SYSTEM_CLIENT_ID_CONFIG,
            this.configurationProvider.getAuthentication().getSystemClientId(),
            SYSTEM_CLIENT_SECRET_CONFIG,
            this.configurationProvider.getAuthentication().getSystemClientSecret()),
        authenticatorContext);
    authenticatorChain.register(
        systemAuthenticator); // Always register authenticator for internal system.

    // Register authenticator define in application.yaml
    final List<AuthenticatorConfiguration> authenticatorConfigurations =
        this.configurationProvider.getAuthentication().getAuthenticators();
    for (AuthenticatorConfiguration internalAuthenticatorConfig : authenticatorConfigurations) {
      final String type = internalAuthenticatorConfig.getType();
      final Map<String, Object> configs = internalAuthenticatorConfig.getConfigs();

      log.debug(String.format("Found configs for Authenticator of type %s: %s ", type, configs));

      // Instantiate the Authenticator class.
      Class<?> clazz = null;
      try {
        clazz = Class.forName(type);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(
            String.format(
                "Failed to find Authenticator class with name %s on the classpath.", type));
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
        final Authenticator authenticator =
            (Authenticator) clazz.getDeclaredConstructor().newInstance();
        // Successfully created authenticator. Now init and register it.
        log.debug(String.format("Initializing Authenticator with name %s", type));
        if (authenticator instanceof HealthStatusAuthenticator) {
          Map<String, Object> authenticatorConfig =
              new HashMap<>(
                  Map.of(
                      SYSTEM_CLIENT_ID_CONFIG,
                      this.configurationProvider.getAuthentication().getSystemClientId()));
          authenticatorConfig.putAll(
              Optional.ofNullable(internalAuthenticatorConfig.getConfigs())
                  .orElse(Collections.emptyMap()));
          authenticator.init(authenticatorConfig, authenticatorContext);
        } else {
          authenticator.init(configs, authenticatorContext);
        }
        log.info(String.format("Registering Authenticator with name %s", type));
        authenticatorChain.register(authenticator);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Failed to instantiate Authenticator with class name %s", clazz.getCanonicalName()),
            e);
      }
    }
  }
}
