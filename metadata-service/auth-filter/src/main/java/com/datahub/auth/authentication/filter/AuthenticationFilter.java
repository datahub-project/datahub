package com.datahub.auth.authentication.filter;

import static com.datahub.authentication.AuthenticationConstants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorConfiguration;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.authenticator.AuthenticatorChain;
import com.datahub.authentication.authenticator.DataHubSystemAuthenticator;
import com.datahub.authentication.authenticator.HealthStatusAuthenticator;
import com.datahub.authentication.authenticator.NoOpAuthenticator;
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
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * A servlet {@link Filter} for authenticating requests inbound to the Metadata Service. This filter
 * is applied to the GraphQL Servlet, the Rest.li Servlet, and the Auth (token) Servlet.
 */
@Component
@Slf4j
public class AuthenticationFilter extends OncePerRequestFilter {

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
  private Set<String> excludedPathPatterns;

  @PostConstruct
  public void init() {
    buildAuthenticatorChain();
    initializeExcludedPaths();
    log.info("AuthenticationFilter initialized.");
  }

  private void initializeExcludedPaths() {
    excludedPathPatterns = new HashSet<>();
    String excludedPaths = configurationProvider.getAuthentication().getExcludedPaths();
    if (StringUtils.hasText(excludedPaths)) {
      excludedPathPatterns.addAll(
          Arrays.stream(excludedPaths.split(","))
              .map(String::trim)
              .filter(path -> !path.isBlank())
              .toList());
    }
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    AuthenticationRequest context = buildAuthContext(request);
    Authentication authentication = null;
    try {
      authentication = this.authenticatorChain.authenticate(context, _logAuthenticatorExceptions);
    } catch (AuthenticationException e) {
      // For AuthenticationExpiredExceptions, terminate and provide that feedback to the user
      log.debug(
          "Failed to authenticate request. Received an AuthenticationExpiredException from authenticator chain.",
          e);
      ((HttpServletResponse) response)
          .sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized to perform this action.");
      return;
    }

    if (authentication != null) {
      String actorUrnStr = authentication.getActor().toUrnStr();
      // Successfully authenticated.
      log.debug(
          "Successfully authenticated request for Actor with type: {}, id: {}",
          authentication.getActor().getType(),
          authentication.getActor().getId());
      AuthenticationContext.setAuthentication(authentication);
      chain.doFilter(request, response);
    } else {
      // Reject request
      log.debug(
          "Failed to authenticate request. Received 'null' Authentication value from authenticator chain.");
      ((HttpServletResponse) response)
          .sendError(
              HttpServletResponse.SC_UNAUTHORIZED,
              "Unauthorized to perform this action due to expired auth.");
      return;
    }
    AuthenticationContext.remove();
  }

  @VisibleForTesting
  @Override
  public boolean shouldNotFilter(HttpServletRequest request) {
    String path = request.getServletPath();
    if (path == null) {
      return false;
    }

    // Check if the path matches any of the excluded patterns
    boolean shouldExclude =
        excludedPathPatterns.stream()
            .anyMatch(
                pattern -> {
                  if (pattern.endsWith("/*")) {
                    // Handle wildcard patterns
                    String basePattern = pattern.substring(0, pattern.length() - 2);
                    return path.startsWith(basePattern);
                  }
                  return path.equals(pattern);
                });

    if (shouldExclude) {
      log.debug("Skipping authentication for excluded path: {}", path);
    }

    return shouldExclude;
  }

  @Override
  public void destroy() {
    // Nothing
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
      log.info("Auth is enabled. Building authenticator chain...");
      this.registerNativeAuthenticator(
          authenticatorChain, authenticatorContext); // Register native authenticators
      this.registerPlugins(authenticatorChain); // Register plugin authenticators
    } else {
      // Authentication is not enabled. Populate authenticator chain with a purposely permissive
      // Authenticator.
      log.info("Auth is disabled. Building no-op authenticator chain...");
      final NoOpAuthenticator noOpAuthenticator = new NoOpAuthenticator();
      noOpAuthenticator.init(
          ImmutableMap.of(
              SYSTEM_CLIENT_ID_CONFIG,
              this.configurationProvider.getAuthentication().getSystemClientId()),
          authenticatorContext);
      authenticatorChain.register(noOpAuthenticator);
    }
  }

  private AuthenticationRequest buildAuthContext(HttpServletRequest request) {
    return new AuthenticationRequest(
        request.getServletPath(),
        request.getPathInfo(),
        Collections.list(request.getHeaderNames()).stream()
            .collect(Collectors.toMap(headerName -> headerName, request::getHeader)));
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
      Class<? extends Authenticator> clazz = null;
      try {
        clazz = (Class<? extends Authenticator>) Class.forName(type);
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
        final Authenticator authenticator = clazz.newInstance();
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
