package com.linkedin.gms.factory.auth;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.DefaultEntitySpecResolver;
import com.datahub.authorization.EntitySpecResolver;
import com.datahub.plugins.PluginConstant;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginPermissionManager;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.SecurityMode;
import com.datahub.plugins.configuration.Config;
import com.datahub.plugins.configuration.ConfigProvider;
import com.datahub.plugins.factory.PluginConfigFactory;
import com.datahub.plugins.loader.IsolatedClassLoader;
import com.datahub.plugins.loader.PluginPermissionManagerImpl;
import com.google.common.collect.ImmutableMap;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({DataHubAuthorizerFactory.class})
public class AuthorizerChainFactory {
  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configurationProvider;

  @Autowired
  @Qualifier("dataHubAuthorizer")
  private DataHubAuthorizer dataHubAuthorizer;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient entityClient;

  @Bean(name = "authorizerChain")
  @Scope("singleton")
  @Nonnull
  protected AuthorizerChain getInstance() {
    final EntitySpecResolver resolver = initResolver();

    // Extract + initialize customer authorizers from application configs.
    final List<Authorizer> authorizers = new ArrayList<>(initCustomAuthorizers(resolver));

    if (configurationProvider.getAuthorization().getDefaultAuthorizer().isEnabled()) {
      AuthorizerContext ctx = new AuthorizerContext(Collections.emptyMap(), resolver);
      this.dataHubAuthorizer.init(Collections.emptyMap(), ctx);
      log.info("Default DataHubAuthorizer is enabled. Appending it to the authorization chain.");
      authorizers.add(this.dataHubAuthorizer);
    }

    return new AuthorizerChain(authorizers, dataHubAuthorizer);
  }

  private EntitySpecResolver initResolver() {
    return new DefaultEntitySpecResolver(systemAuthentication, entityClient);
  }

  private List<Authorizer> initCustomAuthorizers(EntitySpecResolver resolver) {
    final List<Authorizer> customAuthorizers = new ArrayList<>();

    Path pluginBaseDirectory =
        Paths.get(configurationProvider.getDatahub().getPlugin().getAuth().getPath());
    ConfigProvider configProvider = new ConfigProvider(pluginBaseDirectory);

    Optional<Config> optionalConfig = configProvider.load();
    // Register authorizer plugins if present
    optionalConfig.ifPresent(
        (config) -> {
          registerAuthorizer(customAuthorizers, resolver, config);
        });

    return customAuthorizers;
  }

  private void registerAuthorizer(
      List<Authorizer> customAuthorizers, EntitySpecResolver resolver, Config config) {
    PluginConfigFactory authorizerPluginPluginConfigFactory = new PluginConfigFactory(config);
    // Load only Authorizer configuration from plugin config factory
    List<PluginConfig> authorizers =
        authorizerPluginPluginConfigFactory.loadPluginConfigs(PluginType.AUTHORIZER);

    // Select only enabled authorizer for instantiation
    List<PluginConfig> enabledAuthorizers =
        authorizers.stream()
            .filter(
                pluginConfig -> {
                  if (!pluginConfig.getEnabled()) {
                    log.info(String.format("Authorizer %s is not enabled", pluginConfig.getName()));
                  }
                  return pluginConfig.getEnabled();
                })
            .collect(Collectors.toList());

    // Get security mode set by user
    SecurityMode securityMode =
        SecurityMode.valueOf(
            this.configurationProvider.getDatahub().getPlugin().getPluginSecurityMode());
    // Create permission manager with security mode
    PluginPermissionManager permissionManager = new PluginPermissionManagerImpl(securityMode);

    // Save ContextClassLoader. As some plugins are directly using context classloader from current
    // thread to load libraries
    // This will break plugin as their dependencies are inside plugin directory only
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    // Instantiate Authorizer plugins
    enabledAuthorizers.forEach(
        (pluginConfig) -> {
          // Create context
          AuthorizerContext context =
              new AuthorizerContext(
                  ImmutableMap.of(
                      PluginConstant.PLUGIN_HOME, pluginConfig.getPluginHomeDirectory().toString()),
                  resolver);
          IsolatedClassLoader isolatedClassLoader =
              new IsolatedClassLoader(permissionManager, pluginConfig);
          try {
            Thread.currentThread().setContextClassLoader((ClassLoader) isolatedClassLoader);
            Authorizer authorizer =
                (Authorizer) isolatedClassLoader.instantiatePlugin(Authorizer.class);
            log.info("Initializing plugin {}", pluginConfig.getName());
            authorizer.init(pluginConfig.getConfigs().orElse(Collections.emptyMap()), context);
            customAuthorizers.add(authorizer);
            log.info("Plugin {} is initialized", pluginConfig.getName());
          } catch (ClassNotFoundException e) {
            log.debug(String.format("Failed to init the plugin", pluginConfig.getName()));
            throw new RuntimeException(e);
          } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
          }
        });
  }
}
