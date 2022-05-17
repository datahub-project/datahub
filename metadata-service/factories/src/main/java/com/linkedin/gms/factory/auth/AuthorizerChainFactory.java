package com.linkedin.gms.factory.auth;

import com.datahub.authorization.AuthorizerConfiguration;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.Authorizer;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private ConfigurationProvider configurationProvider;

  @Autowired
  @Qualifier("dataHubAuthorizer")
  private DataHubAuthorizer _dataHubAuthorizer;

  @Bean(name = "authorizerChain")
  @Scope("singleton")
  @Nonnull
  protected AuthorizerChain getInstance() {
    // Extract + initialize customer authorizers from application configs.
    final List<Authorizer> authorizers = new ArrayList<>(initCustomAuthorizers());
    // Add the DataHub core policies-based Authorizer - this one should always be enabled.
    authorizers.add(this._dataHubAuthorizer);
    return new AuthorizerChain(authorizers);
  }

  private List<Authorizer> initCustomAuthorizers() {
    final List<Authorizer> customAuthorizers = new ArrayList<>();

    if (this.configurationProvider.getAuthorization().getAuthorizers() != null) {

      final List<AuthorizerConfiguration> authorizerConfigurations =
          this.configurationProvider.getAuthorization().getAuthorizers();

      for (AuthorizerConfiguration authorizer : authorizerConfigurations) {
        final String type = authorizer.getType();
        final Map<String, Object> configs =
            authorizer.getConfigs() != null ? authorizer.getConfigs() : Collections.emptyMap();

        log.debug(String.format("Found configs for notification sink of type %s: %s ", type, configs));

        // Instantiate the Authorizer
        Class<? extends Authorizer> clazz = null;
        try {
          clazz = (Class<? extends Authorizer>) Class.forName(type);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(
              String.format("Failed to find custom Authorizer class with name %s on the classpath.", type));
        }

        // Else construct an instance of the class, each class should have an empty constructor.
        try {
          final Authorizer authorizerInstance = clazz.newInstance();
          authorizerInstance.init(configs);
          customAuthorizers.add(authorizerInstance);
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to instantiate custom Authorizer with class name %s", clazz.getCanonicalName()), e);
        }
      }
    }
    return customAuthorizers;
  }
}