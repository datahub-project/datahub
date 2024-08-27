package com.datahub.auth.authentication;

import static org.mockito.Mockito.*;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.AuthenticatorConfiguration;
import com.datahub.authentication.TokenServiceConfiguration;
import com.datahub.authentication.token.StatefulTokenService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.AuthPluginConfiguration;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.PluginConfiguration;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class AuthTestConfiguration {

  @Bean
  public EntityService<?> entityService() {
    return mock(EntityService.class);
  }

  @Bean("dataHubTokenService")
  public StatefulTokenService statefulTokenService(
      ConfigurationProvider configurationProvider, EntityService<?> entityService) {
    TokenServiceConfiguration tokenServiceConfiguration =
        configurationProvider.getAuthentication().getTokenService();
    return new StatefulTokenService(
        TestOperationContexts.systemContextNoSearchAuthorization(),
        tokenServiceConfiguration.getSigningKey(),
        tokenServiceConfiguration.getSigningAlgorithm(),
        tokenServiceConfiguration.getIssuer(),
        entityService,
        tokenServiceConfiguration.getSalt());
  }

  @Bean
  public ConfigurationProvider configurationProvider() {
    ConfigurationProvider configurationProvider = new ConfigurationProvider();
    AuthenticationConfiguration authenticationConfiguration = new AuthenticationConfiguration();
    authenticationConfiguration.setEnabled(true);
    configurationProvider.setAuthentication(authenticationConfiguration);
    DataHubConfiguration dataHubConfiguration = new DataHubConfiguration();
    PluginConfiguration pluginConfiguration = new PluginConfiguration();
    AuthPluginConfiguration authPluginConfiguration = new AuthPluginConfiguration();
    authenticationConfiguration.setSystemClientId("__datahub_system");
    authenticationConfiguration.setSystemClientSecret("JohnSnowKnowsNothing");
    TokenServiceConfiguration tokenServiceConfiguration = new TokenServiceConfiguration();
    tokenServiceConfiguration.setIssuer("datahub-metadata-service");
    tokenServiceConfiguration.setSigningKey("WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=");
    tokenServiceConfiguration.setSalt("ohDVbJBvHHVJh9S/UA4BYF9COuNnqqVhr9MLKEGXk1O=");
    tokenServiceConfiguration.setSigningAlgorithm("HS256");
    authenticationConfiguration.setTokenService(tokenServiceConfiguration);
    AuthenticatorConfiguration authenticator = new AuthenticatorConfiguration();
    authenticator.setType("com.datahub.authentication.authenticator.DataHubTokenAuthenticator");
    authenticator.setConfigs(
        Map.of(
            "signingKey",
            "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=",
            "salt",
            "ohDVbJBvHHVJh9S/UA4BYF9COuNnqqVhr9MLKEGXk1O="));
    List<AuthenticatorConfiguration> authenticators = List.of(authenticator);
    authenticationConfiguration.setAuthenticators(authenticators);
    authPluginConfiguration.setPath("");
    pluginConfiguration.setAuth(authPluginConfiguration);
    dataHubConfiguration.setPlugin(pluginConfiguration);
    configurationProvider.setDatahub(dataHubConfiguration);
    return configurationProvider;
  }

  @Bean
  // TODO: Constructor injection
  @DependsOn({"configurationProvider", "dataHubTokenService", "entityService"})
  public AuthenticationFilter authenticationFilter() throws ServletException {
    return new AuthenticationFilter();
  }
}
