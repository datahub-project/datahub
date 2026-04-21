package com.linkedin.gms.factory.auth;

import com.datahub.authentication.token.StatefulTokenService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DataHubTokenServiceFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  /** + @Inject + @Named("entityService") + private EntityService<?> _entityService; + */
  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Bean(name = "dataHubTokenService")
  @Scope("singleton")
  @Nonnull
  protected StatefulTokenService getInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext) {
    return new StatefulTokenService(
        systemOpContext,
        configurationProvider.getAuthentication().getTokenService().getSigningKey(),
        configurationProvider.getAuthentication().getTokenService().getSigningAlgorithm(),
        configurationProvider.getAuthentication().getTokenService().getIssuer(),
        _entityService,
        configurationProvider.getAuthentication().getTokenService().getSalt());
  }

  @PostConstruct
  public void validate() {
    if (!configurationProvider.getAuthentication().isEnabled()) {
      return;
    }
    String signingKey = configurationProvider.getAuthentication().getTokenService().getSigningKey();
    String salt = configurationProvider.getAuthentication().getTokenService().getSalt();
    if (signingKey == null || signingKey.isEmpty()) {
      throw new IllegalArgumentException(
          "authentication.tokenService.signingKey must be set and not be empty");
    }
    if (salt == null || salt.isEmpty()) {
      throw new IllegalArgumentException(
          "authentication.tokenService.salt must be set and not be empty");
    }
  }
}
