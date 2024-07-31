package com.linkedin.gms.factory.auth;

import com.datahub.authentication.token.StatefulTokenService;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DataHubTokenServiceFactory {

  @Value("${authentication.tokenService.signingKey:}")
  private String signingKey;

  @Value("${authentication.tokenService.salt:}")
  private String saltingKey;

  @Value("${authentication.tokenService.signingAlgorithm:HS256}")
  private String signingAlgorithm;

  @Value("${authentication.tokenService.issuer:datahub-metadata-service}")
  private String issuer;

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
        systemOpContext, signingKey, signingAlgorithm, issuer, _entityService, saltingKey);
  }
}
