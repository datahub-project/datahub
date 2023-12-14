package com.linkedin.gms.factory.auth;

import com.datahub.authentication.token.StatefulTokenService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class DataHubTokenServiceFactory {

  @Value("${authentication.tokenService.signingKey:}")
  private String signingKey;

  @Value("${authentication.tokenService.salt:}")
  private String saltingKey;

  @Value("${authentication.tokenService.signingAlgorithm:HS256}")
  private String signingAlgorithm;

  @Value("${authentication.tokenService.issuer:datahub-metadata-service}")
  private String issuer;

  /** + @Inject + @Named("entityService") + private EntityService _entityService; + */
  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Bean(name = "dataHubTokenService")
  @Scope("singleton")
  @Nonnull
  protected StatefulTokenService getInstance() {
    return new StatefulTokenService(
        this.signingKey, this.signingAlgorithm, this.issuer, this._entityService, this.saltingKey);
  }
}
