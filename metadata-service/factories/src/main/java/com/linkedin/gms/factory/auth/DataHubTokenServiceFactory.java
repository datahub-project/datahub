package com.linkedin.gms.factory.auth;

import com.datahub.authentication.token.TokenService;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
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

  @Value("${elasticsearch.tokenService.signingAlgorithm:HS256}")
  private String signingAlgorithm;

  @Value("${elasticsearch.tokenService.issuer:datahub-metadata-service}")
  private String issuer;

  @Bean(name = "dataHubTokenService")
  @Scope("singleton")
  @Nonnull
  protected TokenService getInstance() {
    return new TokenService(
        this.signingKey,
        this.signingAlgorithm,
        this.issuer
    );
  }
}

