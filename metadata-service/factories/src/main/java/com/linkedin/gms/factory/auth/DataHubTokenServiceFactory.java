package com.linkedin.gms.factory.auth;

import com.datahub.authentication.token.TokenService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DataHubTokenServiceFactory {

  @Bean(name = "dataHubTokenService")
  @Scope("singleton")
  @Nonnull
  protected TokenService getInstance() {
    return new TokenService(
        "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=",
        "HS256",
        "datahubapp"
    );
  }
}

