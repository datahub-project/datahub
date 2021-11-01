package com.datahub.authentication;

import com.datahub.authentication.token.DataHubTokenService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DataHubTokenServiceFactory {

  @Bean(name = "dataHubTokenService")
  @Scope("singleton")
  @Nonnull
  protected DataHubTokenService getInstance() {
    return new DataHubTokenService(
        "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=",
        "HS256",
        6000000,
        "acryl.io"
    );
  }
}
