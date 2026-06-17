package com.linkedin.gms.factory.config;

import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.config.SystemDataAccessControlConfiguration;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SystemDataAccessControlConfigurationFactory {

  @Bean
  @Nonnull
  public SystemDataAccessControlConfiguration systemDataAccessControlConfiguration(
      @Nonnull ConfigurationProvider configurationProvider) {
    AuthorizationConfiguration authorization = configurationProvider.getAuthorization();
    if (authorization != null && authorization.getSystemDataAccessControl() != null) {
      return authorization.getSystemDataAccessControl();
    }
    return new SystemDataAccessControlConfiguration();
  }
}
