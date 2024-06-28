package com.linkedin.gms.factory.auth;

import com.datahub.authentication.user.NativeUserService;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.services.SecretService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class NativeUserServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Bean(name = "nativeUserService")
  @Scope("singleton")
  @Nonnull
  protected NativeUserService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new NativeUserService(
        _entityService, entityClient, _secretService, _configurationProvider.getAuthentication());
  }
}
