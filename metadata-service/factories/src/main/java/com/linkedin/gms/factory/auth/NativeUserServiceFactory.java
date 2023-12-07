package com.linkedin.gms.factory.auth;

import com.datahub.authentication.user.NativeUserService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class NativeUserServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService _entityService;

  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Autowired private ConfigurationProvider _configurationProvider;

  @Bean(name = "nativeUserService")
  @Scope("singleton")
  @Nonnull
  protected NativeUserService getInstance() throws Exception {
    return new NativeUserService(
        _entityService,
        _javaEntityClient,
        _secretService,
        _configurationProvider.getAuthentication());
  }
}
