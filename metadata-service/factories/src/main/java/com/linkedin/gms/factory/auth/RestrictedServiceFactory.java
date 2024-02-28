package com.linkedin.gms.factory.auth;

import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.service.RestrictedService;
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
public class RestrictedServiceFactory {

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Bean(name = "restrictedService")
  @Scope("singleton")
  @Nonnull
  protected RestrictedService getInstance() throws Exception {
    return new RestrictedService(_secretService);
  }
}
