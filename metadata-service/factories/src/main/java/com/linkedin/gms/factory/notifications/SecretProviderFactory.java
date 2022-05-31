package com.linkedin.gms.factory.notifications;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.datahub.notification.provider.SecretProvider;
import com.linkedin.metadata.secret.SecretService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({RestliEntityClientFactory.class, SystemAuthenticationFactory.class})
public class SecretProviderFactory {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Autowired
  @Qualifier("restliEntityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService secretService;

  @Bean(name = "secretProvider")
  @Scope("singleton")
  @Nonnull
  protected SecretProvider getInstance() {
    return new SecretProvider(this.entityClient, this.systemAuthentication, this.secretService);
  }
}
