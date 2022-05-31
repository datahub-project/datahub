package com.linkedin.gms.factory.notifications;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.datahub.notification.provider.IdentityProvider;
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
public class IdentityProviderFactory {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Autowired
  @Qualifier("restliEntityClient")
  private EntityClient entityClient;

  @Bean(name = "identityProvider")
  @Scope("singleton")
  @Nonnull
  protected IdentityProvider getInstance() {
    return new IdentityProvider(this.entityClient, this.systemAuthentication);
  }
}
