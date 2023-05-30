package com.linkedin.gms.factory.auth;

import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.secret.SecretService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class InviteTokenServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Bean(name = "inviteTokenService")
  @Scope("singleton")
  @Nonnull
  protected InviteTokenService getInstance() throws Exception {
    return new InviteTokenService(this._javaEntityClient, this._secretService);
  }
}
