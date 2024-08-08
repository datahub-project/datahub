package com.linkedin.gms.factory.auth;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import javax.annotation.Nonnull;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Factory responsible for instantiating an instance of {@link Authentication} used to authenticate
 * requests made by the internal system.
 */
@Configuration
@ConfigurationProperties
@Data
public class SystemAuthenticationFactory {

  @Value("${authentication.systemClientId:#{null}}")
  private String systemClientId;

  @Value("${authentication.systemClientSecret:#{null}}")
  private String systemSecret;

  @Bean(name = "systemAuthentication")
  @Scope("singleton")
  @Nonnull
  protected Authentication getInstance() {
    // TODO: Change to service
    final Actor systemActor = new Actor(ActorType.USER, systemClientId);
    return new Authentication(
        systemActor, String.format("Basic %s:%s", systemClientId, systemSecret));
  }
}
