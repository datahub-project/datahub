package com.linkedin.gms.factory.connection;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ConnectionServiceFactory {
  @Bean(name = "connectionService")
  @Scope("singleton")
  @Nonnull
  protected ConnectionService getInstance(final SystemEntityClient systemEntityClient)
      throws Exception {
    return new ConnectionService(systemEntityClient, systemEntityClient.getSystemAuthentication());
  }
}
