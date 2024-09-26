package com.linkedin.gms.factory.connection;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.connection.ConnectionService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConnectionServiceFactory {
  @Bean(name = "connectionService")
  @Nonnull
  protected ConnectionService getInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient)
      throws Exception {
    return new ConnectionService(systemEntityClient);
  }
}
