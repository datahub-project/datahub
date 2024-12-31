package com.linkedin.gms.factory.incident;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.service.IncidentService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

@Configuration
@Import({SystemAuthenticationFactory.class})
public class IncidentServiceFactory {
  @Bean(name = "incidentService")
  @Scope("singleton")
  @Nonnull
  protected IncidentService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new IncidentService(entityClient);
  }
}
