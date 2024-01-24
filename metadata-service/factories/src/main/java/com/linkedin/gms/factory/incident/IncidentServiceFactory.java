package com.linkedin.gms.factory.incident;

import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.service.IncidentService;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
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
@Import({SystemAuthenticationFactory.class})
public class IncidentServiceFactory {

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "incidentService")
  @Scope("singleton")
  @Nonnull
  protected IncidentService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new IncidentService(entityClient, _authentication);
  }
}
