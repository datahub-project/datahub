package com.linkedin.gms.factory.ownership;

import com.datahub.authentication.Authentication;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.service.OwnershipTypeService;
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
public class OwnershipTypeServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "ownerShipTypeService")
  @Scope("singleton")
  @Nonnull
  protected OwnershipTypeService getInstance() throws Exception {
    return new OwnershipTypeService(_javaEntityClient, _authentication);
  }
}
