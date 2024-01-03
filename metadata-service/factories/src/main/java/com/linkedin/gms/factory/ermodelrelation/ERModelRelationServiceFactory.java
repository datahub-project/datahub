package com.linkedin.gms.factory.ermodelrelation;

import com.datahub.authentication.Authentication;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.service.ERModelRelationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

import javax.annotation.Nonnull;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class ERModelRelationServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "ermodelrelationService")
  @Scope("singleton")
  @Nonnull
  protected ERModelRelationService getInstance() throws Exception {
    return new ERModelRelationService(_javaEntityClient, _authentication);
  }
}
