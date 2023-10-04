package com.linkedin.gms.factory.join;

import com.datahub.authentication.Authentication;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.service.JoinService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

import javax.annotation.Nonnull;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class JoinServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "joinService")
  @Scope("singleton")
  @Nonnull
  protected JoinService getInstance() throws Exception {
    return new JoinService(_javaEntityClient, _authentication);
  }
}
