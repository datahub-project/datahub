package com.linkedin.gms.factory.settings;

import com.datahub.authentication.Authentication;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.service.SettingsService;
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
public class SettingsServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication _authentication;

  @Bean(name = "settingsService")
  @Scope("singleton")
  @Nonnull
  protected SettingsService getInstance() throws Exception {
    return new SettingsService(_javaEntityClient, _authentication);
  }
}
