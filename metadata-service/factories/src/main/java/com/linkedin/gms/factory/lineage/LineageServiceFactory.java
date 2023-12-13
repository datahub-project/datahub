package com.linkedin.gms.factory.lineage;

import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.service.LineageService;
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
public class LineageServiceFactory {
  @Autowired
  @Qualifier("javaEntityClient")
  private JavaEntityClient _javaEntityClient;

  @Bean(name = "lineageService")
  @Scope("singleton")
  @Nonnull
  protected LineageService getInstance() throws Exception {
    return new LineageService(this._javaEntityClient);
  }
}
