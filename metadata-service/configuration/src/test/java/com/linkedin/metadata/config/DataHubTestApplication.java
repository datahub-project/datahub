package com.linkedin.metadata.config;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
public class DataHubTestApplication {
  @Autowired private DataHubAppConfiguration dataHubAppConfig;

  public DataHubAppConfiguration getDataHubAppConfig() {
    return dataHubAppConfig;
  }
}
