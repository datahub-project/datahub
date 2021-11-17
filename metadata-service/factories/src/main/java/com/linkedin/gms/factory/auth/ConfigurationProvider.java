package com.linkedin.gms.factory.auth;

import com.datahub.authentication.AuthenticationConfiguration;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Configuration
@ConfigurationProperties
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Data
public class ConfigurationProvider {
  private AuthenticationConfiguration authentication;
}