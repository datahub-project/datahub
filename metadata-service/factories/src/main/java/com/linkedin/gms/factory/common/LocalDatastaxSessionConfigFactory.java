package com.linkedin.gms.factory.common;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class LocalDatastaxSessionConfigFactory {

  @Value("${DATASTAX_DATASOURCE_USERNAME:cassandra}")
  private String datastaxDatasourceUsername;

  @Value("${DATASTAX_DATASOURCE_PASSWORD:cassandra}")
  private String datastaxDatasourcePassword;

  @Value("${DATASTAX_HOSTS:cassandra}")
  private String datastaxHosts;

  @Value("${DATASTAX_PORT:9042}")
  private String datastaxPort;

  @Value("${DATASTAX_DATACENTER:datacenter1}")
  private String datastaxDataCenter;

  @Value("${DATASTAX_KEYSPACE:datahub}")
  private String datastaxKeyspace;

  @Value("${DATASTAX_USE_SSL:false}")
  private String datastaxUseSsl;

  @Bean(name = "gmsDatastaxServiceConfig")
  protected Map<String, String> createInstance() {
    return new HashMap<String, String>() {{
      put("username", datastaxDatasourceUsername);
      put("password", datastaxDatasourcePassword);
      put("hosts", datastaxHosts);
      put("port", datastaxPort);
      put("datacenter", datastaxDataCenter);
      put("keyspace", datastaxKeyspace);
      put("useSsl", datastaxUseSsl);
    }};
  }
}
