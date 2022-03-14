package com.linkedin.gms.factory.common;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class LocalCassandraSessionConfigFactory {

  @Value("${CASSANDRA_DATASOURCE_USERNAME:cassandra}")
  private String cassandraDatasourceUsername;

  @Value("${CASSANDRA_DATASOURCE_PASSWORD:cassandra}")
  private String cassandraDatasourcePassword;

  @Value("${CASSANDRA_HOSTS:cassandra}")
  private String cassandraHosts;

  @Value("${CASSANDRA_PORT:9042}")
  private String cassandraPort;

  @Value("${CASSANDRA_DATACENTER:datacenter1}")
  private String cassandraDataCenter;

  @Value("${CASSANDRA_KEYSPACE:datahub}")
  private String cassandraKeyspace;

  @Value("${CASSANDRA_USE_SSL:false}")
  private String cassandraUseSsl;

  @Bean(name = "gmsCassandraServiceConfig")
  protected Map<String, String> createInstance() {
    return new HashMap<String, String>() {{
      put("username", cassandraDatasourceUsername);
      put("password", cassandraDatasourcePassword);
      put("hosts", cassandraHosts);
      put("port", cassandraPort);
      put("datacenter", cassandraDataCenter);
      put("keyspace", cassandraKeyspace);
      put("useSsl", cassandraUseSsl);
    }};
  }
}
