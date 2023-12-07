package com.linkedin.gms.factory.common;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class LocalCassandraSessionConfigFactory {

  @Value("${cassandra.datasourceUsername}")
  private String datasourceUsername;

  @Value("${cassandra.datasourcePassword}")
  private String datasourcePassword;

  @Value("${cassandra.hosts}")
  private String hosts;

  @Value("${cassandra.port}")
  private String port;

  @Value("${cassandra.datacenter}")
  private String datacenter;

  @Value("${cassandra.keyspace}")
  private String keyspace;

  @Value("${cassandra.useSsl}")
  private String useSsl;

  @Bean(name = "gmsCassandraServiceConfig")
  protected Map<String, String> createInstance() {
    return new HashMap<String, String>() {
      {
        put("username", datasourceUsername);
        put("password", datasourcePassword);
        put("hosts", hosts);
        put("port", port);
        put("datacenter", datacenter);
        put("keyspace", keyspace);
        put("useSsl", useSsl);
      }
    };
  }
}
