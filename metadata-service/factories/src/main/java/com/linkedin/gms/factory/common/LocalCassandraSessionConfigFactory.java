/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
