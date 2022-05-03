package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
public class CassandraSessionFactory {

  @Autowired
  @Qualifier("gmsCassandraServiceConfig")
  private Map<String, String> sessionConfig;

  @Bean(name = "cassandraSession")
  @DependsOn({"gmsCassandraServiceConfig"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
  @Nonnull
  protected CqlSession createSession() {
    int port = Integer.parseInt(sessionConfig.get("port"));
    List<InetSocketAddress> addresses = Arrays.stream(sessionConfig.get("hosts").split(","))
        .map(host -> new InetSocketAddress(host, port))
        .collect(Collectors.toList());

    String dc = sessionConfig.get("datacenter");
    String ks = sessionConfig.get("keyspace");
    String username = sessionConfig.get("username");
    String password = sessionConfig.get("password");

    CqlSessionBuilder csb = CqlSession.builder()
        .addContactPoints(addresses)
        .withLocalDatacenter(dc)
        .withKeyspace(ks)
        .withAuthCredentials(username, password);

    if (sessionConfig.containsKey("useSsl") && sessionConfig.get("useSsl").equals("true")) {
      try {
        csb = csb.withSslContext(SSLContext.getDefault());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return csb.build();
  }
}
