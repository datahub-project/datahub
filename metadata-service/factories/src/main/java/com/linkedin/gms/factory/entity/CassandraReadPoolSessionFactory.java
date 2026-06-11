package com.linkedin.gms.factory.entity;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CassandraConfiguration;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "entityService.impl", havingValue = "cassandra")
@ConditionalOnProperty(name = "datahub.readOnly", havingValue = "false", matchIfMissing = true)
public class CassandraReadPoolSessionFactory {

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean(name = "cassandraReadPoolSession")
  @ConditionalOnProperty(prefix = "cassandra.readPool", name = "enabled", havingValue = "true")
  @Nonnull
  protected CqlSession createReadPoolSession() {
    CassandraConfiguration cassandra =
        Objects.requireNonNull(
            configurationProvider.getCassandra(),
            "cassandra configuration must be present when entityService.impl=cassandra");
    ReadPoolConfiguration readPool =
        Objects.requireNonNull(
            cassandra.getReadPool(), "cassandra.readPool must be configured in application.yaml");

    String hosts =
        StringUtils.hasText(readPool.getHosts()) ? readPool.getHosts() : cassandra.getHosts();
    int port =
        StringUtils.hasText(readPool.getPort())
            ? Integer.parseInt(readPool.getPort())
            : Integer.parseInt(cassandra.getPort());
    String dc =
        StringUtils.hasText(readPool.getDatacenter())
            ? readPool.getDatacenter()
            : cassandra.getDatacenter();

    List<InetSocketAddress> addresses =
        Arrays.stream(hosts.split(","))
            .map(host -> new InetSocketAddress(host.trim(), port))
            .collect(Collectors.toList());

    CqlSessionBuilder csb =
        CqlSession.builder()
            .addContactPoints(addresses)
            .withLocalDatacenter(dc)
            .withKeyspace(cassandra.getKeyspace())
            .withAuthCredentials(
                cassandra.getDatasourceUsername(), cassandra.getDatasourcePassword());

    if (cassandra.isUseSsl()) {
      try {
        csb = csb.withSslContext(SSLContext.getDefault());
      } catch (Exception e) {
        log.error("Error creating cassandra read pool ssl session", e);
      }
    }

    boolean distinctEndpoint = !hosts.equals(cassandra.getHosts());
    if (distinctEndpoint) {
      log.info("Cassandra READ pool configured for distinct hosts: {}", hosts);
    } else {
      log.info("Cassandra READ pool configured as split-pool (same hosts as PRIMARY)");
    }

    return csb.build();
  }
}
