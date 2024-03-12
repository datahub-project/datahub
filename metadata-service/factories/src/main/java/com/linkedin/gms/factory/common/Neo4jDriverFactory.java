package com.linkedin.gms.factory.common;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class Neo4jDriverFactory {
  @Value("${neo4j.username}")
  private String username;

  @Value("${neo4j.password}")
  private String password;

  @Value("${neo4j.uri}")
  private String uri;

  @Value("${neo4j.maxConnectionPoolSize}")
  private Integer neo4jMaxConnectionPoolSize;

  @Value("${neo4j.maxConnectionAcquisitionTimeout}")
  private Long neo4jMaxConnectionAcquisitionTimeout;

  // Kept for sake of backwards compatibility. Instead use NEO4j_MAX_CONNECTION_LIFETIME_IN_SECONDS
  @Value("${NEO4j_MAX_CONNECTION_LIFETIME_IN_HOURS:#{null}}")
  private Long neo4jMaxConnectionLifetimeInHours;

  @Value("${neo4j.maxConnectionLifetimeInSeconds}")
  private Long neo4jMaxConnectionLifetimeInSeconds;

  @Value("${neo4j.maxTransactionRetryTime}")
  private Long neo4jMaxTransactionRetryTime;

  @Value("${neo4j.connectionLivenessCheckTimeout}")
  private Long neo4jConnectionLivenessCheckTimeout;

  @Bean(name = "neo4jDriver")
  protected Driver createInstance() {

    Config.ConfigBuilder builder = Config.builder();
    builder.withMaxConnectionPoolSize(neo4jMaxConnectionPoolSize);
    builder.withConnectionAcquisitionTimeout(
        neo4jMaxConnectionAcquisitionTimeout, TimeUnit.SECONDS);
    builder.withMaxConnectionLifetime(neo4jMaxConnectionLifetime(), TimeUnit.SECONDS);
    builder.withMaxTransactionRetryTime(neo4jMaxTransactionRetryTime, TimeUnit.SECONDS);
    builder.withConnectionLivenessCheckTimeout(
        neo4jConnectionLivenessCheckTimeout, TimeUnit.SECONDS);

    return GraphDatabase.driver(uri, AuthTokens.basic(username, password), builder.build());
  }

  private Long neo4jMaxConnectionLifetime() {

    // neo4jMaxConnectionLifetimeInHours has precedence over neo4jMaxConnectionLifetimeInSeconds
    if (neo4jMaxConnectionLifetimeInHours != null) {
      return neo4jMaxConnectionLifetimeInHours * 3600;
    }
    return neo4jMaxConnectionLifetimeInSeconds;
  }
}
