package com.linkedin.gms.factory.common;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Neo4jDriverFactory {
  @Value("${NEO4J_USERNAME:neo4j}")
  private String username;

  @Value("${NEO4J_PASSWORD:datahub}")
  private String password;

  @Value("${NEO4J_URI:bolt://localhost}")
  private String uri;

  @Value("${NEO4J_MAX_CONNECTION_POOL_SIZE:100}")
  private Integer neo4jMaxConnectionPoolSize;

  @Value("${NEO4J_MAX_CONNECTION_ACQUISITION_TIMEOUT_IN_SECONDS:60}")
  private Long neo4jMaxConnectionAcquisitionTimeout;

  @Value("${NEO4j_MAX_CONNECTION_LIFETIME_IN_HOURS:1}")
  private Long neo4jMaxConnectionLifetime;

  @Value("${NEO4J_MAX_TRANSACTION_RETRY_TIME_IN_SECONDS:30}")
  private Long neo4jMaxTransactionRetryTime;

  @Bean(name = "neo4jDriver")
  protected Driver createInstance() {
    Config.ConfigBuilder builder = Config.builder();
    builder.withMaxConnectionPoolSize(neo4jMaxConnectionPoolSize);
    builder.withConnectionAcquisitionTimeout(neo4jMaxConnectionAcquisitionTimeout, TimeUnit.SECONDS);
    builder.withMaxConnectionLifetime(neo4jMaxConnectionLifetime, TimeUnit.HOURS);
    builder.withMaxTransactionRetryTime(neo4jMaxTransactionRetryTime, TimeUnit.SECONDS);

    return GraphDatabase.driver(uri, AuthTokens.basic(username, password), builder.build());
  }
}
