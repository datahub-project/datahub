package com.linkedin.gms.factory.common;

import org.neo4j.driver.AuthTokens;
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

  @Bean(name = "neo4jDriver")
  protected Driver createInstance() {
    return GraphDatabase.driver(uri, AuthTokens.basic(username, password));
  }
}