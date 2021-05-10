package com.linkedin.metadata.kafka.config;

import org.neo4j.driver.Driver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.linkedin.gms.factory.common.Neo4jDriverFactory;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Configuration
@Import({Neo4jDriverFactory.class})
@RequiredArgsConstructor
public class Neo4jConfig {

  private final Driver neo4jDriver;

  @Bean
  public BaseGraphWriterDAO graphWriterDAO() {
    BaseGraphWriterDAO graphWriterDAO;
    try {
      graphWriterDAO = new Neo4jGraphWriterDAO(neo4jDriver);
    } catch (Exception e) {
      throw new RuntimeException("Error in initializing Neo4j." + e.toString());
    }
    log.info("Neo4jDriver built successfully");

    return graphWriterDAO;
  }
}