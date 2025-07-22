package com.linkedin.metadata.graph.neo4j;

import java.net.URI;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/** Neo4j test server builder using Testcontainers. */
public class Neo4jTestServerBuilder {

  private static final String NEO4J_IMAGE = "neo4j:4.4.28-community";
  private static final String NEO4J_PASSWORD = "testpassword";

  private Neo4jContainer<?> container;
  private Driver driver;

  public Neo4jTestServerBuilder() {
    // Initialize container with required configuration
  }

  public void start() {
    if (container == null) {
      container =
          new Neo4jContainer<>(DockerImageName.parse(NEO4J_IMAGE))
              .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
              .withPlugins("apoc")
              .withAdminPassword(NEO4J_PASSWORD)
              .withEnv("NEO4J_apoc_export_file_enabled", "true")
              .withEnv("NEO4J_apoc_import_file_enabled", "true")
              .withEnv("NEO4J_apoc_import_file_use__neo4j__config", "true")
              .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]");
    }

    if (!container.isRunning()) {
      container.start();
      this.driver =
          GraphDatabase.driver(container.getBoltUrl(), AuthTokens.basic("neo4j", NEO4J_PASSWORD));
    }
  }

  public void shutdown() {
    if (driver != null) {
      driver.close();
      driver = null;
    }
    if (container != null && container.isRunning()) {
      container.stop();
      container = null;
    }
  }

  public URI boltURI() {
    ensureRunning();
    return URI.create(container.getBoltUrl());
  }

  public URI httpURI() {
    ensureRunning();
    return URI.create(container.getHttpUrl());
  }

  public Driver getDriver() {
    ensureRunning();
    return driver;
  }

  public String getPassword() {
    return NEO4J_PASSWORD;
  }

  private void ensureRunning() {
    if (container == null || !container.isRunning()) {
      throw new IllegalStateException("Container is not running. Call start() first.");
    }
  }
}
