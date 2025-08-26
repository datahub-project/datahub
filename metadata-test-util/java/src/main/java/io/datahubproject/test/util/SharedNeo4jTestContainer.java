package io.datahubproject.test.util;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Singleton container that's shared across all tests in the JVM. This significantly reduces test
 * execution time and resource usage.
 */
public class SharedNeo4jTestContainer {

  private static final String NEO4J_IMAGE = "neo4j:4.4.28-community";
  private static final String NEO4J_PASSWORD = "testpassword";

  private static Neo4jContainer<?> container;
  private static Driver driver;

  static {
    // Start container once when class is loaded
    start();

    // Register shutdown hook to stop container when JVM exits
    Runtime.getRuntime().addShutdownHook(new Thread(SharedNeo4jTestContainer::stop));
  }

  private static synchronized void start() {
    if (container == null) {
      container =
          new Neo4jContainer<>(DockerImageName.parse(NEO4J_IMAGE))
              .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
              .withPlugins("apoc")
              .withAdminPassword(NEO4J_PASSWORD)
              .withEnv("NEO4J_apoc_export_file_enabled", "true")
              .withEnv("NEO4J_apoc_import_file_enabled", "true")
              .withEnv("NEO4J_apoc_import_file_use__neo4j__config", "true")
              .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
              .withReuse(true); // Enable container reuse across test runs

      container.start();

      driver =
          GraphDatabase.driver(container.getBoltUrl(), AuthTokens.basic("neo4j", NEO4J_PASSWORD));
    }
  }

  private static synchronized void stop() {
    if (driver != null) {
      try {
        driver.close();
      } catch (Exception e) {
        // Log but don't throw
      }
      driver = null;
    }

    if (container != null) {
      try {
        container.stop();
      } catch (Exception e) {
        // Log but don't throw
      }
      container = null;
    }
  }

  public static Driver getDriver() {
    return driver;
  }

  public static String getBoltUrl() {
    return container.getBoltUrl();
  }

  public static String getHttpUrl() {
    return container.getHttpUrl();
  }

  public static String getPassword() {
    return NEO4J_PASSWORD;
  }

  /** Clear all data in the database. Useful for test isolation. */
  public static void clearDatabase() {
    try (var session = driver.session()) {
      session.run("MATCH (n) DETACH DELETE n").consume();
    }
  }
}
