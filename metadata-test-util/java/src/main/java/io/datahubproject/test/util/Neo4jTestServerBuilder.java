package io.datahubproject.test.util;

import java.net.URI;
import org.neo4j.driver.Driver;

/**
 * Neo4j test server builder that uses a shared container instance. This prevents multiple
 * containers from being started.
 */
public class Neo4jTestServerBuilder {

  private boolean started = false;

  public Neo4jTestServerBuilder() {
    // Constructor doesn't start anything
  }

  public void start() {
    // The shared container is already started via static initialization
    started = true;

    // Clear the database for test isolation
    SharedNeo4jTestContainer.clearDatabase();
  }

  public void shutdown() {
    // Don't actually stop the shared container
    // Just mark as not started
    started = false;
  }

  public URI boltURI() {
    ensureStarted();
    return URI.create(SharedNeo4jTestContainer.getBoltUrl());
  }

  public URI httpURI() {
    ensureStarted();
    return URI.create(SharedNeo4jTestContainer.getHttpUrl());
  }

  public Driver getDriver() {
    ensureStarted();
    return SharedNeo4jTestContainer.getDriver();
  }

  public String getPassword() {
    return SharedNeo4jTestContainer.getPassword();
  }

  private void ensureStarted() {
    if (!started) {
      throw new IllegalStateException("Server not started. Call start() first.");
    }
  }

  // Backwards compatibility
  @Deprecated
  public void newServer() {
    start();
  }
}
