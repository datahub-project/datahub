package com.linkedin.metadata.utils;

import java.net.URI;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.harness.TestServerBuilders;


public class Neo4jTestServerBuilder {

  private final TestServerBuilder builder;
  private ServerControls controls;

  public Neo4jTestServerBuilder() {
    builder = TestServerBuilders.newInProcessBuilder();
  }

  public Neo4jTestServerBuilder(String uri) {
    builder = TestServerBuilders.newInProcessBuilder();
    builder.withConfig("dbms.connector.bolt.listen_address", uri);
  }

  public ServerControls newServer() {
    if (controls == null) {
      controls = builder.newServer();
    }
    return controls;
  }

  public void shutdown() {
    if (controls != null) {
      controls.close();
      controls = null;
    }
  }

  public URI boltURI() {
    if (controls == null) {
      throw new IllegalStateException("Cannot access instance URI.");
    }
    return controls.boltURI();
  }

  public URI httpURI() {
    if (controls == null) {
      throw new IllegalStateException("Cannot access instance URI.");
    }
    return controls.httpURI();
  }

  public URI httpsURI() {
    if (controls == null) {
      throw new IllegalStateException("Cannot access instance URI.");
    }
    return controls.httpURI();
  }

  public GraphDatabaseService getGraphDatabaseService() {
    return controls.graph();
  }
}
