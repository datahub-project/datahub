package com.linkedin.metadata.graph.neo4j;

import apoc.path.PathExplorer;
import java.io.File;
import java.net.URI;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.internal.InProcessNeo4jBuilder;

public class Neo4jTestServerBuilder {

  private final Neo4jBuilder builder;
  private Neo4j controls;

  private Neo4jTestServerBuilder(Neo4jBuilder builder) {
    this.builder = builder;
  }

  public Neo4jTestServerBuilder() {
    this(new InProcessNeo4jBuilder().withProcedure(PathExplorer.class));
  }

  public Neo4jTestServerBuilder(File workingDirectory) {
    this(new InProcessNeo4jBuilder(workingDirectory.toPath()));
  }

  public Neo4j newServer() {
    if (controls == null) {
      controls = builder.build();
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
    return controls.defaultDatabaseService();
  }
}
