package com.linkedin.metadata.neo4j;

import javax.annotation.Nonnull;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;


public class Neo4jDriverFactory {

  private Neo4jDriverFactory() {
  }

  public static Driver createInstance(@Nonnull String uri, @Nonnull String username, @Nonnull String password) {
    return GraphDatabase.driver(uri, AuthTokens.basic(username, password));
  }
}
