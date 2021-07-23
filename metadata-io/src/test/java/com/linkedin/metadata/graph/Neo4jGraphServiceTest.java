package com.linkedin.metadata.graph;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import javax.annotation.Nonnull;


public class Neo4jGraphServiceTest extends GraphServiceTestBase {

  private Neo4jTestServerBuilder _serverBuilder;
  private Driver _driver;
  private Neo4jGraphService _client;

  @BeforeMethod
  public void init() {
    _serverBuilder = new Neo4jTestServerBuilder();
    _serverBuilder.newServer();
    _driver = GraphDatabase.driver(_serverBuilder.boltURI());
    _client = new Neo4jGraphService(_driver);
  }

  @AfterMethod
  public void tearDown() {
    _serverBuilder.shutdown();
  }

  @Override
  protected @Nonnull GraphService getGraphService() {
    return _client;
  }

  @Override
  protected void syncAfterWrite() { }

}
