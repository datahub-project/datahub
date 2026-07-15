package com.linkedin.metadata.search.postgres;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class PostgresSearchEntityTypeTest {

  @Test
  public void extractsEntityTypeFromDocument() {
    assertEquals(
        PostgresSearchEntityType.extractEntityType(
            "{\"_entityType\":\"dataset\",\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,x,PROD)\"}"),
        "dataset");
  }

  @Test
  public void emptyWhenMissing() {
    assertEquals(PostgresSearchEntityType.extractEntityType("{}"), "");
    assertEquals(PostgresSearchEntityType.extractEntityType(null), "");
  }
}
