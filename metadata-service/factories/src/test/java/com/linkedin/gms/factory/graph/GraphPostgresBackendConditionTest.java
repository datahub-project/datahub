package com.linkedin.gms.factory.graph;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class GraphPostgresBackendConditionTest {

  @Test
  public void treatsPostgresTypeCaseInsensitive() {
    assertTrue(GraphPostgresBackendCondition.usePostgresGraphService("postgres"));
    assertTrue(GraphPostgresBackendCondition.usePostgresGraphService("POSTGRES"));
    assertFalse(GraphPostgresBackendCondition.usePostgresGraphService("elasticsearch"));
    assertFalse(GraphPostgresBackendCondition.usePostgresGraphService(null));
    assertFalse(GraphPostgresBackendCondition.usePostgresGraphService("  "));
  }
}
