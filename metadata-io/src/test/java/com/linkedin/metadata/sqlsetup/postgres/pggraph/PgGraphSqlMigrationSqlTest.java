package com.linkedin.metadata.sqlsetup.postgres.pggraph;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import org.testng.annotations.Test;

public class PgGraphSqlMigrationSqlTest {

  @Test
  public void coreSqlSubstitutesPrefixToken() throws Exception {
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            getClass().getClassLoader(), "sqlsetup/pggraph/migrations/V002__core.sql");

    String substituted =
        PostgresSqlUtils.applyTokenReplacements(
            raw, java.util.Map.of(PgGraphSqlMigrationTokens.TOKEN_PREFIX, "metadata_graph"));

    assertFalse(substituted.contains("__PGGRAPH_PREFIX__"));
    assertTrue(substituted.contains("metadata_graph_vertices"));
    assertTrue(substituted.contains("metadata_graph_edges"));
    assertTrue(substituted.contains("metadata_graph_edge_types"));
  }

  @Test
  public void connectedComponentsSqlSubstitutesPrefixToken() throws Exception {
    String raw =
        PostgresSqlUtils.loadClasspathSql(
            getClass().getClassLoader(), "sqlsetup/pggraph/migrations/R__connected_components.sql");

    String substituted =
        PostgresSqlUtils.applyTokenReplacements(
            raw, java.util.Map.of(PgGraphSqlMigrationTokens.TOKEN_PREFIX, "metadata_graph"));

    assertFalse(substituted.contains("__PGGRAPH_PREFIX__"));
    assertTrue(substituted.contains("pgr_connectedComponents"));
    assertTrue(substituted.contains("pgr_breadthFirstSearch"));
    assertTrue(substituted.contains("metadata_graph_cc"));
  }
}
