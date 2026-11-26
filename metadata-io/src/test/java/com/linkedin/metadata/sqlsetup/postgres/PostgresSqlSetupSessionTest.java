package com.linkedin.metadata.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;

public class PostgresSqlSetupSessionTest {

  @Test
  public void ensureSchemaAndSearchPath_missingPreProvisionedSchemaFails() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.prepareStatement(anyString()))
        .thenAnswer(invocation -> namespaceProbe(false, "dhub"));

    try {
      PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, "dhub", false);
      throw new AssertionError("expected missing schema to fail");
    } catch (SQLException ex) {
      assertTrue(ex.getMessage().contains("does not exist"));
    }
    verify(statement, never()).execute(anyString());
  }

  @Test
  public void ensureSchemaAndSearchPath_wrongEffectiveSchemaFails() throws SQLException {
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.execute(anyString())).thenReturn(false);
    when(connection.prepareStatement(anyString()))
        .thenAnswer(invocation -> namespaceProbe(true, "public"));

    try {
      PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, "dhub", false);
      throw new AssertionError("expected mismatched current_schema() to fail");
    } catch (SQLException ex) {
      assertTrue(ex.getMessage().contains("did not resolve"));
    }
    verify(statement).execute("SET search_path TO dhub, public");
    verify(statement, never()).execute("CREATE SCHEMA IF NOT EXISTS dhub");
  }

  private static PreparedStatement namespaceProbe(boolean schemaExists, String currentSchema)
      throws SQLException {
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(schemaExists);
    when(rs.getString(1)).thenReturn(currentSchema);
    return ps;
  }
}
