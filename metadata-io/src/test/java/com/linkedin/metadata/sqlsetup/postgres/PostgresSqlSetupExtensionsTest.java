package com.linkedin.metadata.sqlsetup.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PostgresSqlSetupExtensionsTest {

  private Connection connection;
  private Statement statement;

  @BeforeMethod
  public void setUp() throws SQLException {
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
  }

  @Test
  public void maybeCreateExtension_skipsWhenWantFalse() throws SQLException {
    PostgresSqlSetupExtensions.maybeCreateExtension(
        connection, "pg_partman", false, Set.of("pg_partman"));
    verify(connection, never()).createStatement();
  }

  @Test
  public void maybeCreateExtension_rejectsUnsupportedName() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            PostgresSqlSetupExtensions.maybeCreateExtension(
                connection, "pg_trgm", true, Set.of("pg_partman")));
  }

  @Test
  public void maybeCreateExtension_skipsWhenNotAvailable() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(false);
    when(statement.executeQuery(anyString())).thenReturn(rs);

    PostgresSqlSetupExtensions.maybeCreateExtension(
        connection, "pg_partman", true, Set.of("pg_partman"));

    verify(statement, never()).execute(anyString());
  }

  @Test
  public void maybeCreateExtension_createsWhenAvailable() throws SQLException {
    ResultSet available = mock(ResultSet.class);
    when(available.next()).thenReturn(true);
    when(statement.executeQuery(anyString())).thenReturn(available);
    when(statement.execute(anyString())).thenReturn(false);

    PostgresSqlSetupExtensions.maybeCreateExtension(
        connection, "pg_partman", true, Set.of("pg_partman"));

    verify(statement).execute("CREATE EXTENSION IF NOT EXISTS pg_partman");
  }

  @Test
  public void maybeCreateExtension_swallowsCreateFailure() throws SQLException {
    ResultSet available = mock(ResultSet.class);
    when(available.next()).thenReturn(true);
    when(statement.executeQuery(anyString())).thenReturn(available);
    when(statement.execute(anyString())).thenThrow(new SQLException("permission denied"));

    PostgresSqlSetupExtensions.maybeCreateExtension(
        connection, "pg_partman", true, Set.of("pg_partman"));
  }

  @Test
  public void isExtensionAvailable_andInstalled() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true).thenReturn(false);
    when(statement.executeQuery(anyString())).thenReturn(rs);

    assertTrue(PostgresSqlSetupExtensions.isExtensionAvailable(connection, "pg_partman"));
    assertFalse(PostgresSqlSetupExtensions.isExtensionInstalled(connection, "pg_cron"));
  }
}
