package com.linkedin.gms.factory.common;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import io.datahubproject.metadata.context.RequestStats;
import io.ebean.datasource.DataSourcePool;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;

public class ActorSqlCommentTest {

  private static RequestStats stats(String actor, String op) {
    RequestStats s = new RequestStats(false);
    s.attach(null, actor, op);
    return s;
  }

  @Test
  public void commentNamesActorAndOperationAndIsSafe() {
    assertNull(ActorSqlComment.comment(null));
    assertNull(ActorSqlComment.comment(new RequestStats(false)));
    assertEquals(
        ActorSqlComment.comment(stats("urn:li:corpuser:jdoe", "searchAcrossEntities")),
        "/*datahub_actor='urn:li:corpuser:jdoe',datahub_op='searchAcrossEntities'*/");
    assertEquals(
        ActorSqlComment.comment(stats("urn:li:corpuser:a'*/drop", null)),
        "/*datahub_actor='urn:li:corpuser:a_drop'*/");
  }

  @Test
  public void prefixesStatementsOnlyInsideARequest() throws SQLException {
    DataSourcePool pool = mock(DataSourcePool.class);
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    Statement st = mock(Statement.class);
    when(pool.getConnection()).thenReturn(conn);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(conn.createStatement()).thenReturn(st);

    DataSourcePool wrapped = ActorSqlComment.wrap(pool);
    Connection c = wrapped.getConnection();

    // outside a request: untouched
    c.prepareStatement("select 1");
    verify(conn).prepareStatement("select 1");

    // inside a request: commented, on both prepared and plain statements
    RequestStats s = stats("urn:li:corpuser:jdoe", "getDataset");
    try (Scope ignored = Context.current().with(RequestStats.CONTEXT_KEY, s).makeCurrent()) {
      assertSame(c.prepareStatement("select 2"), ps);
      verify(conn)
          .prepareStatement(
              "/*datahub_actor='urn:li:corpuser:jdoe',datahub_op='getDataset'*/ select 2");
      c.createStatement().execute("select 3");
      verify(st)
          .execute("/*datahub_actor='urn:li:corpuser:jdoe',datahub_op='getDataset'*/ select 3");
    }
  }

  @Test
  public void exceptionsAreUnwrapped() throws SQLException {
    DataSourcePool pool = mock(DataSourcePool.class);
    when(pool.getConnection()).thenThrow(new SQLException("boom"));
    try {
      ActorSqlComment.wrap(pool).getConnection();
    } catch (SQLException e) {
      assertEquals(e.getMessage(), "boom");
      return;
    }
    throw new AssertionError("expected SQLException");
  }
}
