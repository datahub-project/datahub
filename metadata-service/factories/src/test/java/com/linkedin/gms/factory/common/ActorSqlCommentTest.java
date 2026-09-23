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
    when(pool.getConnection()).thenReturn(conn);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    DataSourcePool wrapped = ActorSqlComment.wrap(pool);
    Connection c = wrapped.getConnection();

    // outside a request: untouched
    c.prepareStatement("select 1");
    verify(conn).prepareStatement("select 1");

    // inside a request: commented on prepared statements
    RequestStats s = stats("urn:li:corpuser:jdoe", "getDataset");
    try (Scope ignored = Context.current().with(RequestStats.CONTEXT_KEY, s).makeCurrent()) {
      assertSame(c.prepareStatement("select 2"), ps);
      verify(conn)
          .prepareStatement(
              "/*datahub_actor='urn:li:corpuser:jdoe',datahub_op='getDataset'*/ select 2");
      // plain statements are not rewritten; Ebean uses prepared statements only
      c.createStatement();
      verify(conn).createStatement();
      // the wrapper stays unwrappable for the backend-pid lookup
      c.isWrapperFor(Runnable.class);
      verify(conn).isWrapperFor(Runnable.class);
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
