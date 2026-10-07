package com.linkedin.gms.factory.common;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

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

  @Test
  public void installIsNoOpWhenDisabledAndWrapsWhenEnabled() {
    io.ebean.config.DatabaseConfig cfg = new io.ebean.config.DatabaseConfig();
    ActorSqlComment.install(cfg, "p", false, () -> mock(DataSourcePool.class));
    assertNull(cfg.getDataSource());
    DataSourcePool pool = mock(DataSourcePool.class);
    ActorSqlComment.install(cfg, "p", true, () -> pool);
    assertTrue(cfg.getDataSource() instanceof DataSourcePool);
    // the public overload with a real DataSourceConfig is a no-op when disabled
    ActorSqlComment.install(
        new io.ebean.config.DatabaseConfig(),
        "q",
        new io.ebean.datasource.DataSourceConfig(),
        false);
  }

  @Test
  public void poolProxyPassesNonConnectionResultsThrough() throws SQLException {
    DataSourcePool pool = mock(DataSourcePool.class);
    when(pool.name()).thenReturn("main");
    assertEquals(ActorSqlComment.wrap(pool).name(), "main");
  }

  @Test
  public void commentingConnectionDelegatesEveryMethod() throws Exception {
    Connection delegate = mock(Connection.class, org.mockito.Mockito.RETURNS_DEFAULTS);
    CommentingConnection wrapper = new CommentingConnection(delegate);
    assertSame(wrapper.delegate(), delegate);
    for (java.lang.reflect.Method m : Connection.class.getMethods()) {
      if (m.isDefault() || java.lang.reflect.Modifier.isStatic(m.getModifiers())) {
        continue;
      }
      Object[] args =
          java.util.Arrays.stream(m.getParameterTypes()).map(ActorSqlCommentTest::sample).toArray();
      m.invoke(wrapper, args);
      java.lang.reflect.Method target =
          Connection.class.getMethod(m.getName(), m.getParameterTypes());
      // the SQL-taking methods are verified separately (they rewrite the first argument)
      if (!(args.length > 0 && args[0] instanceof String && isSqlMethod(m.getName()))) {
        target.invoke(org.mockito.Mockito.verify(delegate), args);
      }
    }
    // Wrapper semantics
    assertSame(wrapper.unwrap(CommentingConnection.class), wrapper);
    assertTrue(wrapper.isWrapperFor(Connection.class));
    wrapper.unwrap(Comparable.class);
    org.mockito.Mockito.verify(delegate).unwrap(Comparable.class);
  }

  private static boolean isSqlMethod(String name) {
    return name.equals("prepareStatement")
        || name.equals("prepareCall")
        || name.equals("nativeSQL");
  }

  private static Object sample(Class<?> t) {
    if (t == String.class) return "select 1";
    if (t == int.class) return 0;
    if (t == boolean.class) return false;
    if (t == int[].class) return new int[0];
    if (t == String[].class) return new String[0];
    if (t == Class.class) return Runnable.class;
    if (t == java.util.Properties.class) return new java.util.Properties();
    if (t == java.util.Map.class) return new java.util.HashMap<>();
    if (t == java.util.concurrent.Executor.class)
      return (java.util.concurrent.Executor) Runnable::run;
    if (t == java.sql.Savepoint.class) return mock(java.sql.Savepoint.class);
    if (t == java.sql.ShardingKey.class) return mock(java.sql.ShardingKey.class);
    if (t == Object[].class) return new Object[0];
    return null;
  }
}
