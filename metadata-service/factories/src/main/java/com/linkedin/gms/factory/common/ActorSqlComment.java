package com.linkedin.gms.factory.common;

import io.datahubproject.metadata.context.RequestStats;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import io.ebean.datasource.DataSourceFactory;
import io.ebean.datasource.DataSourcePool;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Optional ({@code telemetry.requestAttribution.postgresActorComment}) JDBC wrapper that prefixes
 * every statement issued while a request is in scope with a comment naming the DataHub actor and
 * operation:
 *
 * <pre>/*datahub_actor='urn:li:corpuser:jdoe',datahub_op='searchAcrossEntities'*&#47; select ...
 * </pre>
 *
 * <p>Postgres writes comments verbatim to the statement log and shows them in {@code
 * pg_stat_activity}, so with this on the database's own records name the actor with no join. The
 * comment carries no trace id or counter on purpose: it varies only by (actor, operation), so the
 * driver's server-side prepared-statement cache still converges to a bounded set of texts. The
 * OpenTelemetry agent's sqlcommenter, when also enabled, adds its own trace comment independently.
 *
 * <p>Implemented as a dynamic proxy over the Ebean pool that hands out {@link CommentingConnection}
 * wrappers. Off (the default) means no wrapper at all; on, each statement pays one string
 * concatenation. Statements issued outside a request (consumers, bootstrap) pass through unchanged.
 * Only prepared statements are rewritten; Ebean issues everything through them.
 */
@Slf4j
public final class ActorSqlComment {
  private ActorSqlComment() {}

  /** Max chars kept from the actor urn and operation name inside the comment. */
  static final int MAX_VALUE_LEN = 200;

  /**
   * When {@code enabled}, builds the pool from {@code config} and installs the commenting wrapper
   * on {@code serverConfig} in place of the plain data source config. No-op otherwise.
   */
  public static void install(
      @Nonnull DatabaseConfig serverConfig,
      @Nonnull String poolName,
      @Nonnull DataSourceConfig config,
      boolean enabled) {
    if (!enabled) {
      return;
    }
    DataSourcePool pool = DataSourceFactory.create(poolName, config);
    serverConfig.setDataSource(wrap(pool));
    log.info("Ebean pool {}: SQL actor comments enabled", poolName);
  }

  /** Wraps a pool so every connection it hands out prefixes statements with the actor comment. */
  @Nonnull
  public static DataSourcePool wrap(@Nonnull DataSourcePool pool) {
    return (DataSourcePool)
        Proxy.newProxyInstance(
            ActorSqlComment.class.getClassLoader(),
            new Class<?>[] {DataSourcePool.class},
            new Delegating(pool) {
              @Override
              Object after(Method m, Object result) {
                return result instanceof Connection ? wrapConnection((Connection) result) : result;
              }
            });
  }

  @Nonnull
  static Connection wrapConnection(@Nonnull Connection c) {
    return new CommentingConnection(c);
  }

  /** {@code sql} with the actor comment prepended, or unchanged when no request is in scope. */
  @Nonnull
  static String prefix(@Nonnull String sql) {
    String c = comment(RequestStats.current().orElse(null));
    return c == null ? sql : c + " " + sql;
  }

  /** The comment for {@code stats}, or null when there is no actor to name. */
  @Nullable
  static String comment(@Nullable RequestStats stats) {
    if (stats == null || stats.getActorUrn() == null) {
      return null;
    }
    StringBuilder sb =
        new StringBuilder(64)
            .append("/*datahub_actor='")
            .append(clean(stats.getActorUrn()))
            .append('\'');
    if (stats.getRequestId() != null) {
      sb.append(",datahub_op='").append(clean(stats.getRequestId())).append('\'');
    }
    return sb.append("*/").toString();
  }

  /** Keeps the value from terminating the comment or the quote; bounded length. */
  @Nonnull
  static String clean(@Nonnull String v) {
    String s = v.length() > MAX_VALUE_LEN ? v.substring(0, MAX_VALUE_LEN) : v;
    return s.replace("*/", "").replace("/*", "").replace('\'', '_').replace('\n', ' ');
  }

  /** Reflective delegate with before/after hooks; unwraps target exceptions. */
  private abstract static class Delegating implements InvocationHandler {
    private final Object target;

    Delegating(Object target) {
      this.target = target;
    }

    Object[] before(Method m, Object[] args) {
      return args;
    }

    Object after(Method m, Object result) {
      return result;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
      try {
        return after(m, m.invoke(target, before(m, args)));
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}
