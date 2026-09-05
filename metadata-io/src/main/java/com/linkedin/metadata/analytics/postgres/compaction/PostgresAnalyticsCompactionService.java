package com.linkedin.metadata.analytics.postgres.compaction;

import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionRequest;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionResult;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionService;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * pgAnalytics {@link AnalyticsCompactionService}: session advisory lock + budgeted {@link
 * AnalyticsCompactor}.
 */
@Slf4j
public class PostgresAnalyticsCompactionService implements AnalyticsCompactionService {

  /** Stable advisory-lock key material (hashed via Postgres {@code hashtext}). */
  static final String ADVISORY_LOCK_KEY = "datahub_analytics_compact";

  @Nonnull private final PgAnalyticsStoreRegistry registry;
  @Nonnull private final AnalyticsCompactor compactor;

  public PostgresAnalyticsCompactionService(@Nonnull PgAnalyticsStoreRegistry registry) {
    this.registry = registry;
    this.compactor = new AnalyticsCompactor(registry);
  }

  /** Test-visible constructor. */
  PostgresAnalyticsCompactionService(
      @Nonnull PgAnalyticsStoreRegistry registry, @Nonnull AnalyticsCompactor compactor) {
    this.registry = registry;
    this.compactor = compactor;
  }

  @Override
  @Nonnull
  public String implementation() {
    return AnalyticsCompactor.IMPLEMENTATION;
  }

  @Override
  @Nonnull
  public AnalyticsCompactionResult compact(@Nonnull AnalyticsCompactionRequest request) {
    PgAnalyticsStoreRegistry.StoreHandle first =
        registry.allStores().values().stream().findFirst().orElse(null);
    if (first == null) {
      return AnalyticsCompactionResult.builder()
          .lockNotAcquired(false)
          .moreWorkRemaining(false)
          .implementation(implementation())
          .message("No pgAnalytics stores registered")
          .build();
    }

    try (Connection lockConn = first.getStore().getDatabase().dataSource().getConnection()) {
      lockConn.setAutoCommit(true);
      try (Statement timeout = lockConn.createStatement()) {
        timeout.execute(
            "SET statement_timeout = '" + Math.max(1L, request.getMaxWallClockMillis()) + "ms'");
      }
      try {
        if (!tryAdvisoryLock(lockConn)) {
          log.debug("pgAnalytics compact skipped; lock not acquired");
          return AnalyticsCompactionResult.lockNotAcquired(implementation());
        }
        try {
          return compactor.compact(request);
        } finally {
          releaseAdvisoryLock(lockConn);
        }
      } finally {
        resetStatementTimeout(lockConn);
      }
    } catch (SQLException e) {
      log.warn("pgAnalytics compact failed acquiring/releasing advisory lock", e);
      return AnalyticsCompactionResult.builder()
          .lockNotAcquired(false)
          .moreWorkRemaining(true)
          .failed(true)
          .implementation(implementation())
          .message("Compaction failed: " + e.getMessage())
          .build();
    }
  }

  private static void resetStatementTimeout(@Nonnull Connection conn) {
    try (Statement reset = conn.createStatement()) {
      reset.execute("SET statement_timeout TO DEFAULT");
    } catch (SQLException e) {
      log.warn("Failed to reset statement_timeout after analytics compact", e);
    }
  }

  private static boolean tryAdvisoryLock(@Nonnull Connection conn) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_try_advisory_lock(hashtext(?))")) {
      ps.setString(1, ADVISORY_LOCK_KEY);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  private static void releaseAdvisoryLock(@Nonnull Connection conn) {
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_unlock(hashtext(?))")) {
      ps.setString(1, ADVISORY_LOCK_KEY);
      ps.executeQuery().close();
    } catch (SQLException e) {
      log.warn("Failed to release analytics compact advisory lock", e);
    }
  }
}
