package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EbeanConfiguration {
  public static final int DEFAULT_QUERY_KEYS_COUNT = 375;

  private String username;
  private String password;
  private String url;
  private String driver;
  private long minConnections;
  private long maxConnections;
  private long maxInactiveTimeSeconds;
  private long maxAgeMinutes;
  private long leakTimeMinutes;
  private long waitTimeoutMillis;
  private boolean autoCreateDdl;
  private boolean postgresUseIamAuth;
  private String batchGetMethod;
  private Integer queryKeysCountForBatch = DEFAULT_QUERY_KEYS_COUNT;

  // Opt-in: serialize concurrent writes/deletes per entity before acquiring row locks, to reduce
  // hot-key contention and prevent lock-order deadlocks between a multi-row FOR UPDATE write and a
  // hard-delete. Postgres uses a transaction-scoped pg_advisory_xact_lock (auto-released); MySQL
  // uses a session-scoped GET_LOCK released explicitly on the same connection. No-op on other
  // engines and when disabled (the default).
  private boolean entityWriteAdvisoryLockEnabled;

  // Write-lock backend for the OL + scoped-retry mode: "none" | "db" | "hazelcast".
  //   none      = no serialization; concurrent writers rely purely on CAS (may thrash on hot keys).
  //   db        = DB advisory lock (Postgres pg_advisory / MySQL GET_LOCK) — simple, no extra
  // infra,
  //               but a waiter holds a pooled DB connection while blocked.
  //   hazelcast = distributed IMap lock — keeps lock waits OFF the DB connection pool (preferred
  // when
  //               connections are the bottleneck); best-effort, CAS remains the correctness guard.
  // Default "none". Back-compat: entityWriteAdvisoryLockEnabled=true is treated as "db" when this
  // is
  // unset/"none".
  private String entityWriteLockBackend = "none";

  // Max seconds to wait to acquire a write lock before proceeding WITHOUT it (CAS still guards).
  // Keeps
  // a slow/absent lock backend from blocking ingest.
  private int entityWriteLockAcquireTimeoutSeconds = 10;

  // Opt-in: write aspects via optimistic locking (compare-and-set on SystemMetadata.version)
  // instead
  // of SELECT ... FOR UPDATE. Per-process; ignored on Cassandra. Default off (legacy path
  // unchanged).
  private boolean optimisticLockingEnabled;

  // Opt-in (requires optimisticLockingEnabled): on an optimistic-lock CONFLICT, retry only the
  // conflicted URN's branch within the open transaction instead of re-running the whole batch via
  // runInTransactionWithRetry. Default off keeps the full-batch retry of the optimistic-locking
  // base.
  private boolean scopedRetryEnabled;

  private ReadPoolConfiguration readPool;

  @Builder.Default
  private TransactionRetryConfiguration transactionRetry = new TransactionRetryConfiguration();

  /**
   * Whether the DAO's in-transaction advisory lock (Postgres pg_advisory / MySQL GET_LOCK) is
   * active. Single source of truth so backend selection is coherent and cannot silently no-lock or
   * double-lock:
   *
   * <ul>
   *   <li>{@code entityWriteLockBackend=db} → active (the "db" backend IS the DAO advisory lock).
   *   <li>{@code entityWriteLockBackend=hazelcast} → NOT active (the Hazelcast pre-transaction gate
   *       is the lock; this prevents double-locking even if the legacy boolean is left true).
   *   <li>{@code entityWriteLockBackend=none}/unset → active only if the legacy {@code
   *       entityWriteAdvisoryLockEnabled} boolean is true (back-compat).
   * </ul>
   */
  /** Backend value normalized to lower-case (null → "none"). Single owner of the parse rule. */
  public String getNormalizedEntityWriteLockBackend() {
    return entityWriteLockBackend == null ? "none" : entityWriteLockBackend.trim().toLowerCase();
  }

  public boolean isDbAdvisoryLockActive() {
    String backend = getNormalizedEntityWriteLockBackend();
    if ("db".equals(backend)) {
      return true;
    }
    if ("hazelcast".equals(backend)) {
      return false;
    }
    return entityWriteAdvisoryLockEnabled;
  }

  /** Test-only config with defaults; note {@code entityWriteAdvisoryLockEnabled} is off. */
  public static final EbeanConfiguration testDefault = EbeanConfiguration.builder().build();
}
