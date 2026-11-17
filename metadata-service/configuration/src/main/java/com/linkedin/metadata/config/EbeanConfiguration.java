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
  // hard-delete. Postgres-only: a transaction-scoped pg_advisory_xact_lock (auto-released on
  // commit). No-op on other engines and when disabled (the default).
  private boolean entityWriteAdvisoryLockEnabled;

  // Pre-transaction write-gate backend for the OL + scoped-retry hot-key path: "none" |
  // "hazelcast".
  //   none      = no gate; concurrent writers rely purely on CAS (may thrash on hot keys).
  //   hazelcast = distributed IMap lock — serializes hot-key writers OFF the DB connection pool
  //               (the intended answer when connections are the bottleneck); best-effort, CAS
  //               remains the correctness guard.
  // Default "none". Independent of entityWriteAdvisoryLockEnabled (the Postgres deadlock-ordering
  // advisory lock); either, both, or neither may be enabled.
  // NOTE: @Builder.Default is required — Lombok's @Builder ignores field initializers otherwise, so
  // EbeanConfiguration.builder().build() would yield null and defeat the "none" default.
  @Builder.Default private String entityWriteLockBackend = "none";

  // Max seconds to wait to acquire a write lock before proceeding WITHOUT it (CAS still guards).
  // Keeps
  // a slow/absent lock backend from blocking ingest.
  // NOTE: @Builder.Default is required — without it the builder would produce 0, meaning the lock
  // never waits.
  @Builder.Default private int entityWriteLockAcquireTimeoutSeconds = 10;

  // Hazelcast write-lock lease: auto-release a held lock after this many seconds so a dead/hung
  // holder never wedges a URN. Keep comfortably above realistic batch duration; lease expiry
  // mid-write degrades to CAS (safe), never loss.
  // NOTE: @Builder.Default is required — without it the builder would produce 0 (immediate lease
  // expiry).
  @Builder.Default private int entityWriteLockLeaseSeconds = 300;

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

  // OL CAS-update batching. Batches independent version-0 CAS UPDATEs into one JDBC executeBatch
  // instead of one round trip per row. Off by default. Requires BOTH optimisticLockingEnabled AND
  // scopedRetryEnabled — batching only runs on the scoped-retry compute path and feeds per-item
  // conflicts back to it; with either prerequisite off it stays disabled (writes go sequential).
  private boolean optimisticWriteBatchEnabled;
  // Minimum eligible batch size; below this, skip batching and go sequential. (The MAX per
  // executeBatch is a hardcoded constant in EntityServiceImpl — packet-limited, not
  // operator-tunable.)
  // NOTE: @Builder.Default is required — without it the builder / testDefault would yield 0, which
  // batches every non-empty pending set instead of respecting this threshold.
  @Builder.Default private int optimisticWriteBatchMinSize = 10;

  private ReadPoolConfiguration readPool;

  @Builder.Default
  private TransactionRetryConfiguration transactionRetry = new TransactionRetryConfiguration();

  /** Backend value normalized to lower-case (null → "none"). Single owner of the parse rule. */
  public String getNormalizedEntityWriteLockBackend() {
    return entityWriteLockBackend == null ? "none" : entityWriteLockBackend.trim().toLowerCase();
  }

  /** Test-only config with defaults; note {@code entityWriteAdvisoryLockEnabled} is off. */
  public static final EbeanConfiguration testDefault = EbeanConfiguration.builder().build();
}
