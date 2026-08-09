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

  // Opt-in: write aspects via optimistic locking (compare-and-set on SystemMetadata.version) instead
  // of SELECT ... FOR UPDATE. Per-process; ignored on Cassandra. Default off (legacy path unchanged).
  private boolean optimisticLockingEnabled;

  // Opt-in (requires optimisticLockingEnabled): on an optimistic-lock CONFLICT, retry only the
  // conflicted URN's branch within the open transaction instead of re-running the whole batch via
  // runInTransactionWithRetry. Default off keeps the full-batch retry of the optimistic-locking base.
  private boolean scopedRetryEnabled;

  private ReadPoolConfiguration readPool;

  @Builder.Default
  private TransactionRetryConfiguration transactionRetry = new TransactionRetryConfiguration();

  /** Test-only config with defaults; note {@code entityWriteAdvisoryLockEnabled} is off. */
  public static final EbeanConfiguration testDefault = EbeanConfiguration.builder().build();
}
