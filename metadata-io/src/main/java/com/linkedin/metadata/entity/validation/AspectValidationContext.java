package com.linkedin.metadata.entity.validation;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Thread-local context for collecting aspect deletion requests during validation.
 *
 * <p>When aspect size validation detects oversized aspects configured for DELETE remediation, it
 * adds deletion requests to this ThreadLocal storage. After the database transaction commits,
 * EntityService retrieves these pending deletions and executes them through proper EntityService
 * flow.
 *
 * <p><b>Thread Safety:</b>
 *
 * <ul>
 *   <li>Each request thread has its own isolated deletion list
 *   <li>No cross-thread contamination (Thread A never sees Thread B's deletions)
 *   <li>List cleared at start of each request and in finally block
 *   <li>Safe for thread pool reuse (cleanup prevents memory leaks)
 * </ul>
 *
 * <p><b>Usage Pattern:</b>
 *
 * <pre>
 * try {
 *   AspectValidationContext.clearPendingDeletions(); // Start fresh
 *   // ... transaction runs, validators add deletions ...
 *   dao.runInTransactionWithRetry(...);
 *   // After commit, process deletions
 *   List&lt;AspectDeletionRequest&gt; deletions = AspectValidationContext.getPendingDeletions();
 *   processDeletions(deletions);
 * } finally {
 *   AspectValidationContext.clearPendingDeletions(); // Always cleanup
 * }
 * </pre>
 */
public class AspectValidationContext {
  private static final ThreadLocal<List<AspectDeletionRequest>> PENDING_DELETIONS =
      ThreadLocal.withInitial(ArrayList::new);

  private AspectValidationContext() {
    // Utility class
  }

  /**
   * Adds a deletion request to the current thread's pending deletion list.
   *
   * @param request deletion request to add
   */
  public static void addPendingDeletion(@Nonnull AspectDeletionRequest request) {
    PENDING_DELETIONS.get().add(request);
  }

  /**
   * Returns a copy of the current thread's pending deletion list.
   *
   * @return immutable copy of pending deletions
   */
  @Nonnull
  public static List<AspectDeletionRequest> getPendingDeletions() {
    return new ArrayList<>(PENDING_DELETIONS.get());
  }

  /**
   * Clears the current thread's pending deletion list and removes the ThreadLocal entry.
   *
   * <p>Must be called in finally block to prevent memory leaks when threads return to pool.
   */
  public static void clearPendingDeletions() {
    PENDING_DELETIONS.remove();
  }
}
