package com.linkedin.metadata.utils;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Thread-local marker naming the hook currently executing on this consumer thread. Set by the MCL
 * listener around each {@code invoke}/{@code invokeBatch} call and cleared afterward.
 *
 * <p>Read by instrumentation deep in the call stack (a client proxy that counts external reads, or
 * a fan-out site recording its width) to attribute work to the owning hook without threading the
 * hook name through every method. The guard is what keeps that instrumentation correct even when
 * the consumer runs embedded in GMS: work done outside a hook invocation sees an empty value.
 *
 * <p>Lives in metadata-utils so both the listener module and the deep service modules that contain
 * fan-out sites can read it.
 */
public final class HookExecutionContext {

  private static final ThreadLocal<String> CURRENT_HOOK = new ThreadLocal<>();

  private HookExecutionContext() {}

  public static void set(@Nonnull String hookName) {
    CURRENT_HOOK.set(hookName);
  }

  public static void clear() {
    CURRENT_HOOK.remove();
  }

  /** The hook executing on this thread, or empty if none. */
  public static Optional<String> current() {
    return Optional.ofNullable(CURRENT_HOOK.get());
  }

  /**
   * The hook executing on this thread, or {@code null} if none. Allocation-free variant of {@link
   * #current()} for hot paths (e.g. per-call client instrumentation).
   */
  public static String getCurrentOrNull() {
    return CURRENT_HOOK.get();
  }
}
