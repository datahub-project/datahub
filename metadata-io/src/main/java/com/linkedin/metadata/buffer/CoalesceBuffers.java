package com.linkedin.metadata.buffer;

import java.util.function.BinaryOperator;

/** Static helpers for common {@link CoalesceBuffer} merge policies. */
public final class CoalesceBuffers {

  private CoalesceBuffers() {}

  /**
   * Keeps the higher of two {@link Long} values (e.g. "highest maxVersion hint seen for a key").
   * Null-safe: a null argument loses to any non-null value; only returns null if both are null.
   *
   * <p>This exact instance is the only merge policy {@link HazelcastCoalesceBuffer} supports
   * (checked by reference identity) since Hazelcast entry processors cannot ship an arbitrary
   * {@link BinaryOperator} over the wire. Callers that need Hazelcast support must pass this
   * constant, not an equivalent lambda.
   */
  public static final BinaryOperator<Long> KEEP_MAX_LONG =
      (current, candidate) -> {
        if (current == null) {
          return candidate;
        }
        if (candidate == null) {
          return current;
        }
        // Strict > (not >=) to match HazelcastCoalesceBuffer.KeepMaxLongProcessor: keep the
        // incumbent on ties so both backends apply an identical keep-max policy.
        return candidate > current ? candidate : current;
      };
}
