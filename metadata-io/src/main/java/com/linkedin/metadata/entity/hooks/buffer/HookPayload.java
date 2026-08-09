package com.linkedin.metadata.entity.hooks.buffer;

import com.datahub.util.RecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Payload for one pending post-commit hook replay: the committed MCL serialized as JSON plus a
 * retry counter used by {@link PostCommitHookDrainer} to move poison keys to the DLQ after a bounded
 * number of failed attempts.
 *
 * <p>The MCL is stored as a JSON string (via {@link RecordUtils#toJsonString}) rather than as a
 * Java-serialized object so the value crosses the Hazelcast wire with its default String serializer
 * — no custom {@code IdentifiedDataSerializable} / compact-serialization registration needed, and
 * the round-trip is the same {@code JacksonDataTemplateCodec} path used elsewhere for
 * RecordTemplates. The MCL carries previous + current aspect, which the hooks read to compute
 * their per-transition delta, so the full MCL is retained (no compacted/derived snapshot).
 *
 * <p>Stable serialized form: {@code serialVersionUID = 1L} so in-flight entries survive rolling
 * deploys (same contract as {@link HookKey} / {@code RetentionKey}).
 */
public final class HookPayload implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Max replay attempts before a key is moved to the DLQ; bounds poison-key memory. */
  static final int MAX_RETRIES = 5;

  private final String mclJson;
  private int retryCount;

  public HookPayload(@Nonnull String mclJson) {
    this(mclJson, 0);
  }

  public HookPayload(@Nonnull String mclJson, int retryCount) {
    this.mclJson = Objects.requireNonNull(mclJson, "mclJson must not be null");
    this.retryCount = retryCount;
  }

  @Nonnull
  public String getMclJson() {
    return mclJson;
  }

  public int getRetryCount() {
    return retryCount;
  }

  /** Increment and return the new count; used when a replay fails and the key stays in the buffer. */
  public HookPayload incrementRetry() {
    return new HookPayload(mclJson, retryCount + 1);
  }

  /** @return true once this entry has exhausted {@link #MAX_RETRIES} and must move to the DLQ. */
  public boolean isPoison() {
    return retryCount >= MAX_RETRIES;
  }

  /** Reconstruct the committed MCL from its JSON form. */
  @Nonnull
  public MetadataChangeLog toMcl() {
    return RecordUtils.toRecordTemplate(MetadataChangeLog.class, mclJson);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HookPayload)) {
      return false;
    }
    HookPayload that = (HookPayload) o;
    return mclJson.equals(that.mclJson) && retryCount == that.retryCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mclJson, retryCount);
  }
}
