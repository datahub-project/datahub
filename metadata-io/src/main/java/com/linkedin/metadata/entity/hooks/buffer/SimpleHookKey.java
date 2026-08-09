package com.linkedin.metadata.entity.hooks.buffer;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * OSS default {@link HookKey}: a plain {@code (hookId, urn, aspectName, sequence)} tuple with no
 * routing metadata. Used by {@link SimpleHookContextResolver} for the single-database (single-
 * tenant) deployment, where every drained replay runs under the system {@link
 * io.datahubproject.metadata.context.OperationContext}.
 *
 * <p>Explicit {@code serialVersionUID = 1L} so in-flight Hazelcast entries survive rolling deploys
 * (matches the {@code RetentionKey} / {@code SimpleRetentionKey} contract).
 */
public final class SimpleHookKey implements HookKey {

  private static final long serialVersionUID = 1L;

  private final String hookId;
  private final String urn;
  private final String aspectName;
  private final long sequence;

  public SimpleHookKey(
      @Nonnull String hookId, @Nonnull String urn, @Nonnull String aspectName, long sequence) {
    this.hookId = Objects.requireNonNull(hookId, "hookId must not be null");
    this.urn = Objects.requireNonNull(urn, "urn must not be null");
    this.aspectName = Objects.requireNonNull(aspectName, "aspectName must not be null");
    this.sequence = sequence;
  }

  @Override
  public String getHookId() {
    return hookId;
  }

  @Override
  public String getUrn() {
    return urn;
  }

  @Override
  public String getAspectName() {
    return aspectName;
  }

  @Override
  public long getSequence() {
    return sequence;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SimpleHookKey)) {
      return false;
    }
    SimpleHookKey that = (SimpleHookKey) o;
    return sequence == that.sequence
        && hookId.equals(that.hookId)
        && urn.equals(that.urn)
        && aspectName.equals(that.aspectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hookId, urn, aspectName, sequence);
  }

  @Override
  public String toString() {
    return "SimpleHookKey{"
        + "hookId='"
        + hookId
        + '\''
        + ", urn='"
        + urn
        + '\''
        + ", aspectName='"
        + aspectName
        + '\''
        + ", sequence="
        + sequence
        + '}';
  }
}
