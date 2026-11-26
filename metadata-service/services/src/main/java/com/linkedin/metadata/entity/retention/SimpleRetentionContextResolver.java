package com.linkedin.metadata.entity.retention;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * OSS default {@link RetentionContextResolver}. Keys carry no routing metadata ({@link
 * SimpleRetentionKey}), all drained entries group together, and {@code resolveOpContext} returns
 * the system context unchanged — matching a single-database deployment.
 */
public class SimpleRetentionContextResolver implements RetentionContextResolver {

  static final String GROUP_KEY = "default";

  @Override
  @Nonnull
  public RetentionKey enrichKey(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName) {
    return new SimpleRetentionKey(urn.toString(), aspectName);
  }

  @Override
  @Nonnull
  public String groupKey(@Nonnull RetentionKey key) {
    return GROUP_KEY;
  }

  @Override
  @Nonnull
  public OperationContext resolveOpContext(
      @Nonnull RetentionKey key, @Nonnull OperationContext systemOperationContext) {
    return systemOperationContext;
  }
}
