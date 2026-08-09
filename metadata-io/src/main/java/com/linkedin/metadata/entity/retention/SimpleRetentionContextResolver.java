package com.linkedin.metadata.entity.retention;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.retention.buffer.RetentionKey;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * OSS default {@link RetentionContextResolver}. Keys carry no routing metadata ({@link
 * RetentionKey} is a plain {@code (urn, aspectName)} record), all drained entries group together,
 * and {@code resolveOpContext} returns the system context unchanged — matching a single-database
 * deployment. An extension module that routes to multiple databases provides its own {@link
 * RetentionContextResolver} implementation (and a matching {@link RetentionKey} subtype whose
 * equality includes the routing metadata).
 */
public class SimpleRetentionContextResolver implements RetentionContextResolver<RetentionKey> {

  static final String GROUP_KEY = "default";

  @Override
  @Nonnull
  public RetentionKey enrichKey(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName) {
    return new RetentionKey(urn.toString(), aspectName);
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
