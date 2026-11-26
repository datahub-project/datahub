package com.linkedin.metadata.utils.elasticsearch;

import com.datahub.context.OperationFingerprint;
import javax.annotation.Nonnull;

/**
 * Resolves the search index-name prefix for the current operation.
 *
 * <p>OSS resolves a single, statically configured prefix (see {@link
 * ConfiguredIndexPrefixResolver}). An extension module may register a {@code @Primary} bean that
 * derives the prefix from the in-flight operation (e.g. per-namespace index isolation) without the
 * caller — or {@link IndexConvention} itself — knowing how.
 *
 * <p>Takes an {@link OperationFingerprint} (not {@code OperationContext}) so this SPI can live in
 * {@code metadata-utils} alongside {@link IndexConvention} without pulling in a dependency on
 * {@code metadata-operation-context}, which would create a module cycle. {@code OperationContext}
 * implements {@link OperationFingerprint}, so callers holding a full context pass it straight
 * through.
 */
public interface IndexPrefixResolver {

  /**
   * Resolve the index-name prefix for {@code operation}.
   *
   * <p>The result is prepended (with a {@code _} separator) to every index name and index pattern.
   * An empty string means "no prefix". An implementation that derives the prefix from the operation
   * MUST fall back to the deploy-wide prefix when the operation carries no such identity (bootstrap
   * / system paths), otherwise cross-cutting admin work (reindex, index enumeration) would be
   * silently scoped to a single namespace.
   *
   * @param operation fingerprint of the in-flight operation ({@link OperationFingerprint#EMPTY} for
   *     bootstrap / test paths)
   * @return the prefix, or the empty string for no prefix; never {@code null}
   */
  @Nonnull
  String resolvePrefix(@Nonnull OperationFingerprint operation);
}
