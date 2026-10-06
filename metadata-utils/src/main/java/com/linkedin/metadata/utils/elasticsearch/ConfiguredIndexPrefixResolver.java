package com.linkedin.metadata.utils.elasticsearch;

import com.datahub.context.OperationFingerprint;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * OSS default {@link IndexPrefixResolver}: returns a single, statically configured prefix for every
 * operation, ignoring the fingerprint. Wired from {@code ElasticSearchConfiguration.index.prefix}.
 * An extension module may override with a {@code @Primary} bean to resolve the prefix per
 * operation.
 */
public class ConfiguredIndexPrefixResolver implements IndexPrefixResolver {

  private final String prefix;

  public ConfiguredIndexPrefixResolver(@Nullable final String prefix) {
    this.prefix = StringUtils.isEmpty(prefix) ? "" : prefix;
  }

  @Nonnull
  @Override
  public String resolvePrefix(@Nonnull final OperationFingerprint operation) {
    return prefix;
  }
}
