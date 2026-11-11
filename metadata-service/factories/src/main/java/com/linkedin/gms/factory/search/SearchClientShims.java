package com.linkedin.gms.factory.search;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The search clients created at startup, keyed by cluster name.
 *
 * <p>This is a holder rather than a bare {@code Map<String, SearchClientShim<?>>} bean because
 * Spring treats a map-typed injection point as "give me every bean of the value type keyed by bean
 * name", which would quietly produce a different map than the one this factory built.
 */
public class SearchClientShims {

  private final Map<String, SearchClientShim<?>> byCluster;

  public SearchClientShims(@Nonnull Map<String, SearchClientShim<?>> byCluster) {
    this.byCluster = Collections.unmodifiableMap(byCluster);
  }

  @Nonnull
  public Map<String, SearchClientShim<?>> byCluster() {
    return byCluster;
  }

  @Nullable
  public SearchClientShim<?> get(@Nonnull String clusterName) {
    return byCluster.get(clusterName);
  }
}
