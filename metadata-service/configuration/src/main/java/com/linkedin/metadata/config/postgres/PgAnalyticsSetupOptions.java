package com.linkedin.metadata.config.postgres;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.Value;

/** Resolved pgAnalytics registry when {@code postgres.pgAnalytics.enabled} is true. */
@Value
public class PgAnalyticsSetupOptions {
  String defaultStoreName;
  Map<String, PgAnalyticsStoreOptions> stores;

  /** Normalized metric_family → store name. */
  Map<String, String> routing;

  public PgAnalyticsSetupOptions(
      @Nonnull String defaultStoreName,
      @Nonnull Map<String, PgAnalyticsStoreOptions> stores,
      @Nonnull Map<String, String> routing) {
    this.defaultStoreName = Objects.requireNonNull(defaultStoreName, "defaultStoreName");
    this.stores = Collections.unmodifiableMap(new LinkedHashMap<>(stores));
    this.routing = Collections.unmodifiableMap(new LinkedHashMap<>(routing));
  }

  @Nonnull
  public PgAnalyticsStoreOptions getDefaultStore() {
    PgAnalyticsStoreOptions store = stores.get(defaultStoreName);
    if (store == null) {
      throw new IllegalStateException(
          "pgAnalytics default store '" + defaultStoreName + "' is missing from stores map");
    }
    return store;
  }

  @Nonnull
  public PgAnalyticsStoreOptions resolveStore(@Nonnull String metricFamily) {
    String key = metricFamily.trim().toLowerCase(Locale.ROOT);
    String storeName = routing.getOrDefault(key, defaultStoreName);
    PgAnalyticsStoreOptions store = stores.get(storeName);
    if (store == null) {
      throw new IllegalStateException(
          "pgAnalytics routing for " + key + " targets unknown store '" + storeName + "'");
    }
    return store;
  }
}
