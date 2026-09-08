package com.linkedin.metadata.analytics.postgres;

import com.linkedin.metadata.config.postgres.PgAnalyticsSetupOptions;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import io.ebean.Database;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.Getter;

public final class PgAnalyticsStoreRegistry {

  @Getter @Nonnull private final PgAnalyticsSetupOptions setupOptions;
  @Nonnull private final Map<String, StoreHandle> storesByName;

  public PgAnalyticsStoreRegistry(
      @Nonnull PgAnalyticsSetupOptions setupOptions,
      @Nonnull Map<String, StoreHandle> storesByName) {
    this.setupOptions = Objects.requireNonNull(setupOptions, "setupOptions");
    this.storesByName =
        Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(storesByName)));
    if (!this.storesByName.containsKey(setupOptions.getDefaultStoreName())) {
      throw new IllegalArgumentException(
          "Registry missing default store '" + setupOptions.getDefaultStoreName() + "'");
    }
  }

  @Nonnull
  public StoreHandle resolve(@Nonnull String metricFamily) {
    PgAnalyticsStoreOptions options = setupOptions.resolveStore(metricFamily);
    StoreHandle handle = storesByName.get(options.getName());
    if (handle == null) {
      throw new IllegalStateException(
          "No runtime handle for pgAnalytics store '" + options.getName() + "'");
    }
    return handle;
  }

  @Nonnull
  public StoreHandle getDefault() {
    return require(setupOptions.getDefaultStoreName());
  }

  @Nonnull
  public StoreHandle require(@Nonnull String storeName) {
    StoreHandle handle = storesByName.get(storeName);
    if (handle == null) {
      throw new IllegalStateException("Unknown pgAnalytics store '" + storeName + "'");
    }
    return handle;
  }

  @Nonnull
  public Map<String, StoreHandle> allStores() {
    return storesByName;
  }

  @Getter
  public static final class StoreHandle {
    @Nonnull private final PgAnalyticsStoreOptions options;
    @Nonnull private final Database database;
    @Nonnull private final PostgresAnalyticsStore store;

    public StoreHandle(
        @Nonnull PgAnalyticsStoreOptions options,
        @Nonnull Database database,
        @Nonnull PostgresAnalyticsStore store) {
      this.options = Objects.requireNonNull(options, "options");
      this.database = Objects.requireNonNull(database, "database");
      this.store = Objects.requireNonNull(store, "store");
    }
  }
}
