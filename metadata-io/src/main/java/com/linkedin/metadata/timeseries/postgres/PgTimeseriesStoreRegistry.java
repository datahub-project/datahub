package com.linkedin.metadata.timeseries.postgres;

import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
import io.ebean.Database;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Runtime registry of per-store JDBC {@link Database} handles and DAOs for pgTimeseries. Built once
 * at Spring startup from {@link PgTimeseriesSetupOptions}.
 */
public final class PgTimeseriesStoreRegistry {

  @Getter @Nonnull private final PgTimeseriesSetupOptions setupOptions;

  @Nonnull private final Map<String, StoreHandle> storesByName;

  public PgTimeseriesStoreRegistry(
      @Nonnull PgTimeseriesSetupOptions setupOptions,
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
  public StoreHandle resolve(@Nonnull String entityName, @Nonnull String aspectName) {
    PgTimeseriesStoreOptions options = setupOptions.resolveStore(entityName, aspectName);
    StoreHandle handle = storesByName.get(options.getName());
    if (handle == null) {
      throw new IllegalStateException(
          "No runtime handle for pgTimeseries store '" + options.getName() + "'");
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
      throw new IllegalStateException("Unknown pgTimeseries store '" + storeName + "'");
    }
    return handle;
  }

  @Nonnull
  public Map<String, StoreHandle> allStores() {
    return storesByName;
  }

  @Getter
  public static final class StoreHandle {
    @Nonnull private final PgTimeseriesStoreOptions options;
    @Nonnull private final Database database;
    @Nonnull private final PostgresTimeseriesAspectDao dao;

    public StoreHandle(
        @Nonnull PgTimeseriesStoreOptions options,
        @Nonnull Database database,
        @Nonnull PostgresTimeseriesAspectDao dao) {
      this.options = Objects.requireNonNull(options, "options");
      this.database = Objects.requireNonNull(database, "database");
      this.dao = Objects.requireNonNull(dao, "dao");
    }
  }
}
