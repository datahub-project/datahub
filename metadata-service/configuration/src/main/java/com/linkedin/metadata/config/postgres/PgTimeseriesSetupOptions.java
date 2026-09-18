package com.linkedin.metadata.config.postgres;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Resolved pgTimeseries registry when {@code postgres.pgTimeseries.enabled} is true; see {@link
 * PostgresSqlSetupProperties#buildPgTimeseriesOptions()}.
 */
@Value
public class PgTimeseriesSetupOptions {
  /** Name of the fallback store for unlisted aspects (usually {@code default}). */
  String defaultStoreName;

  /** Store name → options (insertion order preserved). */
  Map<String, PgTimeseriesStoreOptions> stores;

  /**
   * Normalized {@code entity.aspect} → store name. Unlisted aspects use {@link #defaultStoreName}.
   */
  Map<String, String> routing;

  public PgTimeseriesSetupOptions(
      @Nonnull String defaultStoreName,
      @Nonnull Map<String, PgTimeseriesStoreOptions> stores,
      @Nonnull Map<String, String> routing) {
    this.defaultStoreName = Objects.requireNonNull(defaultStoreName, "defaultStoreName");
    this.stores = Collections.unmodifiableMap(new LinkedHashMap<>(stores));
    this.routing = Collections.unmodifiableMap(new LinkedHashMap<>(routing));
  }

  @Nonnull
  public PgTimeseriesStoreOptions getDefaultStore() {
    PgTimeseriesStoreOptions store = stores.get(defaultStoreName);
    if (store == null) {
      throw new IllegalStateException(
          "pgTimeseries default store '" + defaultStoreName + "' is missing from stores map");
    }
    return store;
  }

  /**
   * Resolves the store for {@code entityName}/{@code aspectName}. Unlisted pairs use the default
   * store.
   */
  @Nonnull
  public PgTimeseriesStoreOptions resolveStore(
      @Nonnull String entityName, @Nonnull String aspectName) {
    String key = routingKey(entityName, aspectName);
    String storeName = routing.getOrDefault(key, defaultStoreName);
    PgTimeseriesStoreOptions store = stores.get(storeName);
    if (store == null) {
      throw new IllegalStateException(
          "pgTimeseries routing for " + key + " targets unknown store '" + storeName + "'");
    }
    return store;
  }

  @Nonnull
  public static String routingKey(@Nonnull String entityName, @Nonnull String aspectName) {
    return entityName.trim().toLowerCase(Locale.ROOT)
        + "."
        + aspectName.trim().toLowerCase(Locale.ROOT);
  }
}
