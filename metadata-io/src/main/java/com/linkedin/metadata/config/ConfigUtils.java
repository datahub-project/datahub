/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config;

import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.config.shared.ResultsLimitConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

@Slf4j
public class ConfigUtils {
  private ConfigUtils() {}

  public static int applyLimit(
      @Nonnull SearchServiceConfiguration config, @Nullable Integer limit) {
    return applyLimit(config.getLimit().getResults(), limit);
  }

  public static int applyLimit(@Nonnull GraphServiceConfiguration config, @Nullable Integer limit) {
    return applyLimit(config.getLimit().getResults(), limit);
  }

  public static int applyLimit(
      @Nonnull SystemMetadataServiceConfig config, @Nullable Integer limit) {
    return applyLimit(config.getLimit().getResults(), limit);
  }

  public static int applyLimit(
      @Nonnull TimeseriesAspectServiceConfig config, @Nullable Integer limit) {
    return applyLimit(config.getLimit().getResults(), limit);
  }

  static int applyLimit(@Nonnull ResultsLimitConfig config, @Nullable Integer limit) {
    if (limit == null || limit < 0) {
      return config.getApiDefault();
    }

    if (limit > config.getMax()) {
      if (config.isStrict()) {
        throw new IllegalArgumentException("Result count exceeds limit of " + config.getMax());
      } else {
        log.warn(
            "Requested result count {} exceeds limit {}, applying default limit.",
            limit,
            config.getMax());
        return config.getApiDefault();
      }
    }

    return limit;
  }
}
