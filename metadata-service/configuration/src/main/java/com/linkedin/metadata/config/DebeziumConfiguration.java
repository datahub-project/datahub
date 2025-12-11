/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;

/**
 * Configuration for Debezium CDC connectors. Follows the Kafka Connect configuration model where
 * arbitrary properties can be passed through to the Debezium connector.
 */
@Data
public class DebeziumConfiguration {
  public static final String TYPE_POSTGRES = "postgres";
  public static final String TYPE_MYSQL = "mysql";

  /** The name of the Debezium connector. */
  private String name;

  /** The Kafka Connect REST API URL. */
  private String url;

  /** Request timeout for kafka connect config REST API calls. */
  private int requestTimeoutMillis;

  /** Base configuration properties shared across all connector types */
  private Map<String, String> config;

  /** Type of database being used */
  private String type;

  private Map<String, String> postgresConfig;

  private Map<String, String> mysqlConfig;

  /**
   * Get the complete configuration by merging base config with database-specific config.
   *
   * @return Combined configuration map including both base and database-specific settings
   */
  public Map<String, String> getConfig() {
    Map<String, String> mergedConfig = new HashMap<>(config != null ? config : new HashMap<>());

    if (TYPE_POSTGRES.equals(type) && postgresConfig != null) {
      mergedConfig.putAll(postgresConfig);
    } else if (TYPE_MYSQL.equals(type) && mysqlConfig != null) {
      mergedConfig.putAll(mysqlConfig);
    }

    return mergedConfig;
  }
}
