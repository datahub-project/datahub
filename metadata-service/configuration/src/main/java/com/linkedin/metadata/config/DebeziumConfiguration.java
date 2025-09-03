package com.linkedin.metadata.config;

import java.util.Map;
import lombok.Data;

/**
 * Configuration for Debezium CDC connectors. Follows the Kafka Connect configuration model where
 * arbitrary properties can be passed through to the Debezium connector.
 */
@Data
public class DebeziumConfiguration {

  /** The name of the Debezium connector. */
  private String name;

  /** The Kafka Connect REST API URL. */
  private String url;

  /** Request timeout for kafka connect config REST API calls. */
  private int requestTimeoutMillis;

  /**
   * Debezium connector configuration properties. These are passed directly to Kafka Connect and
   * must follow Debezium's connector-specific schema (e.g., database.hostname, connector.class).
   */
  private Map<String, String> config;
}
