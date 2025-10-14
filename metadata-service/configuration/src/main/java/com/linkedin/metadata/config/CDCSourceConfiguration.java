package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Configuration for CDC (Change Data Capture) source processing. Maps to the cdcSource block in
 * application.yaml and supports pluggable CDC implementations like Debezium.
 */
@Data
public class CDCSourceConfiguration {

  /** Whether CDC processing is enabled. */
  private boolean enabled;

  /** Whether to configure the CDC source (e.g., Debezium connector). */
  private boolean configureSource;

  /** The type of CDC source implementation. */
  private String type;

  /**
   * Debezium-specific configuration. Present when type is "debezium" or "debezium-kafka-connector".
   */
  private DebeziumConfiguration debeziumConfig;

  // Future CDC implementations (Maxwell, Canal, etc.) can be added as separate properties

  /**
   * Returns the implementation-specific configuration object based on the CDC type. This allows
   * type-safe access to configuration without knowing the specific implementation.
   */
  public Object getCdcImplConfig() {
    if (type == null) {
      return null;
    }

    switch (type.toLowerCase()) {
      case "debezium":
      case "debezium-kafka-connector":
        return debeziumConfig;
      default:
        return null; // Unsupported CDC type
    }
  }

  /**
   * Sets the implementation-specific configuration object. Type checking ensures only valid
   * configuration objects are accepted.
   */
  public void setCdcImplConfig(Object config) {
    if (config == null || config instanceof DebeziumConfiguration) {
      this.debeziumConfig = (DebeziumConfiguration) config;
    }
    // Future implementations (Maxwell, Canal, etc.) can be handled here
  }
}
