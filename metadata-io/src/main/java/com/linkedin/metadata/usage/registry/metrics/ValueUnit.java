package com.linkedin.metadata.usage.registry.metrics;

import javax.annotation.Nonnull;

/** Parsed from {@code value_unit} in usage metric registry YAML. */
public enum ValueUnit {
  COUNT,
  INPUT_BYTES,
  OUTPUT_BYTES,
  COST_UNITS;

  @Nonnull
  public static ValueUnit fromYaml(@Nonnull String raw) {
    return switch (raw.toLowerCase()) {
      case "count" -> COUNT;
      case "input_bytes" -> INPUT_BYTES;
      case "output_bytes" -> OUTPUT_BYTES;
      case "cost_units" -> COST_UNITS;
      default -> throw new IllegalArgumentException("Unknown value_unit: " + raw);
    };
  }
}
