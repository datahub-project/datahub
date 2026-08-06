package com.linkedin.metadata.usage.registry.operations;

import javax.annotation.Nonnull;

/** Registry activity_class values used for MAU emit_when rules. */
public enum ActivityClass {
  READ,
  WRITE,
  /** Operational/admin activity — contributes to {@code active_readers} distinct metrics. */
  OPERATION;

  @Nonnull
  public String dimensionValue() {
    return switch (this) {
      case READ -> "read";
      case WRITE -> "write";
      case OPERATION -> "operation";
    };
  }

  public static ActivityClass fromYaml(String raw) {
    return switch (raw.toLowerCase()) {
      case "read" -> READ;
      case "write" -> WRITE;
      case "operation" -> OPERATION;
      default -> throw new IllegalArgumentException("Unknown activity_class: " + raw);
    };
  }
}
