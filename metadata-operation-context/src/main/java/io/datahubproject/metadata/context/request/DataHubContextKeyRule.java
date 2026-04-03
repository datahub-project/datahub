package io.datahubproject.metadata.context.request;

import java.util.Set;
import javax.annotation.concurrent.Immutable;
import lombok.Value;

/** One accepted {@code X-DataHub-Context} key and how it maps to a Micrometer tag. */
@Value
@Immutable
public class DataHubContextKeyRule {
  /** Lowercase header key (e.g. {@code skill}). */
  String headerKey;

  /** Micrometer / Prometheus tag name (e.g. {@code agent_skill}). */
  String metricTagName;

  /**
   * Sanitized allowed values; empty means any sanitized client value is accepted. When non-empty,
   * values not in this set become {@link DataHubContextParsePolicy#getOtherLabel()}.
   */
  Set<String> allowedValues;

  public boolean restrictsValues() {
    return !allowedValues.isEmpty();
  }
}
