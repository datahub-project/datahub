package com.linkedin.metadata.usage.registry.metrics;

import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.manifest.UsageMetricRegistryManifest;
import com.linkedin.metadata.config.usage.metric.MetricRegistryYamlDefinition;
import io.datahubproject.metadata.context.RequestContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

@Getter
public class UsageMetricRegistry {

  public static final String API_USAGE_FAMILY = "api_usage";

  /** Inventory gauges (entity counts) landed by GMS entity-count publisher. */
  public static final String SYSTEM_USAGE_FAMILY = "system_usage";

  private final Map<String, Map<String, MetricDefinition>> families;

  public UsageMetricRegistry(
      @Nonnull UsageMetricRegistryManifest manifest,
      @Nonnull List<UsageMetricContributor> contributors) {
    Map<String, Map<String, MetricDefinition>> built = new HashMap<>();
    manifest
        .getMetricRegistry()
        .forEach(
            (family, metrics) -> {
              Map<String, MetricDefinition> familyMetrics = new HashMap<>();
              metrics.forEach(
                  (name, def) ->
                      familyMetrics.put(name, MetricDefinition.fromYamlDefinition(name, def)));
              built.put(family, familyMetrics);
            });
    for (UsageMetricContributor contributor : contributors) {
      contributor.contribute(built);
    }
    Map<String, Map<String, MetricDefinition>> frozen = new HashMap<>();
    built.forEach((family, metrics) -> frozen.put(family, Collections.unmodifiableMap(metrics)));
    this.families = Collections.unmodifiableMap(frozen);
  }

  @Nonnull
  public static UsageMetricRegistry loadBundled(
      @Nonnull UsageMetricRegistryLoader loader,
      @Nonnull List<UsageMetricContributor> contributors) {
    return new UsageMetricRegistry(loader.loadBundled(), contributors);
  }

  @Nonnull
  public Map<String, MetricDefinition> apiUsageMetrics() {
    return Optional.ofNullable(families.get(API_USAGE_FAMILY))
        .orElseThrow(() -> new IllegalStateException("Missing api_usage metric family"));
  }

  public enum MergeKind {
    ADDITIVE,
    DISTINCT,
    /** Gauge snapshot quantity (compaction / inventory samples). */
    LATEST,
    /** High-water compaction (analytics peak gauges). */
    MAX;

    static MergeKind fromYaml(String raw) {
      return switch (raw.toLowerCase()) {
        case "additive" -> ADDITIVE;
        case "distinct" -> DISTINCT;
        case "latest" -> LATEST;
        case "max" -> MAX;
        default -> throw new IllegalArgumentException("Unknown merge_kind: " + raw);
      };
    }
  }

  public enum EmitWhen {
    ALWAYS,
    ACTIVITY_ALLOWLIST,
    READER_ACTIVITY_ALLOWLIST,
    WRITER_ACTIVITY_ALLOWLIST,
    INGESTION_REQUEST,
    COST_PROFILE,
    /**
     * Additive metrics recorded only via {@link
     * com.linkedin.metadata.usage.store.UsageAggregationStore#recordReportedUsage} (not
     * request-path {@code recordRequest}).
     */
    REPORTED;

    /** True when the metric is incremented only from report-driven usage. */
    public boolean isReportDriven() {
      return this == REPORTED;
    }

    static EmitWhen fromYaml(String raw) {
      return switch (raw.toLowerCase()) {
        case "always" -> ALWAYS;
        case "activity_allowlist" -> ACTIVITY_ALLOWLIST;
        case "reader_activity_allowlist" -> READER_ACTIVITY_ALLOWLIST;
        case "writer_activity_allowlist" -> WRITER_ACTIVITY_ALLOWLIST;
        case "ingestion_request" -> INGESTION_REQUEST;
        case "cost_profile" -> COST_PROFILE;
        case "reported" -> REPORTED;
        default -> throw new IllegalArgumentException("Unknown emit_when: " + raw);
      };
    }
  }

  public record MetricDefinition(
      String metricName,
      MergeKind mergeKind,
      String distinctKey,
      ValueUnit valueUnit,
      EmitWhen emitWhen,
      /** Empty = all request APIs. Otherwise only matching {@link RequestContext.RequestAPI}. */
      Set<RequestContext.RequestAPI> requestApis) {

    public MetricDefinition {
      requestApis = requestApis == null ? Set.of() : Set.copyOf(requestApis);
    }

    public boolean matchesRequestApi(@Nullable RequestContext.RequestAPI requestApi) {
      if (requestApis.isEmpty()) {
        return true;
      }
      return requestApi != null && requestApis.contains(requestApi);
    }

    public static MetricDefinition fromYamlDefinition(
        @Nonnull String name, @Nonnull MetricRegistryYamlDefinition def) {
      return new MetricDefinition(
          name,
          MergeKind.fromYaml(def.getMergeKind()),
          def.getDistinctKey(),
          ValueUnit.fromYaml(def.getValueUnit()),
          EmitWhen.fromYaml(def.getEmitWhen()),
          parseRequestApis(def.getRequestApis()));
    }

    @Nonnull
    private static Set<RequestContext.RequestAPI> parseRequestApis(@Nullable List<String> rawApis) {
      if (rawApis == null || rawApis.isEmpty()) {
        return Set.of();
      }
      Set<RequestContext.RequestAPI> parsed = new HashSet<>();
      for (String raw : rawApis) {
        if (raw == null || raw.isBlank()) {
          continue;
        }
        try {
          parsed.add(RequestContext.RequestAPI.valueOf(raw.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Unknown request_api in metric registry: " + raw, e);
        }
      }
      return parsed;
    }
  }
}
