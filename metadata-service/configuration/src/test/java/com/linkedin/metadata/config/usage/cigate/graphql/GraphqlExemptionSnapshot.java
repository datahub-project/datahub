package com.linkedin.metadata.config.usage.cigate.graphql;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlUsageSurface.GraphqlSurfaceEntry;
import com.linkedin.metadata.config.usage.cigate.graphql.GraphqlUsageSurface.GraphqlSurfaceKind;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Reviewable list of GraphQL surface intentionally left unclassified in usage_operations.yaml. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GraphqlExemptionSnapshot(@Nonnull List<Exemption> exemptions) {

  public GraphqlExemptionSnapshot {
    exemptions = List.copyOf(new ArrayList<>(exemptions));
  }

  @Nonnull
  public GraphqlExemptionSnapshot merge(@Nonnull GraphqlExemptionSnapshot overlay) {
    Map<String, Exemption> merged = new LinkedHashMap<>();
    for (Exemption exemption : exemptions) {
      merged.put(exemption.key(), exemption);
    }
    for (Exemption exemption : overlay.exemptions) {
      merged.putIfAbsent(exemption.key(), exemption);
    }
    return new GraphqlExemptionSnapshot(List.copyOf(merged.values()));
  }

  @Nonnull
  Set<String> keys() {
    Set<String> keys = new TreeSet<>();
    for (Exemption exemption : exemptions) {
      keys.add(exemption.key());
    }
    return keys;
  }

  public boolean isExempt(@Nonnull GraphqlSurfaceEntry entry) {
    return keys().contains(entry.key());
  }

  @Nullable
  public String exemptionReasonFor(@Nonnull GraphqlSurfaceEntry entry) {
    for (Exemption exemption : exemptions) {
      if (exemption.key().equals(entry.key())) {
        return exemption.exemptionReason();
      }
    }
    return null;
  }

  @Nonnull
  public String toJson(@Nonnull ObjectMapper mapper) throws IOException {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Nonnull
  public static GraphqlExemptionSnapshot empty() {
    return new GraphqlExemptionSnapshot(List.of());
  }

  @Nonnull
  public static GraphqlExemptionSnapshot fromJsonPath(
      @Nonnull Path path, @Nonnull ObjectMapper mapper) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return mapper.readValue(in, GraphqlExemptionSnapshot.class);
    }
  }

  /**
   * Drop exemptions for surface entries that are now classified via usage_operations.yaml or gate
   * heuristics.
   */
  @Nonnull
  public static GraphqlExemptionSnapshot reconcile(
      @Nonnull GraphqlUsageSurface surface,
      @Nonnull GraphqlExemptionSnapshot existing,
      @Nonnull GraphqlUsageCoverageClassifier classifier) {
    List<Exemption> reconciled = new ArrayList<>();
    for (Exemption exemption : existing.exemptions) {
      GraphqlSurfaceEntry entry = new GraphqlSurfaceEntry(exemption.name(), exemption.kind());
      if (!classifier.classify(entry).isAccountedFor()) {
        reconciled.add(exemption);
      }
    }
    return new GraphqlExemptionSnapshot(reconciled);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Exemption(
      @Nonnull String name, @Nonnull GraphqlSurfaceKind kind, @Nonnull String exemptionReason) {

    @Nonnull
    String key() {
      return kind.name() + ":" + name;
    }
  }
}
